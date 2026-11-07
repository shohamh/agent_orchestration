"""Schedules, attached sources, pool lists and program cards: what New card relies on."""
import asyncio
import base64
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit import distributed, launch_context, schedules
from flowkit.artifacts import ancestor_files
from flowkit.executor import step_context
from flowkit.fleet_matching import card_requirements, card_session, match_worker
from flowkit.workers import selection_for, selector

UTC = timezone.utc


def at(text):
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


class Cron(unittest.TestCase):
    def next(self, cron, after, zone=UTC):
        return schedules.next_after(schedules.parse_cron(cron), at(after), zone)

    def test_common_recurrences(self):
        self.assertEqual(self.next('0 9 * * *', '2026-11-06T08:00:00'), at('2026-11-06T09:00:00'))
        self.assertEqual(self.next('0 9 * * *', '2026-11-06T09:00:00'), at('2026-11-07T09:00:00'))
        self.assertEqual(self.next('15 * * * *', '2026-11-06T08:20:00'), at('2026-11-06T09:15:00'))
        # 2026-11-06 is a Friday: the next weekday morning is Monday.
        self.assertEqual(self.next('0 9 * * 1-5', '2026-11-06T10:00:00'), at('2026-11-09T09:00:00'))
        self.assertEqual(self.next('*/20 * * * *', '2026-11-06T08:41:00'), at('2026-11-06T09:00:00'))
        self.assertEqual(self.next('0 0 * * 7', '2026-11-06T00:00:00'), at('2026-11-08T00:00:00'), '7 is Sunday')

    def test_both_day_fields_mean_either(self):
        # The 10th, or any Monday (the 9th comes first).
        self.assertEqual(self.next('0 0 10 * 1', '2026-11-06T00:00:00'), at('2026-11-09T00:00:00'))

    def test_runs_at_local_time_across_daylight_saving(self):
        zone = schedules.zone_for('Asia/Jerusalem')
        # Israel left summer time on 25 Oct 2026: 09:00 local moves from 06:00 to 07:00 UTC.
        self.assertEqual(self.next('0 9 * * *', '2026-10-24T12:00:00', zone), at('2026-10-25T07:00:00'))
        self.assertEqual(self.next('0 9 * * *', '2026-10-23T12:00:00', zone), at('2026-10-24T06:00:00'))

    def test_unknown_zone_uses_the_browser_offset(self):
        self.assertEqual(schedules.zone_for('Not/AZone', 120).utcoffset(None), timedelta(hours=2))

    def test_bad_expressions_are_refused(self):
        for cron in ('* * * *', '60 * * * *', '* 24 * * *', 'a * * * *', '*/0 * * * *', '5-1 * * * *'):
            with self.assertRaises(ValueError, msg=cron): schedules.parse_cron(cron)
        with self.assertRaisesRegex(ValueError, 'never runs'):
            self.next('0 0 30 2 *', '2026-11-06T00:00:00')


class Store(unittest.TestCase):
    NOW = at('2026-11-06T08:00:00')

    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = schedules.Schedules(Path(directory.name) / 'schedules.json')

    def create(self, **when):
        return self.store.create({'system': 'hatchet', 'board': 'b', 'title': 'Nightly', 'request': {'kind': 'program', 'command': 'x'},
                                  'schedule': when}, now=self.NOW)

    def test_one_time_schedule_is_gone_after_it_starts(self):
        item = self.create(at='2026-11-06T09:00:00Z')
        self.assertEqual(self.store.claim(['hatchet'], now=self.NOW), [], 'not due yet')
        self.assertEqual(self.store.claim(['prefect'], now=at('2026-11-06T09:00:00')), [], 'another backend')
        claimed = self.store.claim(['hatchet'], now=at('2026-11-06T09:00:30'))
        self.assertEqual([c['id'] for c in claimed], [item['id']])
        self.assertEqual(self.store.claim(['hatchet'], now=at('2026-11-06T09:01:00')), [], 'claimed once')
        self.store.record(item['id'], run='run-1')
        self.assertEqual(self.store.list(), [])

    def test_one_time_schedule_that_failed_stays_with_its_error(self):
        item = self.create(at='2026-11-06T09:00:00Z')
        self.store.claim(['hatchet'], now=at('2026-11-06T09:00:00'))
        self.store.record(item['id'], error='no worker')
        kept = self.store.list()[0]
        self.assertEqual(kept['last_error'], 'no worker')
        self.assertFalse(kept['enabled'])

    def test_a_repeat_moves_on_and_does_not_make_up_missed_runs(self):
        item = self.create(cron='0 9 * * *', timezone='UTC', summary='Every day at 09:00')
        self.assertEqual(item['next_at'], '2026-11-06T09:00:00Z')
        # The board was down for three days: one start, then the next future time.
        self.assertEqual(len(self.store.claim(['hatchet'], now=at('2026-11-09T12:00:00'))), 1)
        self.assertEqual(self.store.list()[0]['next_at'], '2026-11-10T09:00:00Z')
        self.store.record(item['id'], run='run-1')
        self.assertEqual(self.store.list()[0]['last_run'], 'run-1')

    def test_a_first_time_comes_before_the_recurrence(self):
        item = self.create(at='2026-11-20T10:30:00Z', cron='0 9 * * *', timezone='UTC')
        self.assertEqual(item['next_at'], '2026-11-20T10:30:00Z')
        self.store.claim(['hatchet'], now=at('2026-11-20T10:30:00'))
        self.assertEqual(self.store.list()[0]['next_at'], '2026-11-21T09:00:00Z')

    def test_paused_schedules_do_not_start_and_resume_from_now(self):
        item = self.create(cron='0 9 * * *', timezone='UTC')
        self.store.set_enabled(item['id'], False)
        self.assertEqual(self.store.claim(['hatchet'], now=at('2026-11-06T09:00:00')), [])
        self.store.set_enabled(item['id'], True, now=at('2026-11-08T10:00:00'))
        self.assertEqual(self.store.list()[0]['next_at'], '2026-11-09T09:00:00Z')

    def test_a_repeat_skips_overlaps_unless_told_otherwise(self):
        self.assertEqual(self.create(cron='0 9 * * *')['overlap'], 'skip')
        self.assertEqual(self.create(cron='0 9 * * *', overlap='start')['overlap'], 'start')
        with self.assertRaisesRegex(ValueError, 'overlap'): self.create(cron='0 9 * * *', overlap='queue')

    def test_invalid_times_are_refused(self):
        with self.assertRaisesRegex(ValueError, 'future'): self.create(at='2026-11-01T00:00:00Z')
        with self.assertRaisesRegex(ValueError, 'needs a time'): self.create()
        with self.assertRaisesRegex(ValueError, 'five fields'): self.create(cron='daily')

    def test_preview(self):
        self.assertEqual(schedules.upcoming({'cron': '0 9 * * *', 'timezone': 'UTC'}, now=self.NOW),
                         ['2026-11-06T09:00:00Z', '2026-11-07T09:00:00Z', '2026-11-08T09:00:00Z'])
        self.assertEqual(schedules.upcoming({'at': '2026-11-07T00:00:00Z'}, now=self.NOW), ['2026-11-07T00:00:00Z'])

    def test_a_schedule_keeps_its_uploads(self):
        self.store.create({'system': 'hatchet', 'board': 'b', 'title': 't', 'schedule': {'cron': '0 9 * * *'},
                           'request': {'context': {'files': [{'id': 'a' * 32, 'name': 'x'}]}}}, now=self.NOW)
        self.assertEqual(self.store.referenced_uploads(), {'a' * 32})


class Pools(unittest.TestCase):
    def test_any_listed_pool_matches_and_all_tags_are_required(self):
        worker = {'id': 'pc', 'pools': ['build'], 'tags': ['gpu'], 'online': True}
        wanted = lambda **kw: match_worker(worker, kw)
        self.assertNotEqual(wanted(pools=['build', 'other'])['status'], 'incompatible')
        refused = wanted(pools=['other', 'third'])
        self.assertEqual(refused['status'], 'incompatible')
        self.assertIn('any of the pools other, third', refused['reasons'][0]['message'])
        self.assertEqual(wanted(pools=['build'], tags=['gpu', 'cuda'])['status'], 'incompatible')
        self.assertNotEqual(wanted(pools=[])['status'], 'incompatible', 'no pools means all pools')

    def test_selector_accepts_only_a_list_of_names(self):
        self.assertEqual(selector({'pools': ['a', 'b']}), {'pools': ['a', 'b']})
        for bad in ({'pools': 'a'}, {'pools': [1]}, {'pools': ['']}):
            with self.assertRaises(ValueError): selector(bad)

    def test_a_step_pool_replaces_the_default_pools(self):
        flow = SimpleNamespace(worker={}, steps={'a': SimpleNamespace(worker={}), 'b': SimpleNamespace(worker={'pool': 'gpu'})})
        configured = {'default': {'pools': ['x', 'y'], 'tags': ['t']}}
        self.assertEqual(selection_for(flow, 'a', configured), {'pools': ['x', 'y'], 'tags': ['t']})
        self.assertEqual(selection_for(flow, 'b', configured), {'pool': 'gpu', 'tags': ['t']})


class Attached(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.dir = Path(directory.name)
        patcher = patch.dict(os.environ, {'FLOW_WORKSPACES': str(self.dir / 'workspaces')})
        patcher.start()
        self.addCleanup(patcher.stop)

    def upload(self, name='notes.txt', content=b'hello'):
        return launch_context.save_upload(name, base64.b64encode(content).decode())

    def test_normalize_gives_every_source_its_folder(self):
        saved = self.upload()
        context = launch_context.normalize({
            'text': 'Use these',
            'files': [{'id': saved['id'], 'name': 'notes.txt'}],
            'repositories': [{'url': 'https://github.com/team/project.git', 'ref': 'main'}, {'url': 'git@gitlab.com:team/project.git'}],
            'confluence': [{'url': 'https://acme.atlassian.net/wiki/spaces/ENG/pages/123456/Runbook', 'include_descendants': False}]})
        self.assertEqual([r['target'] for r in context['repositories']], ['repos/project', 'repos/project-2'])
        self.assertEqual(context['repositories'][0]['ref'], 'main')
        self.assertEqual(context['files'][0]['target'], 'context/files/notes.txt')
        self.assertEqual(context['confluence'], [{'base_url': 'https://acme.atlassian.net/wiki', 'page_id': '123456',
                                                  'target': 'context/confluence/123456', 'include_descendants': False}])
        self.assertEqual(context['text'], 'Use these')
        self.assertIn('repos/project (repository https://github.com/team/project.git)', launch_context.note(context))
        self.assertIsNone(launch_context.normalize({}))

    def test_confluence_server_links(self):
        self.assertEqual(launch_context.parse_confluence_url('https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=148639291'),
                         ('https://cwiki.apache.org/confluence', '148639291'))
        with self.assertRaisesRegex(ValueError, 'Confluence page'):
            launch_context.parse_confluence_url('https://example.com/wiki/home')

    def test_unsafe_sources_are_refused(self):
        for repo in ('ext::sh -c id', '/etc', 'file:///etc', '--upload-pack=x', 'https://user:pw@github.com/a/b.git'):
            with self.assertRaises(ValueError, msg=repo): launch_context.normalize({'repositories': [{'url': repo}]})
        with self.assertRaisesRegex(ValueError, 'Invalid git ref'):
            launch_context.normalize({'repositories': [{'url': 'https://x.io/a.git', 'ref': '--upload-pack=x'}]})
        with self.assertRaisesRegex(ValueError, 'no longer available'):
            launch_context.normalize({'files': [{'id': 'a' * 32, 'name': 'gone.txt'}]})
        for name in ('../up.txt', 'a/b.txt', 'con.txt', ''):
            with self.assertRaises(ValueError, msg=name): self.upload(name=name)

    def test_old_unused_uploads_expire_but_scheduled_ones_stay(self):
        old, kept = self.upload('old.txt'), self.upload('kept.txt')
        import time
        for saved in (old, kept):
            os.utime(launch_context.uploads_root() / saved['id'], (time.time() - 8 * 86400,) * 2)
        launch_context.prune_uploads(keep={kept['id']})
        self.assertFalse((launch_context.uploads_root() / old['id']).exists())
        self.assertTrue((launch_context.uploads_root() / kept['id']).exists())

    @unittest.skipUnless(shutil.which('git'), 'needs git')
    async def test_a_local_repository_and_upload_reach_the_workspace(self):
        origin = self.dir / 'origin'
        origin.mkdir()
        run = lambda *args: subprocess.run(['git', *args], cwd=origin, check=True, capture_output=True)
        run('init', '-b', 'main')
        (origin / 'a.txt').write_text('a')
        run('add', '.')
        run('-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-m', 'a')
        run('checkout', '-b', 'feature')
        (origin / 'b.txt').write_text('b')
        run('add', '.')
        run('-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-m', 'b')
        commit = subprocess.run(['git', 'rev-parse', 'HEAD~1'], cwd=origin, check=True, capture_output=True, text=True).stdout.strip()
        run('checkout', 'main')
        saved = self.upload()
        context = {'files': [{'id': saved['id'], 'name': 'notes.txt', 'source': 'upload:x', 'target': 'context/files/notes.txt'}],
                   'repositories': [{'url': str(origin), 'target': 'repos/branch', 'ref': 'feature', 'shallow': True},
                                    {'url': str(origin), 'target': 'repos/commit', 'ref': commit, 'shallow': True}]}
        workspace, events = self.dir / 'work', []
        async def sink(event): events.append(event)
        await launch_context.prepare_locally(context, workspace, sink)
        self.assertEqual((workspace / 'context/files/notes.txt').read_bytes(), b'hello')
        self.assertTrue((workspace / 'repos/branch/b.txt').exists(), 'a branch other than the default')
        self.assertFalse((workspace / 'repos/commit/b.txt').exists(), 'an exact commit')
        self.assertTrue(any(e['type'] == 'tool' and e['status'] == 'ok' for e in events))

    async def test_a_source_that_fails_fails_the_run_with_its_reason(self):
        async def sink(event): pass
        with self.assertRaisesRegex(RuntimeError, 'repos/nope'):
            await launch_context.prepare_on_worker({'repositories': [{'url': str(self.dir / 'missing'), 'target': 'repos/nope'}]}, self.dir, sink)

    async def test_every_step_of_a_run_gets_the_same_files_once(self):
        saved = self.upload()
        context = {'files': [{'id': saved['id'], 'name': 'notes.txt', 'source': 'x', 'target': 'context/files/notes.txt'}]}
        root = distributed.workspace_path('run-1')
        root.mkdir(parents=True)
        async def sink(event): pass
        async def receive(): await asyncio.Event().wait()
        first, second = await asyncio.gather(*(distributed.attached_files(root, context, {}, 'hatchet', receive, sink) for _ in range(2)))
        self.assertEqual(first, second)
        self.assertEqual(base64.b64decode(first['context/files/notes.txt']['data']), b'hello')
        launch_context.delete_upload(saved['id'])
        self.assertEqual(await distributed.attached_files(root, context, {}, 'hatchet', receive, sink), first, 'kept with the run')

    def test_a_step_can_delete_an_attached_file_for_its_dependents(self):
        root = self.dir / 'run'
        (root / '.flow-artifacts').mkdir(parents=True)
        (root / '.flow-artifacts' / 'a.json').write_text(json.dumps({'context/x.txt': None, 'out.txt': {'data': '', 'mode': 420}}))
        flow = SimpleNamespace(steps={'a': SimpleNamespace(needs=[]), 'b': SimpleNamespace(needs=['a'])})
        result = ancestor_files(flow, 'b', root, {'context/x.txt': {'data': '', 'mode': 420}, 'context/y.txt': {'data': '', 'mode': 420}})
        self.assertEqual(set(result), {'context/y.txt', 'out.txt'})

    def test_a_flow_reads_the_typed_text_as_input_text(self):
        flow = SimpleNamespace(input={'topic': 'x'})
        step = SimpleNamespace(needs=[])
        context = step_context(flow, step, {'flowContext': {'text': 'hello'}}, {})
        self.assertEqual(context['input'], {'topic': 'x', 'text': 'hello'})
        self.assertEqual(context['_flow_context'], {'text': 'hello'})
        declared = SimpleNamespace(input={'text': 'own'})
        self.assertEqual(step_context(declared, step, {'flowContext': {'text': 'hello'}}, {})['input'], {'text': 'own'})


class ProgramCard(unittest.IsolatedAsyncioTestCase):
    def test_a_program_needs_its_executable_when_that_can_be_read(self):
        self.assertEqual(card_requirements({'goal': 'gcc main.c', 'agent': 'program'}, {'pools': ['a']}), {'pools': ['a'], 'command': 'gcc'})
        self.assertEqual(card_requirements({'goal': '"$TOOL" run', 'agent': 'program'}, {'tags': ['x']}).get('command', '"$TOOL"'), '"$TOOL"')
        self.assertEqual(card_session({'goal': 'x', 'agent': 'program', 'attached': True, 'programInput': 'y'})['agent'], 'program')

    def test_a_program_card_is_a_step_job_every_worker_can_run(self):
        from flowkit.definition import parse
        from flowkit.fleet_matching import software_requirement
        job = launch_context.program_job('make test', 'hello')
        self.assertEqual(job['context']['input'], {'text': 'hello'})
        self.assertTrue(job['discardFiles'])
        step = parse(job['flow']).steps['program']
        self.assertEqual(step.timeout_minutes, 1440)
        self.assertEqual(software_requirement(step), {'command': 'make'})
        # A pipeline's executable can't be read: it isn't refused as ambiguous, as a flow step would be.
        piped = parse(launch_context.program_job('"$TOOL" run | sort')['flow']).steps['program']
        self.assertNotIn('ambiguous_command', software_requirement(piped))

    def test_repositories_clone_with_their_ref_and_quoted_values(self):
        job = launch_context.clone_job([{'url': 'https://x.io/a.git', 'target': 'repos/a'},
                                        {'url': 'git@x.io:b.git', 'target': 'repos/b', 'ref': 'develop'},
                                        {'url': 'https://x.io/c.git', 'target': 'repos/c', 'ref': 'abc1234'}])
        run = job['flow']['steps']['clone']['run']
        self.assertIn('clone --no-hardlinks --depth 1 -- {{ input.url0 }} {{ input.target0 }}', run)
        self.assertIn('--depth 1 --branch {{ input.ref1 }} -- {{ input.url1 }}', run)
        self.assertIn('checkout --detach {{ input.ref2 }}', run)
        self.assertNotIn('x.io', run, 'values only reach the shell through the template')
        self.assertEqual(job['context']['input']['ref1'], 'develop')

    async def run_card(self, config):
        events = []
        async def sink(event): events.append(event)
        async def receive(): await asyncio.Event().wait()
        with tempfile.TemporaryDirectory() as workspaces, patch.dict(os.environ, {'FLOW_WORKSPACES': workspaces}):
            try:
                result = await distributed.execute_session(config, 'run-1', receive, sink)
            except RuntimeError as exc:
                result = exc
        return result, events

    async def test_runs_on_the_coordinator_without_a_worker(self):
        command = f'"{sys.executable}" -c "import json,sys; print(json.load(sys.stdin)[\'input\'][\'text\'])"'
        result, events = await self.run_card({'goal': command, 'agent': 'program', 'workspace': '', 'programInput': 'from the form', 'context': None, 'worker': None})
        self.assertEqual(result['state'], 'done')
        tools = [e for e in events if e['type'] == 'tool']
        self.assertEqual(tools[-1]['status'], 'ok')
        self.assertIn('from the form', tools[-1]['output'])
        self.assertEqual(events[-1]['state'], 'done')

    async def test_a_failing_program_fails_the_card_with_its_output(self):
        command = f'"{sys.executable}" -c "print(\'boom\'); raise SystemExit(3)"'
        result, events = await self.run_card({'goal': command, 'agent': 'program', 'worker': None})
        self.assertIsInstance(result, RuntimeError)
        self.assertIn('code 3', str(result))
        tools = [e for e in events if e['type'] == 'tool']
        self.assertEqual(tools[-1]['status'], 'error')
        self.assertIn('boom', tools[-1]['output'])
        self.assertTrue(any(e['type'] == 'error' for e in events))

    async def test_program_can_ignore_input_while_streaming_more_than_a_pipe(self):
        command = f'"{sys.executable}" -c "import os; os.close(0); [print(i) for i in range(20000)]"'
        result, events = await asyncio.wait_for(self.run_card({
            'goal': command, 'agent': 'program', 'worker': None,
            'programInput': 'x' * 16000,
        }), 30)
        self.assertEqual(result['state'], 'done')
        self.assertEqual(result['output']['stdout'].splitlines(), [str(i) for i in range(19960, 20000)])

    async def test_ignored_input_preserves_a_nonzero_exit(self):
        command = f'"{sys.executable}" -c "import os; os.close(0); print(\'failed-child\'); raise SystemExit(7)"'
        result, events = await self.run_card({
            'goal': command, 'agent': 'program', 'worker': None, 'programInput': 'x' * 16000,
        })
        self.assertIsInstance(result, RuntimeError)
        self.assertIn('code 7', str(result))
        self.assertIn('failed-child', str(result))


if __name__ == '__main__':
    unittest.main()
