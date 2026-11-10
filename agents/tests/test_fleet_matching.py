"""Fleet requirement matching, launch validation and the cancellable waiting lifecycle."""
import asyncio
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from aiohttp import web
from aiohttp.test_utils import TestServer
from flowkit.definition import parse
from agents.tests.worker_fixture import make_app


def stamp(age_seconds=0):
    return (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).isoformat()


def entry(status, age=0):
    return {'status': status, 'checked_at': stamp(age), 'auth_status': 'unknown'}


def worker(id='pc-a', pools=('build',), tags=('linux', 'gpu'), online=True, commands=None, agents=None, **extra):
    data = {'id': id, 'pools': list(pools), 'tags': list(tags), 'slots': 2, 'cpu_limit': 90, 'ram_limit': 90,
            'online': online, 'active': 0, 'settings_sync': {'state': 'applied'}, 'capacity': {'state': 'known', 'cpu_percent': 10, 'ram_percent': 10, 'gate_id': 'test-gate'}}
    data['inventory'] = {'commands': commands or {}, 'agents': agents or {}, 'refreshed_at': stamp()}
    data.update(extra)
    return data


def codes(result):
    return [r['code'] for r in result['reasons']]


class MatchTests(unittest.TestCase):
    def test_worker_waits_when_gate_settings_are_unverified_or_missing(self):
        from flowkit.fleet_matching import validate_launch
        for sync, capacity in (({'state': 'failed'}, {'state': 'known', 'cpu_percent': 10, 'ram_percent': 10}),
                               ({'state': 'unsupported'}, None),
                               ({'state': 'applied'}, None)):
            with self.subTest(sync=sync, capacity=capacity):
                decision = validate_launch({}, [worker(settings_sync=sync, capacity=capacity)])
                self.assertEqual(decision['status'], 'wait')
                self.assertEqual(decision['candidates'], [])

    def test_tags_resolution_names_pool_only_when_selected(self):
        from flowkit.fleet_matching import validate_launch
        fleet = [worker(tags=('linux',))]
        unpooled = validate_launch({'tags': ['cuda']}, fleet)
        self.assertEqual(unpooled['status'], 'blocked')
        self.assertNotIn('pool', unpooled['resolution'])
        self.assertIn('pool build', validate_launch({'pool': 'build', 'tags': ['cuda']}, fleet)['resolution'])

    def test_malformed_inventory_is_unknown(self):
        from flowkit.fleet_matching import match_worker
        for inventory in ([], {'commands': ['python']}, {'commands': {'python': 'installed'}}, 'bad'):
            result = match_worker(worker(inventory=inventory), {'command': 'python'})
            self.assertEqual((result['status'], [r['code'] for r in result['reasons']]), ('unknown', ['software_unknown']))

    def test_detected_software_carries_its_last_check_without_changing_the_match(self):
        from flowkit.fleet_matching import match_worker
        checked = stamp(120)
        live = worker(agents={'codex': {'status': 'installed', 'checked_at': checked}})
        result = match_worker(live, {'agent': 'codex'})
        self.assertEqual(result['status'], 'compatible')
        self.assertEqual(result['detected'], [{'kind': 'agent', 'name': 'codex', 'status': 'installed', 'checked_at': checked, 'cached': False}])
        # An offline PC: the last known result is shown, but it is never used as a verified answer.
        offline = worker(online=False, inventory={}, last_known={'agents': {'codex': {'status': 'absent', 'checked_at': checked}}})
        result = match_worker(offline, {'agent': 'codex'})
        self.assertEqual((result['status'], sorted(codes(result))), ('unknown', ['offline', 'software_unknown']))
        self.assertEqual(result['detected'], [{'kind': 'agent', 'name': 'codex', 'status': 'absent', 'checked_at': checked, 'cached': True}])
        never = match_worker(worker(online=False, inventory={}), {'command': 'python'})
        self.assertEqual(never['detected'], [{'kind': 'command', 'name': 'python', 'status': 'unknown', 'checked_at': None, 'cached': False}])
        self.assertEqual(match_worker(live, {'pool': 'build'})['detected'], [])

    def test_pool_plus_all_tags_and_software_required(self):
        from flowkit.fleet_matching import match_worker
        w = worker(commands={'python': entry('installed')})
        self.assertEqual(match_worker(w, {'pool': 'build', 'tags': ['linux', 'gpu'], 'command': 'python'})['status'], 'compatible')
        result = match_worker(w, {'pool': 'build', 'tags': ['linux', 'cuda'], 'command': 'python'})
        self.assertEqual(result['status'], 'incompatible')
        self.assertIn('tags', codes(result))
        self.assertIn('cuda', result['reasons'][0]['message'])
        self.assertEqual(codes(match_worker(w, {'pool': 'other', 'command': 'python'})), ['pool'])

    def test_worker_id_selection_and_step_overrides(self):
        from flowkit.fleet_matching import requirements_for, validate_launch
        flow = parse({'name': 'placement', 'worker': {'pool': 'build'}, 'steps': {
            'a': {'kind': 'program', 'run': 'python a.py', 'worker': {'tags': ['gpu']}},
            'b': {'kind': 'agent', 'agent': 'demo', 'prompt': 'x'}}})
        self.assertEqual(requirements_for(flow, 'a'), {'pool': 'build', 'tags': ['gpu'], 'command': 'python'})
        routed = requirements_for(flow, 'b', {'default': {'pool': 'build'}, 'steps': {'b': {'id': 'pc-b'}}})
        self.assertEqual(routed, {'id': 'pc-b', 'agent': 'demo'})
        fleet = [worker('pc-a', agents={'demo': entry('installed')}), worker('pc-b', pools=('other',), agents={'demo': entry('installed')})]
        decision = validate_launch(routed, fleet)
        self.assertEqual((decision['status'], decision['candidates']), ('ready', ['pc-b']))
        missing = validate_launch({'id': 'pc-z'}, fleet)
        self.assertEqual(missing['status'], 'blocked')
        self.assertIn('pc-z', missing['message'])
        self.assertTrue(missing['resolution'])

    def test_verified_absence_blocks_but_stale_offline_and_unknown_wait(self):
        from flowkit.fleet_matching import match_worker, validate_launch
        req = {'pool': 'build', 'command': 'tool'}
        absent = worker(commands={'tool': entry('absent')})
        self.assertEqual(match_worker(absent, req)['status'], 'incompatible')
        blocked = validate_launch(req, [absent])
        self.assertEqual(blocked['status'], 'blocked')
        self.assertIn('tool', blocked['message'])
        self.assertIn('Install', blocked['resolution'])
        stale = worker(commands={'tool': entry('absent', age=3 * 86400)})
        self.assertEqual(match_worker(stale, req)['status'], 'unknown')
        offline = worker(online=False, inventory={})
        self.assertEqual(sorted(codes(match_worker(offline, req))), ['offline', 'software_unknown'])
        unknown = worker(commands={'tool': entry('unknown')})
        self.assertEqual(codes(match_worker(unknown, req)), ['software_unknown'])
        for candidate in (stale, offline, unknown):
            decision = validate_launch(req, [absent, candidate])
            self.assertEqual(decision['status'], 'wait', decision)
            self.assertTrue(decision['waiting_reason'])
        self.assertEqual(validate_launch(req, [absent, offline])['waiting_reason'], 'offline')
        self.assertEqual(validate_launch(req, [])['status'], 'blocked')

    def test_busy_and_high_load_wait_with_reasons(self):
        from flowkit.fleet_matching import match_worker, validate_launch
        req = {'command': 'python'}
        commands = {'python': entry('installed')}
        cases = {'full_slots': worker(commands=commands, active=2),
                 'high_cpu': worker(commands=commands, capacity={'state': 'known', 'cpu_percent': 90, 'ram_percent': 5, 'gate_id': 'test-gate'}),
                 'high_ram': worker(commands=commands, capacity={'state': 'known', 'cpu_percent': 5, 'ram_percent': 95, 'gate_id': 'test-gate'}),
                 'missing_metrics': worker(commands=commands, capacity={'state': 'unknown', 'cpu_percent': None, 'ram_percent': None, 'gate_id': 'test-gate'})}
        for code, item in cases.items():
            result = match_worker(item, req)
            self.assertEqual((result['status'], codes(result)), ('unknown', [code]))
            decision = validate_launch(req, [item])
            self.assertEqual((decision['status'], decision['waiting_reason']), ('wait', code))

    def test_ambiguous_command_requires_explicit_requirement(self):
        from flowkit.fleet_matching import requirements_for, validate_launch
        flow = parse({'name': 'ambiguous', 'steps': {
            'piped': {'kind': 'program', 'run': 'python a.py | sort'},
            'declared': {'kind': 'program', 'run': 'python a.py | sort', 'requires_command': 'python'},
            'builtin': {'kind': 'program', 'run': 'echo hello'}}})
        piped = requirements_for(flow, 'piped')
        self.assertTrue(piped.get('ambiguous_command'))
        decision = validate_launch(piped, [worker(commands={'python': entry('installed')})])
        self.assertEqual(decision['status'], 'blocked')
        self.assertIn('ambiguous_command', codes(decision))
        self.assertIn('requires_command', decision['resolution'])
        self.assertEqual(requirements_for(flow, 'declared'), {'command': 'python'})
        self.assertEqual(requirements_for(flow, 'builtin'), {})


def tool_bin(root):
    """A real executable so the managed executor's own recheck finds it."""
    directory = root/'bin'; directory.mkdir()
    if os.name == 'nt': (directory/'fleet-test-tool.cmd').write_text('@echo off\r\necho tool\r\n')
    else:
        path = directory/'fleet-test-tool'; path.write_text('#!/bin/sh\necho tool\n'); path.chmod(0o755)
    return str(directory)


def switchable(available):
    from flowkit import capabilities
    def probe(kind, name):
        if kind == 'command' and name == 'fleet-test-tool':
            return capabilities._result('installed' if available['tool'] else 'absent')
        return capabilities.probe_agent(name) if kind == 'agent' else capabilities.probe_command(name)
    return probe


async def until(predicate, timeout=20, message='condition'):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline: raise AssertionError(f'timed out waiting for {message}')
        await asyncio.sleep(.05)


def info(root, run, step):
    try: return json.loads((root/'canonical'/run/'.flow-workers'/f'{step}.json').read_text(encoding='utf-8'))
    except (OSError, ValueError): return {}


TOOL_FLOW = {'name': 'tool-flow', 'worker': {'pool': 'build'}, 'steps': {
    'go': {'kind': 'program', 'run': 'python -c "print(\'ran\')"', 'requires_command': 'fleet-test-tool'}}}


class FleetFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.registry = self.root/'workers.yaml'
        self.env = patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(self.registry), 'FLOW_WORKSPACES': str(self.root/'canonical'),
                                           'PATH': tool_bin(self.root) + os.pathsep + os.environ.get('PATH', '')})
        self.env.start(); self.addCleanup(self.env.stop)
        for target, value in (('flowkit.distributed.PLACEMENT_POLL', .1), ('flowkit.distributed.PROBE_INTERVAL', 0),
                              ('flowkit.distributed.TIMEOUT_SLACK', 0),
                              ('flowkit.worker_service.AUTHORIZATION_TIMEOUT', .5), ('flowkit.worker_service.COORDINATOR_STALE', .5)):
            patcher = patch(target, value); patcher.start(); self.addCleanup(patcher.stop)
        # Admission lifecycle tests exercise placement transitions, not host load. Keep their
        # real capacity window deterministic while the worker services continue sampling.
        sampler = patch('flowkit.capacity.MachineSampler.sample', lambda _: (1.0, 1.0))
        sampler.start(); self.addCleanup(sampler.stop)
        self.events = []

    async def sink(self, event): self.events.append(event)

    async def receive(self): await asyncio.Event().wait()

    def write_registry(self, entries):
        self.registry.write_text('workers:\n' + ''.join(
            f'- id: {w}\n  pools: [build]\n' + (f'  url: {url}\n' if url else '') for w, url in entries))

    async def service(self, name, available=None, slots=1):
        kwargs = {'probe': switchable(available)} if available is not None else {}
        app = make_app(worker_id=name, root=self.root/name, slots=slots, **kwargs)
        server = TestServer(app); await server.start_server()
        self.addAsyncCleanup(server.close)
        return app, server


class WaitingLifecycleTests(FleetFixture):
    async def test_engine_triggered_flow_without_selection_uses_fleet(self):
        from flowkit.distributed import execute_step
        self.write_registry([('pc-a', None)])
        flow = parse({'name': 'direct', 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
        task = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'direct-run', self.receive, self.sink, engine='prefect'))
        try:
            await until(lambda: task.done() or info(self.root, 'direct-run', 'go').get('state') == 'waiting', timeout=5)
            self.assertFalse(task.done(), 'direct engine launch must wait for a fleet worker')
            self.assertEqual(info(self.root, 'direct-run', 'go')['waiting_reason'], 'offline')
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_offline_worker_waits_visibly_then_starts_when_it_connects(self):
        from flowkit.distributed import execute_step
        self.write_registry([('pc-a', None)])  # saved, awaiting connection
        flow = parse({'name': 'offline-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(\'ran\')"'}}})
        task = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'offline-run', self.receive, self.sink))
        await until(lambda: info(self.root, 'offline-run', 'go').get('state') == 'waiting', message='waiting state')
        waiting = info(self.root, 'offline-run', 'go')
        self.assertEqual(waiting['waiting_reason'], 'offline')
        self.assertFalse(waiting.get('intervention_needed'))
        self.assertNotIn('url', json.dumps(waiting))
        _, server = await self.service('pc-a')
        self.write_registry([('pc-a', server.make_url(''))])
        output = await asyncio.wait_for(task, 30)
        self.assertIn('ran', output['stdout'])
        self.assertEqual(info(self.root, 'offline-run', 'go')['state'], 'done')

    async def test_recorded_cancel_stops_waiting_even_if_engine_cancel_fails(self):
        from flowkit.distributed import execute_step, mark_cancelled
        marker = self.root/'should-not-start.txt'
        self.write_registry([('pc-a', None)])
        flow = parse({'name': 'cancel-marker', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': f'python -c "open(r\'{marker.as_posix()}\', \'w\').write(\'x\')"'}}})
        task = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'marked-run', self.receive, self.sink))
        try:
            await until(lambda: info(self.root, 'marked-run', 'go').get('state') == 'waiting')
            self.assertTrue(mark_cancelled('marked-run'))
            app, server = await self.service('pc-a')
            self.write_registry([('pc-a', server.make_url(''))])
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
            self.assertFalse(marker.exists())
            self.assertFalse(any(j.started for j in app['jobs'].values()))
        finally:
            if not task.done(): task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_cancel_while_worker_offline_never_starts_later(self):
        from flowkit.distributed import execute_step
        marker = self.root/'started.txt'
        app, server = await self.service('pc-a')
        self.write_registry([('pc-a', server.make_url(''))])
        gate = app['capacity']
        self.assertTrue(gate.try_acquire('blocker')['acquired'])  # the only slot is busy
        flow = parse({'name': 'cancel-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program',
            'run': f'python -c "open(r\'{marker.as_posix()}\', \'w\').write(\'x\')"'}}})
        task = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'cancel-run', self.receive, self.sink))
        await until(lambda: any(getattr(j, 'waiting_reason', None) == 'full_slots' for j in app['jobs'].values()), message='queued job')
        job = next(iter(app['jobs'].values()))
        await until(lambda: info(self.root, 'cancel-run', 'go').get('waiting_reason') == 'full_slots', message='visible queue reason')

        async def offline(*args, **kwargs):
            import aiohttp
            raise aiohttp.ClientConnectionError('worker offline')
        from flowkit.distributed import mark_cancelled
        with patch('flowkit.distributed._delete_job', offline):
            self.assertTrue(mark_cancelled('cancel-run'))  # what the board's cancel API does first
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
        self.assertEqual(info(self.root, 'cancel-run', 'go')['state'], 'cancelled')
        self.assertEqual(job.state, 'queued')  # the worker never heard about the cancellation
        gate.release('blocker')  # capacity becomes available again
        await asyncio.sleep(3)
        self.assertFalse(job.started)
        self.assertFalse(marker.exists())
        self.assertNotEqual(job.state, 'running')

    async def test_software_disappearing_before_start_waits_for_another_worker(self):
        from flowkit.distributed import execute_step
        first = {'tool': True}
        app_a, server_a = await self.service('pc-a', first)
        _, server_b = await self.service('pc-b', {'tool': True})
        self.write_registry([('pc-a', server_a.make_url('')), ('pc-b', server_b.make_url(''))])
        self.assertTrue(app_a['capacity'].try_acquire('blocker')['acquired'])
        task = asyncio.create_task(execute_step(parse(TOOL_FLOW), 'go', {'input': {}, 'steps': {}}, 'moved-run', self.receive, self.sink))
        await until(lambda: any(getattr(j, 'waiting_reason', None) == 'full_slots' for j in app_a['jobs'].values()), message='placed on pc-a')
        first['tool'] = False  # uninstalled after launch, before execution starts
        app_a['capacity'].release('blocker')
        output = await asyncio.wait_for(task, 30)
        self.assertIn('ran', output['stdout'])
        self.assertEqual([e['worker']['id'] for e in self.events if e.get('type') == 'worker'], ['pc-b'])
        self.assertEqual([j.state for j in app_a['jobs'].values()], ['rejected'])
        self.assertFalse(any(j.started for j in app_a['jobs'].values()))

    async def test_intervention_needed_when_no_worker_can_satisfy_then_resumes(self):
        from flowkit.distributed import execute_step
        available = {'tool': True}
        app, server = await self.service('pc-a', available)
        self.write_registry([('pc-a', server.make_url(''))])
        self.assertTrue(app['capacity'].try_acquire('blocker')['acquired'])
        task = asyncio.create_task(execute_step(parse(TOOL_FLOW), 'go', {'input': {}, 'steps': {}}, 'stuck-run', self.receive, self.sink))
        await until(lambda: any(getattr(j, 'waiting_reason', None) == 'full_slots' for j in app['jobs'].values()), message='queued job')
        available['tool'] = False
        app['capacity'].release('blocker')
        await until(lambda: info(self.root, 'stuck-run', 'go').get('intervention_needed'), message='intervention needed')
        stuck = info(self.root, 'stuck-run', 'go')
        self.assertEqual((stuck['state'], stuck['waiting_reason']), ('waiting', 'intervention_needed'))
        self.assertIn('fleet-test-tool', json.dumps(stuck['waiting']))
        self.assertTrue(stuck['resolution'])
        self.assertFalse(task.done())
        available['tool'] = True  # repaired: waiting resumes automatically
        output = await asyncio.wait_for(task, 30)
        self.assertIn('ran', output['stdout'])

    async def test_ambiguous_program_fails_before_placement(self):
        from flowkit.distributed import execute_step
        self.write_registry([('pc-a', None)])
        flow = parse({'name': 'ambiguous-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python a.py | sort'}}})
        with self.assertRaisesRegex(ValueError, 'requires_command'):
            await asyncio.wait_for(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'ambiguous-run', self.receive, self.sink), 5)


class StartLinearizationTests(FleetFixture):
    """A start confirmation is one step with the board's cancel marker and with a settings edit."""
    FLOW = {'name': 'linear-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(1)"'}}}

    async def held_start_service(self, name):
        """A worker whose start confirmation is received but not answered until released."""
        app = make_app(worker_id=name, root=self.root/name, slots=1)
        entered, proceed = asyncio.Event(), asyncio.Event()
        @web.middleware
        async def hold(request, handler):
            if request.method == 'POST' and request.path.endswith('/start'):
                entered.set(); await proceed.wait()
            return await handler(request)
        app.middlewares.append(hold)
        server = TestServer(app); await server.start_server()
        self.addAsyncCleanup(server.close)
        return app, server, entered, proceed

    async def test_cancel_marker_waits_for_an_inflight_start_and_then_cancels_it(self):
        from flowkit.distributed import execute_step, mark_cancelled
        app, server, entered, proceed = await self.held_start_service('pc-a')
        self.write_registry([('pc-a', server.make_url(''))])
        marker = self.root/'canonical'/'race-run'/'.flow-cancel.json'
        task = asyncio.create_task(execute_step(parse(self.FLOW), 'go', {'input': {}, 'steps': {}}, 'race-run', self.receive, self.sink))
        try:
            await asyncio.wait_for(entered.wait(), 20)
            marking = asyncio.create_task(asyncio.to_thread(mark_cancelled, 'race-run'))
            await asyncio.sleep(.5)
            self.assertFalse(marking.done(), 'the cancellation must not be recorded while a start is being confirmed')
            self.assertFalse(marker.exists())
            proceed.set()
            self.assertTrue(await asyncio.wait_for(marking, 10))
            with self.assertRaises(asyncio.CancelledError): await asyncio.wait_for(task, 20)
            self.assertEqual(info(self.root, 'race-run', 'go')['state'], 'cancelled')
        finally:
            proceed.set()
            if not task.done(): task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_cancel_marker_recorded_before_the_start_is_never_confirmed(self):
        from flowkit.distributed import execute_step, mark_cancelled
        from flowkit.worker_service import capacity_call
        app, server, entered, proceed = await self.held_start_service('pc-a')
        self.write_registry([('pc-a', server.make_url(''))])
        self.assertTrue((await capacity_call(app['capacity'].try_acquire, 'blocker'))['acquired'])  # keep the job queued
        task = asyncio.create_task(execute_step(parse(self.FLOW), 'go', {'input': {}, 'steps': {}}, 'early-run', self.receive, self.sink))
        try:
            await until(lambda: any(getattr(j, 'waiting_reason', None) == 'full_slots' for j in app['jobs'].values()), message='queued job')
            self.assertTrue(await asyncio.to_thread(mark_cancelled, 'early-run'))
            app['capacity'].release('blocker')
            with self.assertRaises(asyncio.CancelledError): await asyncio.wait_for(task, 20)
            self.assertFalse(entered.is_set(), 'no start may be confirmed after the cancellation was recorded')
            self.assertFalse(any(j.started for j in app['jobs'].values()))
        finally:
            proceed.set()
            if not task.done(): task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_settings_edit_waits_for_an_inflight_start(self):
        from flowkit.distributed import execute_step
        from flowkit.fleet_registry import save_worker, _read
        app, server, entered, proceed = await self.held_start_service('pc-a')
        self.write_registry([('pc-a', server.make_url(''))])
        before = _read()[0]['settings_revision']
        task = asyncio.create_task(execute_step(parse(self.FLOW), 'go', {'input': {}, 'steps': {}}, 'settings-run', self.receive, self.sink))
        try:
            await asyncio.wait_for(entered.wait(), 20)
            saving = asyncio.create_task(asyncio.to_thread(save_worker, {'id': 'pc-a', 'slots': 1}))
            await asyncio.sleep(.5)
            self.assertFalse(saving.done(), 'a settings edit must not land between the recheck and the start')
            self.assertEqual(_read()[0]['settings_revision'], before)
            proceed.set()
            output = await asyncio.wait_for(task, 30)  # started under the settings it was admitted with
            self.assertIn('1', output['stdout'])
            await asyncio.wait_for(saving, 10)
            self.assertGreater(_read()[0]['settings_revision'], before)
        finally:
            proceed.set()
            if not task.done(): task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def fake_worker(worker_id, capabilities=None, polls=(), cancel_status=200):
    """An execution service that may be older than this coordinator (ignores `authorize`)."""
    from aiohttp import web
    state = {'submitted': 0, 'polls': 0}
    report = capabilities if capabilities is not None else {'commands': {'python': entry('installed')}, 'agents': {}, 'refreshed_at': stamp()}
    async def health(request): return web.json_response({'id': worker_id, 'hostname': 'fake', 'active': 0, 'slots': 1, 'capacity': {'state': 'known', 'cpu_percent': 10, 'ram_percent': 10, 'gate_id': worker_id + '-gate', 'settings_revision': 0, 'settings': {'slots': 1, 'cpu_limit': 90, 'ram_limit': 90}}})
    async def caps(request): return web.json_response(report)
    async def create(request):
        state['submitted'] += 1
        return web.json_response({'id': (await request.json())['id']})
    async def status(request):
        state['polls'] += 1
        if state['polls'] <= len(polls): return web.json_response(polls[state['polls'] - 1])
        return web.json_response({'error': 'job not found'}, status=404)  # the service restarted
    async def ok(request): return web.json_response({'ok': True})
    app = web.Application()
    app.router.add_get('/health', health)
    app.router.add_get('/capabilities', caps)
    app.router.add_post('/capabilities/refresh', caps)
    app.router.add_post('/jobs', create)
    app.router.add_get('/jobs/{id}', status)
    app.router.add_post('/jobs/{id}/input', ok)
    async def cancel(request): return web.json_response({'ok': cancel_status == 200, 'terminated': cancel_status == 200}, status=cancel_status)
    app.router.add_delete('/jobs/{id}', cancel)
    return app, state


class AttemptOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_owner_lock_is_exclusive_across_processes(self):
        import subprocess
        from flowkit.distributed import _try_owner, _release_owner
        with tempfile.TemporaryDirectory() as tmp:
            code = "import sys,time; from pathlib import Path; from flowkit.distributed import _try_owner; h=_try_owner(Path(sys.argv[1]),'step'); print('locked' if h else 'failed',flush=True); sys.stdin.readline()"
            process = subprocess.Popen([sys.executable, '-u', '-c', code, tmp], cwd=str(Path(__file__).resolve().parents[1]), stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
            try:
                self.assertEqual(await asyncio.to_thread(process.stdout.readline), 'locked\n')
                self.assertIsNone(_try_owner(Path(tmp), 'step'))
            finally:
                process.stdin.write('stop\n'); process.stdin.flush()
                await asyncio.to_thread(process.wait, 10)
                process.stdin.close(); process.stdout.close()
            owner = _try_owner(Path(tmp), 'step')
            self.assertIsNotNone(owner)
            _release_owner(owner)

    async def test_corrupt_previous_record_never_proves_no_submission(self):
        from flowkit.distributed import _placed
        from flowkit.restart import atomic_json
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            atomic_json(root/'.flow-attempts'/'step.json', 'old-job')
            path = root/'.flow-workers'/'step.json'
            path.parent.mkdir()
            path.write_text('{broken', encoding='utf-8')
            async def receive(): await asyncio.Event().wait()
            async def sink(event): pass
            with patch('flowkit.distributed.wait_for_worker', side_effect=AssertionError('unsafe retry entered placement')):
                with self.assertRaisesRegex(RuntimeError, 'previous|record'):
                    await _placed(root, 'step', {}, {}, {}, SimpleNamespace(timeout_minutes=60), receive, sink, None, lambda data: None)
            self.assertEqual(json.loads((root/'.flow-attempts'/'step.json').read_text()), 'old-job')

    async def test_crash_before_submission_does_not_strand_retry(self):
        from flowkit.distributed import _placed
        from flowkit.restart import atomic_json
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            atomic_json(root/'.flow-attempts'/'step.json', 'dead-attempt')
            entered = asyncio.Event()
            async def wait(*args):
                entered.set()
                await asyncio.Event().wait()
            async def receive(): await asyncio.Event().wait()
            async def sink(event): pass
            with patch('flowkit.distributed.wait_for_worker', wait):
                task = asyncio.create_task(_placed(root, 'step', {}, {}, {}, SimpleNamespace(timeout_minutes=60), receive, sink, None, lambda data: None))
                try:
                    await asyncio.wait_for(entered.wait(), 2)
                    self.assertFalse(task.done())
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)

    async def test_disconnect_after_submit_waits_until_worker_reconnects(self):
        from aiohttp import web
        from flowkit.distributed import _placed
        from types import SimpleNamespace
        online = {'value': False}
        jobs = []
        async def health(request): return web.json_response({'id': 'pc-a', 'capacity': {'gate_id': 'gate-a'}})
        async def create(request):
            body = await request.json(); jobs.append(body['id'])
            return web.json_response({'id': body['id']})
        async def status(request):
            if request.match_info['id'] == jobs[0]: raise web.HTTPServiceUnavailable()
            return web.json_response({'state': 'done', 'events': [], 'cursor': 0, 'worker': None, 'output': 'ok', 'files': {}})
        async def cancel(request):
            if not online['value']: raise web.HTTPServiceUnavailable()
            return web.json_response({'terminated': True})
        app = web.Application(); app.router.add_get('/health', health); app.router.add_post('/jobs', create)
        app.router.add_get('/jobs/{id}', status); app.router.add_delete('/jobs/{id}', cancel)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            async with TestServer(app) as server:
                url = str(server.make_url('')).rstrip('/')
                registry = root/'workers.yaml'; registry.write_text(f'workers:\n- id: pc-a\n  url: {url}\n')
                worker = {'id': 'pc-a', 'url': url, '_headers': {}, '_gate_id': 'gate-a'}
                async def choose(*args): return worker
                async def receive(): await asyncio.Event().wait()
                async def sink(event): pass
                with patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(registry)}), patch('flowkit.distributed.wait_for_worker', choose), patch('flowkit.distributed.PLACEMENT_POLL', .1):
                    task = asyncio.create_task(_placed(root, 'step', {}, {}, {}, SimpleNamespace(timeout_minutes=60), receive, sink, None, lambda data: None))
                    try:
                        await until(lambda: task.done() or (root/'.flow-workers'/'step.json').exists() and json.loads((root/'.flow-workers'/'step.json').read_text()).get('state') == 'waiting', timeout=5)
                        self.assertFalse(task.done(), 'uncertain worker must wait, not fail')
                        online['value'] = True
                        self.assertEqual(await asyncio.wait_for(task, 5), 'ok')
                    finally:
                        if not task.done(): task.cancel()
                        await asyncio.gather(task, return_exceptions=True)

    async def test_concurrent_attempt_cannot_replace_live_attempt(self):
        from flowkit.distributed import _placed
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            waiting = asyncio.Event()
            async def wait(*args):
                waiting.set()
                await asyncio.Event().wait()
            async def receive(): await asyncio.Event().wait()
            async def sink(event): pass
            async def run():
                return await _placed(root, 'step', {}, {}, {}, SimpleNamespace(timeout_minutes=60), receive, sink, None, lambda data: None)
            with patch('flowkit.distributed.wait_for_worker', wait):
                first = asyncio.create_task(run())
                await waiting.wait()
                second = asyncio.create_task(run())
                try:
                    with self.assertRaisesRegex(RuntimeError, 'previous|attempt'):
                        await asyncio.wait_for(second, 2)
                finally:
                    first.cancel()
                    await asyncio.gather(first, second, return_exceptions=True)


class ReviewFixTests(FleetFixture):
    """Round-1 review fixes; reuses the lifecycle fixture (temporary registry and workspaces)."""
    async def serve(self, app):
        server = TestServer(app); await server.start_server()
        self.addAsyncCleanup(server.close)
        return server

    async def test_legacy_worker_without_gate_never_becomes_placement_candidate(self):
        from aiohttp import web
        from flowkit.workers import fleet_status
        from flowkit.fleet_matching import validate_launch
        app = web.Application()
        async def health(request): return web.json_response({'id': 'legacy', 'hostname': 'old', 'active': 0})
        async def caps(request): return web.json_response({'commands': {'python': entry('installed')}, 'agents': {}})
        app.router.add_get('/health', health)
        app.router.add_get('/capabilities', caps)
        server = await self.serve(app)
        self.write_registry([('legacy', server.make_url(''))])
        public, _ = await fleet_status([{'command': 'python'}])
        self.assertEqual(public[0]['settings_sync']['state'], 'unsupported')
        decision = validate_launch({'command': 'python'}, public)
        self.assertEqual(decision['status'], 'wait')
        self.assertEqual(decision['waiting_reason'], 'gate_unavailable')
        self.assertEqual(decision['candidates'], [])

    async def test_replaced_gate_at_same_endpoint_cannot_confirm_old_attempt(self):
        import uuid
        from flowkit.distributed import _claim_attempt
        from flowkit.restart import atomic_json
        app, _ = fake_worker('pc-a')
        server = await self.serve(app)
        url = str(server.make_url('')).rstrip('/')
        self.write_registry([('pc-a', url)])
        old = str(uuid.uuid4())
        root = self.root/'canonical'/'replaced-run'
        atomic_json(root/'.flow-attempts'/'go.json', old)
        atomic_json(root/'.flow-workers'/'go.json', {'job': old, 'submitted_worker': 'pc-a'})
        atomic_json(root/'.flow-attempt-targets'/f'{old}.json', {'worker': 'pc-a', 'url': url, 'gate_id': 'old-gate'})
        with self.assertRaisesRegex(RuntimeError, 'identity|previous|gate'):
            await _claim_attempt(root, 'go', str(uuid.uuid4()))
        self.assertEqual(json.loads((root/'.flow-attempts'/'go.json').read_text()), old)

    async def test_changed_worker_endpoint_cannot_confirm_old_attempt(self):
        import uuid
        from flowkit.distributed import _claim_attempt
        from flowkit.restart import atomic_json
        app, state = fake_worker('pc-a')
        server = await self.serve(app)
        self.write_registry([('pc-a', server.make_url(''))])
        old = str(uuid.uuid4())
        root = self.root/'canonical'/'changed-run'
        atomic_json(root/'.flow-attempts'/'go.json', old)
        atomic_json(root/'.flow-workers'/'go.json', {'job': old, 'submitted_worker': 'pc-a'})
        atomic_json(root/'.flow-attempt-targets'/f'{old}.json', {'worker': 'pc-a', 'url': 'http://127.0.0.1:1'})
        with self.assertRaisesRegex(RuntimeError, 'endpoint|previous'):
            await _claim_attempt(root, 'go', str(uuid.uuid4()))
        self.assertEqual(json.loads((root/'.flow-attempts'/'go.json').read_text()), old)
        self.assertEqual(state['submitted'], 0)

    async def test_explicit_restart_reconciles_previous_remote_job(self):
        import uuid
        from flowkit.distributed import reconcile_source_attempts
        from flowkit.restart import atomic_json
        app, _ = fake_worker('pc-a')
        server = await self.serve(app)
        self.write_registry([('pc-a', server.make_url(''))])
        old = str(uuid.uuid4())
        root = self.root/'canonical'/'source-run'
        atomic_json(root/'.flow-attempts'/'go.json', old)
        atomic_json(root/'.flow-workers'/'go.json', {'job': old, 'submitted_worker': 'pc-a'})
        atomic_json(root/'.flow-attempt-targets'/f'{old}.json', {'worker': 'pc-a', 'url': str(server.make_url('')).rstrip('/'), 'gate_id': 'pc-a-gate'})
        await reconcile_source_attempts('source-run')
        self.assertFalse((root/'.flow-attempts'/'go.json').exists())

    async def test_uncertain_cancellation_fences_retry(self):
        from flowkit.distributed import execute_step
        running = {'state': 'running', 'events': [], 'cursor': 0, 'worker': None, 'error': None, 'waiting_reason': None}
        app, state = fake_worker('pc-a', polls=[running], cancel_status=503)
        server = await self.serve(app)
        self.write_registry([('pc-a', server.make_url(''))])
        flow = parse({'name': 'uncertain', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python test.py'}}})
        async def execute():
            return await execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'uncertain-run', self.receive, self.sink)
        with self.assertRaisesRegex(RuntimeError, 'job not found'):
            await asyncio.wait_for(execute(), 10)
        record = info(self.root, 'uncertain-run', 'go')
        self.assertEqual(record['submitted_worker'], 'pc-a')
        self.assertTrue((self.root/'canonical'/'uncertain-run'/'.flow-attempts'/'go.json').exists())
        with self.assertRaisesRegex(RuntimeError, 'previous attempt'):
            await asyncio.wait_for(execute(), 10)
        self.assertEqual(state['submitted'], 1)

    async def test_worker_reported_start_is_never_placed_again(self):
        from flowkit.distributed import execute_step
        queued = {'state': 'queued', 'events': [], 'cursor': 0, 'worker': None, 'error': None, 'waiting_reason': None}
        evidence = {'running state': {**queued, 'state': 'running'},
                    'worker field': {**queued, 'worker': {'id': 'old', 'hostname': 'fake'}},
                    'step events': {**queued, 'events': [{'type': 'log', 'stream': 'stdout', 'text': 'hi'}], 'cursor': 1}}
        flow = parse({'name': 'old-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
        for name, started in evidence.items():
            with self.subTest(name):
                app, state = fake_worker('old', polls=[started])
                server = await self.serve(app)
                self.write_registry([('old', server.make_url(''))])
                run = 'old-run-' + name.split()[0]
                with self.assertRaisesRegex(RuntimeError, 'job not found'):
                    await asyncio.wait_for(execute_step(flow, 'go', {'input': {}, 'steps': {}}, run, self.receive, self.sink), 10)
                self.assertEqual(state['submitted'], 1)  # no second placement while the first may run
                self.assertEqual(info(self.root, run, 'go')['state'], 'failed')

    async def test_engine_timeout_while_waiting_keeps_the_reason(self):
        from flowkit.distributed import execute_step
        self.write_registry([('pc-a', None)])
        flow = parse({'name': 'expiring', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
        with patch('flowkit.distributed.step_timeout_seconds', lambda step: .5):
            with self.assertRaises(asyncio.TimeoutError):  # what the engine's execution timeout does
                await asyncio.wait_for(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'expired-run', self.receive, self.sink), .8)
        record = info(self.root, 'expired-run', 'go')
        self.assertEqual((record['state'], record['waiting_reason']), ('timed_out', 'offline'))
        self.assertIn('offline', record['message'])
        self.assertEqual(record['timeout_minutes'], 60)

    async def test_user_cancel_and_engine_interruption_are_distinct(self):
        from flowkit.distributed import execute_step, mark_cancelled
        self.write_registry([('pc-a', None)])
        flow = parse({'name': 'cancelled', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(1)"'}}})
        for run, by_user in (('user-run', True), ('engine-run', False)):
            task = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, run, self.receive, self.sink))
            await until(lambda: info(self.root, run, 'go').get('waiting_reason') == 'offline', message='waiting')
            if by_user: self.assertTrue(mark_cancelled(run))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            record = info(self.root, run, 'go')
            self.assertEqual(record['state'], 'cancelled' if by_user else 'interrupted')
            self.assertEqual(record['waiting_reason'], 'offline')

    async def test_malformed_inventory_does_not_abort_placement(self):
        from flowkit.fleet_matching import validate_launch
        from flowkit.workers import fleet_status
        servers = []
        for name, report in (('bad-list', []), ('bad-section', {'commands': ['python'], 'agents': 'x'}),
                             ('bad-entry', {'commands': {'python': 'installed'}}), ('good', None)):
            app, _ = fake_worker(name, capabilities=report)
            servers.append((name, (await self.serve(app)).make_url('')))
        self.write_registry(servers)
        public, _ = await fleet_status([{'command': 'python'}])
        decision = validate_launch({'command': 'python'}, public)
        self.assertEqual((decision['status'], decision['candidates']), ('ready', ['good']))
        statuses = {m['worker']: m['status'] for m in decision['workers']}
        self.assertEqual(statuses, {'bad-list': 'unknown', 'bad-section': 'unknown', 'bad-entry': 'unknown', 'good': 'compatible'})

    async def test_restart_selection_checks_step_software(self):
        import threading, urllib.request, urllib.error, flow_host
        auth = patch.dict(os.environ, {'FLOW_HOST_TOKEN': 'restart-selection-test-token'})
        auth.start(); self.addCleanup(auth.stop)
        report = {'commands': {'fleet-test-tool': entry('absent'), 'python': entry('installed')}, 'agents': {}, 'refreshed_at': stamp()}
        app, _ = fake_worker('pc-a', capabilities=report)
        server = await self.serve(app)
        self.write_registry([('pc-a', server.make_url('')), ('pc-b', None)])
        (self.root/'canonical'/'sel-run').mkdir(parents=True)
        host = flow_host.serve(None, '127.0.0.1', 0)
        threading.Thread(target=host.serve_forever, daemon=True).start()
        self.addCleanup(host.server_close); self.addCleanup(host.shutdown)
        flow = {'name': 'sel-flow', 'steps': {'go': {'kind': 'program', 'run': 'fleet-test-tool --x', 'requires_command': 'fleet-test-tool'},
                                              'other': {'kind': 'program', 'run': 'python a.py'}}}
        def select(step, worker, with_flow=True):
            body = json.dumps({'step': step, 'worker': worker, **({'flow': flow} if with_flow else {})}).encode()
            request = urllib.request.Request(f'http://127.0.0.1:{host.server_address[1]}/execution/sel-run/selection', body,
                                             {'Content-Type': 'application/json', 'Authorization': f'Bearer {os.environ["FLOW_HOST_TOKEN"]}'})
            try:
                with urllib.request.urlopen(request, timeout=20) as response: return response.status, json.loads(response.read())
            except urllib.error.HTTPError as exc: return exc.code, json.loads(exc.read())
        status, body = await asyncio.to_thread(select, 'go', {'id': 'pc-a'})
        self.assertEqual(status, 400)
        self.assertIn('fleet-test-tool is not installed on pc-a', body['error'])
        self.assertIn('Install', body['error'])
        self.assertFalse((self.root/'canonical'/'sel-run'/'.flow-selections'/'go.json').exists())
        self.assertEqual((await asyncio.to_thread(select, 'other', {'id': 'pc-a'}))[0], 200)
        self.assertEqual((await asyncio.to_thread(select, 'go', {'id': 'pc-b'}))[0], 200)  # offline: waits
        self.assertEqual((await asyncio.to_thread(select, 'go', {'id': 'pc-a'}, False))[0], 200)  # legacy: selector only


class LaunchRouteTests(unittest.TestCase):
    def setUp(self):
        import threading, flow_host
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        path = Path(self.tmp.name)/'workers.yaml'
        path.write_text('workers:\n- {id: pc-a, pools: [build], tags: [linux]}\n', encoding='utf-8')
        self.env = patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(path), 'FLOW_HOST_TOKEN': ''})
        self.env.start(); self.addCleanup(self.env.stop)
        self.server = flow_host.serve(None, '127.0.0.1', 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close); self.addCleanup(self.server.shutdown)

    def validate(self, flow, execution=None):
        import urllib.request
        body = json.dumps({'flow': flow, **({'execution': execution} if execution else {})}).encode()
        request = urllib.request.Request(f'http://127.0.0.1:{self.server.server_address[1]}/launch/validate', body, {'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=20) as response: return json.loads(response.read())

    def test_launch_blocks_confirmed_mismatch_and_waits_for_offline_match(self):
        flow = {'name': 'routed', 'steps': {'go': {'kind': 'program', 'run': 'python a.py'}}}
        waiting = self.validate(flow, {'default': {'pool': 'build', 'tags': ['linux']}})
        self.assertEqual(waiting['status'], 'wait')
        self.assertEqual(waiting['steps']['go']['waiting_reason'], 'offline')
        blocked = self.validate(flow, {'default': {'pool': 'build', 'tags': ['gpu']}})
        self.assertEqual(blocked['status'], 'blocked')
        self.assertIn('gpu', blocked['steps']['go']['message'])
        local = self.validate(flow)
        self.assertEqual((local['status'], local['fleet']), ('ready', False))
        self.assertNotIn('url', json.dumps([waiting, blocked]))

    def post(self, body):
        import urllib.request, urllib.error
        request = urllib.request.Request(f'http://127.0.0.1:{self.server.server_address[1]}/launch/validate', json.dumps(body).encode(), {'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(request, timeout=20) as response: return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc: return exc.code, json.loads(exc.read())

    def test_standalone_card_launch_check_uses_its_agent_selection_and_engine(self):
        checked = stamp(60)
        cache = Path(os.environ['FLOW_WORKERS_FILE'] + '.inventory.json')
        cache.write_text(json.dumps({'pc-a': {'agents': {'codex': {'status': 'installed', 'checked_at': checked}}, 'commands': {}}}), encoding='utf-8')
        status, waiting = self.post({'card': {'agent': 'codex', 'worker': {'pool': 'build'}}, 'engine': 'hatchet'})
        self.assertEqual(status, 200, waiting)
        self.assertEqual((waiting['status'], waiting['fleet']), ('wait', True))
        self.assertEqual(waiting['card']['waiting_reason'], 'offline')
        detected = waiting['card']['workers'][0]['detected']
        self.assertEqual(detected, [{'kind': 'agent', 'name': 'codex', 'status': 'installed', 'checked_at': checked, 'cached': True}])
        status, blocked = self.post({'card': {'agent': 'codex', 'worker': {'tags': ['gpu']}}, 'engine': 'prefect'})
        self.assertEqual((status, blocked['status']), (200, 'blocked'))
        self.assertIn('gpu', blocked['card']['message'])
        self.assertTrue(blocked['card']['resolution'])
        for bad in ({'card': {'agent': 'codex', 'worker': {}}}, {'card': {'agent': 'nope', 'worker': {'id': 'pc-a'}}},
                    {'card': {'agent': 'codex', 'worker': {'url': 'http://x'}}}):
            with self.subTest(bad=bad): self.assertEqual(self.post(bad)[0], 400)
        self.assertNotIn('url', json.dumps([waiting, blocked]))

    def test_offline_step_shows_its_last_known_check(self):
        checked = stamp(30)
        Path(os.environ['FLOW_WORKERS_FILE'] + '.inventory.json').write_text(
            json.dumps({'pc-a': {'commands': {'python': {'status': 'installed', 'checked_at': checked}}, 'agents': {}}}), encoding='utf-8')
        flow = {'name': 'routed', 'steps': {'go': {'kind': 'program', 'run': 'python a.py'}}}
        decision = self.validate(flow, {'default': {'pool': 'build'}})['steps']['go']
        self.assertEqual(decision['status'], 'wait')
        self.assertEqual(decision['workers'][0]['detected'], [{'kind': 'command', 'name': 'python', 'status': 'installed', 'checked_at': checked, 'cached': True}])


if __name__ == '__main__': unittest.main()
