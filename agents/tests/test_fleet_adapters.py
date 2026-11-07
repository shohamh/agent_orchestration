"""Engine adapters on the shared execution service, and workspace integrity across PCs.

Every engine mode (Hatchet portable flows and cards, Prefect served deployments and cards, the
Trigger.dev local development worker bridge) places work through the same requirement object,
waiting lifecycle and per-PC admission gate. Engine servers are not needed: the adapters are
built with the pinned SDKs offline and their task functions invoked with stand-in contexts, and
the services are real worker services on this machine, each root simulating one PC.
Live engine servers are not exercised here (see board/workers_smoke_test.py).
"""
import subprocess
import asyncio
import base64
import json
import os
import re
import sys
import tempfile
import time
import unittest
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

AGENTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AGENTS))
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer
from flowkit.capacity import CapacityGate
from flowkit.definition import parse
from flowkit.worker_service import make_app as production_make_app
from agents.tests.worker_fixture import make_app

DAY = timedelta(hours=24)


async def until(predicate, timeout=30, message='condition'):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline: raise AssertionError(f'timed out waiting for {message}')
        await asyncio.sleep(.05)


def offline_hatchet():
    """A real pinned-SDK client that never connects (a syntactically valid, unsigned token)."""
    encode = lambda value: base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip('=')
    token = encode({'alg': 'HS256'}) + '.' + encode({'sub': '00000000-0000-0000-0000-000000000000',
        'server_url': 'http://localhost:1', 'grpc_broadcast_address': 'localhost:1'}) + '.unsigned'
    with patch.dict(os.environ, {'HATCHET_CLIENT_TOKEN': token, 'HATCHET_CLIENT_TLS_STRATEGY': 'none'}):
        from hatchet_sdk import Hatchet
        return Hatchet()


def fake_hatchet_context(outputs=None):
    logs = []
    async def aio_log(line, raise_on_error=False): logs.append(line)
    async def aio_put_stream(value): pass
    return SimpleNamespace(task_output=lambda task: {'output': (outputs or {}).get(task.name)}, step_run_id='step-run-1',
                           workflow_run_id='hatchet-run-1', aio_log=aio_log, aio_put_stream=aio_put_stream), logs


class Recorder:
    """Stands in for the shared placement entry points to capture what an adapter submits."""
    def __init__(self, result):
        self.calls, self.result = [], result

    async def __call__(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.result


FLOW = {'name': 'adapter-flow', 'worker': {'pool': 'build'}, 'steps': {
    'a': {'kind': 'program', 'run': 'python -c "print(1)"', 'retries': 2, 'timeout_minutes': 30, 'worker': {'tags': ['gpu']}},
    'b': {'kind': 'agent', 'agent': 'demo', 'prompt': 'review', 'needs': ['a']}}}


class AdapterWiringTests(unittest.IsolatedAsyncioTestCase):
    """Each adapter forwards the requirement and its engine identity to the shared service."""

    async def test_hatchet_flow_tasks_wait_past_default_schedule_timeout(self):
        import flow_worker_hatchet
        flow = parse(FLOW)
        recorder = Recorder({'stdout': '1'})
        with patch('flowkit.distributed.execute_step', recorder):  # adapters bind it when building
            workflow = flow_worker_hatchet.build_workflow(offline_hatchet(), flow)
        tasks = {t.name: t for t in workflow.tasks}
        self.assertEqual(set(tasks), {'a', 'b'})
        for sid, task in tasks.items():
            # The SDK default (five minutes) would cancel a step queued behind waiting steps.
            self.assertGreaterEqual(task.schedule_timeout, DAY, sid)
            self.assertEqual(task.execution_timeout, timedelta(minutes=flow.steps[sid].timeout_minutes))
            self.assertEqual(task.retries, flow.steps[sid].retries)
        ctx, _ = fake_hatchet_context()
        payload = workflow.input_validator.validate_python({'flowExecution': {'steps': {'a': {'id': 'pc-b'}}}})
        self.assertEqual(await tasks['a']._fn(payload, ctx), {'output': {'stdout': '1'}})
        (got_flow, step, context, run_id, _, _), kwargs = recorder.calls[0]
        self.assertEqual((got_flow.name, step, run_id, kwargs), ('adapter-flow', 'a', 'hatchet-run-1', {'engine': 'hatchet'}))
        self.assertEqual(context['_flow_execution'], {'steps': {'a': {'id': 'pc-b'}}})  # the run's override

    async def test_hatchet_card_accepts_requirement_and_waits_past_default(self):
        import hatchet_worker
        recorder = Recorder({'state': 'done', 'agent': 'demo', 'workspace': 'remote', 'text': ''})
        with patch('flowkit.distributed.execute_session', recorder):
            standalone = hatchet_worker.build_task(offline_hatchet())
            task = standalone._task
            self.assertGreaterEqual(task.schedule_timeout, DAY)
            self.assertEqual(task.execution_timeout, DAY)
            validate = standalone.input_validator.validate_python
            payload = validate({'goal': 'fix it', 'agent': 'demo', 'worker': {'pool': 'build', 'tags': ['gpu']}})
            self.assertIsNone(validate({'goal': 'fix it'}).worker)  # existing cards: no selection
            ctx, logs = fake_hatchet_context()
            self.assertEqual((await task._fn(payload, ctx))['state'], 'done')
        (config, run_id, _, sink), kwargs = recorder.calls[0]
        self.assertEqual((config['worker'], config['agent'], run_id, kwargs),
                         ({'pool': 'build', 'tags': ['gpu']}, 'demo', 'hatchet-run-1', {'engine': 'hatchet'}))
        await sink({'type': 'status', 'state': 'working'})
        self.assertEqual(logs, ['EVT {"type": "status", "state": "working"}'])  # event schema unchanged

    def prefect_patches(self, captured):
        def fake_task(**options):
            def wrap(fn):
                captured[fn.__name__] = (fn, options)
                return fn
            return wrap
        @asynccontextmanager
        async def client(): yield SimpleNamespace()
        logger = SimpleNamespace(lines=[], info=lambda line: logger.lines.append(line))
        return logger, [patch('prefect.task', fake_task), patch('prefect.flow', lambda **kw: (lambda fn: fn)),
                        patch('prefect.get_client', client), patch('prefect.get_run_logger', lambda: logger),
                        patch('prefect.runtime.flow_run.get_id', lambda: 'prefect-run-1'),
                        patch('prefect.runtime.task_run.get_id', lambda: 'task-run-1')]

    async def test_prefect_flow_steps_report_engine(self):
        with tempfile.TemporaryDirectory() as state, patch.dict(os.environ, {'FLOW_STATE_DIR': state}):
            import flow_worker_prefect
        captured = {}
        _, patches = self.prefect_patches(captured)
        for p in patches: p.start(); self.addCleanup(p.stop)
        flow = parse(FLOW)
        recorder = Recorder({'stdout': '1'})
        with patch('flowkit.distributed.execute_step', recorder):
            flow_worker_prefect.build_flow(flow)
            run, options = captured['a']
            self.assertEqual((options['retries'], options['timeout_seconds']), (2, 30 * 60))
            self.assertEqual(await run({'flowExecution': {'default': {'pool': 'build'}}}, {}), {'stdout': '1'})
        (_, step, context, run_id, _, _), kwargs = recorder.calls[0]
        self.assertEqual((step, run_id, kwargs), ('a', 'prefect-run-1', {'engine': 'prefect'}))
        self.assertEqual(context['_flow_execution'], {'default': {'pool': 'build'}})

    async def test_prefect_card_accepts_requirement(self):
        import prefect_worker
        captured = {}
        logger, patches = self.prefect_patches(captured)
        for p in patches: p.start(); self.addCleanup(p.stop)
        recorder = Recorder({'state': 'done', 'agent': 'demo', 'workspace': 'remote', 'text': ''})
        with patch('flowkit.distributed.execute_session', recorder):
            card = prefect_worker.build_flow()
            await card(goal='fix it', agent='demo', worker={'id': 'pc-b'})
            await card(goal='local card', agent='demo')
        (config, run_id, _, sink), kwargs = recorder.calls[0]
        self.assertEqual((config['worker'], run_id, kwargs), ({'id': 'pc-b'}, 'prefect-run-1', {'engine': 'prefect'}))
        self.assertIsNone(recorder.calls[1][0][0]['worker'])
        await sink({'type': 'agent', 'text': 'hi'})
        self.assertEqual(logger.lines, ['EVT {"type": "agent", "text": "hi"}'])

    def test_trigger_runtime_queued_steps_do_not_expire(self):
        # Static check (no Trigger.dev server offline): development runs expire after ten minutes in
        # the queue unless a ttl is set, which would fail steps queued behind waiting steps.
        source = (AGENTS.parent/'trigger.dev/example-project/src/flows-runtime.ts').read_text(encoding='utf-8')
        self.assertEqual(len(re.findall(r'\bttl: QUEUE_TTL\b', source)), 2, 'parent and step tasks set a queue ttl')
        self.assertRegex(source, r'const QUEUE_TTL = "1d"')


def write_registry(path, entries):
    """entries: (id, url or None, extra yaml lines)."""
    path.write_text('workers:\n' + ''.join(
        f'- id: {wid}\n' + ''.join(f'  {line}\n' for line in extra) + (f'  url: {url}\n' if url else '')
        for wid, url, extra in entries), encoding='utf-8')


class ServiceFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.registry = self.root/'workers.yaml'
        self.canonical = self.root/'canonical'
        env = patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(self.registry), 'FLOW_WORKSPACES': str(self.canonical),
                                      'TEST_WORKER_TOKEN': 'adapter-secret-token'})
        env.start(); self.addCleanup(env.stop)
        for target, value in (('flowkit.distributed.PLACEMENT_POLL', .1), ('flowkit.distributed.PROBE_INTERVAL', 0),
                              ('flowkit.distributed.TIMEOUT_SLACK', 0),
                              ('flowkit.worker_service.AUTHORIZATION_TIMEOUT', 2), ('flowkit.worker_service.COORDINATOR_STALE', 2)):
            p = patch(target, value); p.start(); self.addCleanup(p.stop)
        # Controlled machine samples: these tests are about adapters and placement, not this host's
        # load (CPU/RAM admission is covered by test_fleet_capacity). Slots and cooldown stay real.
        p = patch('flowkit.capacity.MachineSampler.sample', lambda sampler: (1.0, 1.0)); p.start(); self.addCleanup(p.stop)
        self.events = []

    async def sink(self, event): self.events.append(event)

    async def receive(self): await asyncio.Event().wait()

    async def service(self, name, slots=1, token=None, gate=None, middlewares=()):
        if gate is not None:
            app = production_make_app(worker_id=name, root=self.root/name, slots=slots, token=token, capacity=gate)
        else:
            app = make_app(worker_id=name, root=self.root/name, slots=slots, token=token)
        app.middlewares.extend(middlewares)
        server = TestServer(app); await server.start_server()
        self.addAsyncCleanup(server.close)
        return app, server

    def record(self, run, key):
        try: return json.loads((self.canonical/run/'.flow-workers'/f'{key}.json').read_text(encoding='utf-8'))
        except (OSError, ValueError): return {}


def responder(inbox, events, finish=True):
    """A user at the board: deny the demo's permission request, answer, then mark the card done."""
    async def sink(event):
        events.append(event)
        if event.get('type') == 'approval': await inbox.put({'type': 'approval', 'id': event['id'], 'optionId': 'deny'})
        if finish and event.get('type') == 'status' and event.get('state') == 'awaiting_review':
            continued = any(e.get('type') == 'user' and e.get('text') == 'thanks' for e in events)
            await inbox.put({'type': 'done'} if continued else {'type': 'message', 'text': 'thanks'})
    return sink


class AttachedSourceTests(ServiceFixture):
    """What is attached in New card, through real execution services."""
    READ = "python -c \"from pathlib import Path as P; print(P('context/files/notes.txt').read_text(), P('repos/origin/a.txt').read_text())\""

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.origin = self.root/'origin'
        self.origin.mkdir()
        git = lambda *args: subprocess.run(['git', *args], cwd=self.origin, check=True, capture_output=True)
        git('init', '-b', 'main')
        (self.origin/'a.txt').write_text('from git', encoding='utf-8')
        git('add', '.')
        git('-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-m', 'a')
        _, self.plain = await self.service('pc-a')
        _, self.gpu = await self.service('pc-b')
        write_registry(self.registry, [('pc-a', self.plain.make_url(''), ['pools: [build]', 'tags: []']),
                                       ('pc-b', self.gpu.make_url(''), ['pools: [gpu]', 'tags: []'])])
        from flowkit import launch_context
        self.upload = launch_context.save_upload('notes.txt', base64.b64encode(b'from the form').decode())
        self.context = {'files': [{'id': self.upload['id'], 'name': 'notes.txt', 'source': 'x', 'target': 'context/files/notes.txt'}],
                        'repositories': [{'url': str(self.origin), 'target': 'repos/origin', 'shallow': True}]}

    async def test_a_program_card_runs_in_a_chosen_pool_with_what_was_attached(self):
        from flowkit.distributed import execute_session
        config = {'goal': self.READ, 'agent': 'program', 'workspace': '', 'simulateFailures': 0, 'resumeSessionId': '',
                  'worker': {'pools': ['gpu', 'other']}, 'context': self.context, 'programInput': 'typed'}
        result = await asyncio.wait_for(execute_session(config, 'program-run', self.receive, self.sink, engine='hatchet'), 60)
        self.assertEqual(result['state'], 'done')
        self.assertIn('from the form from git', result['output']['stdout'])
        self.assertEqual(self.record('program-run', 'card')['worker']['id'], 'pc-b')
        self.assertEqual(self.record('program-run', 'context-setup')['state'], 'done')
        tools = {e['id']: e for e in self.events if e.get('type') == 'tool'}
        self.assertEqual(tools['program']['status'], 'ok')
        self.assertEqual(tools['context:repositories']['status'], 'ok')
        self.assertEqual(self.events[-1]['state'], 'done')

    async def test_a_program_card_fails_with_its_exit_and_reports_it(self):
        from flowkit.distributed import execute_session
        config = {'goal': 'python -c "print(1/0)"', 'agent': 'program', 'worker': {'pools': ['build']}}
        with self.assertRaisesRegex(RuntimeError, 'code 1'):
            await asyncio.wait_for(execute_session(config, 'failed-run', self.receive, self.sink, engine='hatchet'), 60)
        self.assertEqual({e['id']: e for e in self.events if e.get('type') == 'tool'}['program']['status'], 'error')

    async def test_a_bad_source_fails_the_card_before_it_starts(self):
        from flowkit.distributed import execute_session
        config = {'goal': 'python -c "print(1)"', 'agent': 'program', 'worker': {'pools': ['build']},
                  'context': {'repositories': [{'url': str(self.root/'missing'), 'target': 'repos/missing'}]}}
        with self.assertRaisesRegex(RuntimeError, 'Could not prepare.*repos/missing'):
            await asyncio.wait_for(execute_session(config, 'bad-source', self.receive, self.sink, engine='hatchet'), 60)
        self.assertEqual(self.record('bad-source', 'card'), {}, 'the card itself never started')
        tools = {e['id']: e for e in self.events if e.get('type') == 'tool'}
        self.assertEqual(tools['context:repositories']['status'], 'error')
        self.assertIn('does not appear to be a git repository', tools['context:repositories']['output'])
        self.assertTrue(any(e['type'] == 'error' and 'repos/missing' in e['message'] for e in self.events))

    async def test_an_agent_card_is_told_what_was_attached_and_works_in_that_folder(self):
        from flowkit.distributed import execute_session
        inbox, events = asyncio.Queue(), []
        config = {'goal': 'Summarize', 'agent': 'demo', 'workspace': '', 'simulateFailures': 0, 'resumeSessionId': '',
                  'worker': {'pools': ['build']}, 'context': self.context, 'programInput': ''}
        result = await asyncio.wait_for(execute_session(config, 'agent-run', inbox.get, responder(inbox, events), engine='prefect'), 60)
        self.assertEqual(result['state'], 'done')
        workspace = Path(result['workspace'])
        self.assertEqual(workspace.name, 'agent-run')
        self.assertIn(str(self.root/'pc-a'), result['workspace'], 'on the worker')

    async def test_every_step_of_a_flow_sees_the_attachments_prepared_once(self):
        from flowkit.definition import parse
        from flowkit.distributed import execute_step
        flow = parse({'name': 'attached', 'steps': {
            'left': {'kind': 'program', 'run': self.READ},
            'right': {'kind': 'program', 'worker': {'pool': 'gpu'}, 'run': self.READ},
            'join': {'kind': 'program', 'needs': ['left', 'right'], 'run': self.READ}}})
        base = {'input': {}, 'steps': {}, '_flow_execution': {'default': {'pools': ['build']}}, '_flow_context': self.context}
        left, right = await asyncio.wait_for(asyncio.gather(
            execute_step(flow, 'left', base, 'flow-run', self.receive, self.sink, engine='hatchet'),
            execute_step(flow, 'right', base, 'flow-run', self.receive, self.sink, engine='hatchet')), 90)
        self.assertIn('from the form from git', left['stdout'])
        self.assertIn('from the form from git', right['stdout'])
        self.assertEqual({left_or_right: self.record('flow-run', left_or_right)['worker']['id'] for left_or_right in ('left', 'right')}, {'left': 'pc-a', 'right': 'pc-b'})
        # Parallel steps got one copy: cloning twice would make their joining step conflict.
        joined = await asyncio.wait_for(execute_step(flow, 'join', base, 'flow-run', self.receive, self.sink, engine='hatchet'), 60)
        self.assertIn('from the form from git', joined['stdout'])
        self.assertEqual(self.record('flow-run', 'context-setup')['state'], 'done')


class CardPlacementTests(ServiceFixture):
    async def test_card_runs_on_matching_worker_with_interactive_input(self):
        from flowkit.distributed import execute_session
        _, plain = await self.service('pc-a')
        _, gpu = await self.service('pc-b')
        write_registry(self.registry, [('pc-a', plain.make_url(''), ['pools: [build]', 'tags: [linux]']),
                                       ('pc-b', gpu.make_url(''), ['pools: [build]', 'tags: [linux, gpu]'])])
        inbox, events = asyncio.Queue(), []
        config = {'goal': 'Fix the cart', 'agent': 'demo', 'workspace': '', 'simulateFailures': 0, 'resumeSessionId': '',
                  'worker': {'pool': 'build', 'tags': ['gpu']}}
        result = await asyncio.wait_for(execute_session(config, 'card-run', inbox.get, responder(inbox, events), engine='prefect'), 60)
        self.assertEqual(set(result), {'state', 'agent', 'workspace', 'text'})  # run_session's output schema
        self.assertEqual((result['state'], result['agent']), ('done', 'demo'))
        self.assertNotIn(str(self.root), result['workspace'])  # never a coordinator path
        record = self.record('card-run', 'card')
        self.assertEqual((record['state'], record['worker']['id'], record['engine']), ('done', 'pc-b', 'prefect'))
        self.assertEqual(record['requested'], {'pool': 'build', 'tags': ['gpu']})
        self.assertIn({'type': 'user', 'text': 'thanks', 'attachments': [], 'queued': False}, [
            {k: e.get(k) for k in ('type', 'text', 'attachments', 'queued')} for e in events if e.get('type') == 'user'])
        self.assertFalse([e for e in events if e.get('type') == 'worker'], 'card transcripts keep their event schema')
        self.assertEqual(events[-1]['type'], 'status')
        self.assertEqual(events[-1]['state'], 'done')

    async def test_card_without_selection_runs_here_as_before(self):
        from flowkit.distributed import execute_session
        write_registry(self.registry, [('pc-a', None, ['pools: [build]'])])
        inbox, events = asyncio.Queue(), []
        result = await asyncio.wait_for(execute_session({'goal': 'local', 'agent': 'demo', 'worker': None}, 'local-card',
                                                         inbox.get, responder(inbox, events), engine='hatchet'), 60)
        self.assertEqual(result['state'], 'done')
        self.assertFalse((self.canonical/'local-card').exists())  # no fleet record, no placement

    async def test_fleet_card_never_assumes_a_host_workspace(self):
        from flowkit.distributed import execute_session
        app, server = await self.service('pc-a')
        write_registry(self.registry, [('pc-a', server.make_url(''), ['pools: [build]'])])
        for extra in ({'workspace': str(self.root)}, {'resumeSessionId': 'abc'}):
            with self.subTest(extra), self.assertRaisesRegex(ValueError, 'workspace'):
                await execute_session({'goal': 'x', 'agent': 'demo', 'worker': {'pool': 'build'}, **extra}, 'ws-card', self.receive, self.sink)
        import uuid
        async with ClientSession() as client:  # the worker refuses one too (defense in depth)
            body = {'id': str(uuid.uuid4()), 'files': {}, 'assets': {}, 'start': {
                'session': {'goal': 'x', 'agent': 'demo', 'workspace': str(self.root)}, 'runId': 'ws-card'}}
            response = await client.post(server.make_url('/jobs'), json=body)
            self.assertEqual(response.status, 400)
            self.assertIn('workspace', (await response.json())['error'])
        self.assertEqual(app['jobs'], {})

    async def test_card_cancelled_while_waiting_never_starts(self):
        from flowkit.distributed import execute_session, mark_cancelled
        write_registry(self.registry, [('pc-a', None, ['pools: [build]'])])  # awaiting connection
        task = asyncio.create_task(execute_session({'goal': 'x', 'agent': 'demo', 'worker': {'id': 'pc-a'}}, 'wait-card',
                                                   self.receive, self.sink, engine='hatchet'))
        await until(lambda: self.record('wait-card', 'card').get('waiting_reason') == 'offline', message='card waiting')
        self.assertNotIn('url', json.dumps(self.record('wait-card', 'card')))
        self.assertTrue(mark_cancelled('wait-card'))
        task.cancel()
        with self.assertRaises(asyncio.CancelledError): await task
        record = self.record('wait-card', 'card')
        self.assertEqual((record['state'], record['waiting_reason'], record['engine']), ('cancelled', 'offline', 'hatchet'))
        self.assertIsNone(record['worker'])


class SharedGateTests(ServiceFixture):
    async def test_two_engine_adapters_share_one_worker_slot(self):
        from flowkit.distributed import execute_session, execute_step
        gate = CapacityGate(self.root/'pc-a-capacity.db', slots=1)  # the PC's one worker-wide slot
        now = time.monotonic()
        for i in range(11): gate.observe(1, 1, now - 10 + i)
        app, server = await self.service('pc-a', slots=4, gate=gate)
        # The registry's one slot is pushed to the PC's gate, which enforces it across engines
        # (the service itself would run four jobs at once).
        write_registry(self.registry, [('pc-a', server.make_url(''), ['pools: [build]', 'slots: 1'])])
        inbox, card_events = asyncio.Queue(), []
        card = asyncio.create_task(execute_session({'goal': 'hold the slot', 'agent': 'demo', 'worker': {'id': 'pc-a'}}, 'gate-card',
                                                   inbox.get, responder(inbox, card_events, finish=False), engine='prefect'))
        await until(lambda: self.record('gate-card', 'card').get('state') == 'running', message='card running')
        await until(lambda: any(e.get('state') == 'awaiting_review' for e in card_events), 60, 'card turn finished')
        marker = self.root/'step-started.txt'
        flow = parse({'name': 'gate-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program',
            'run': f'python -c "import time; open(r\'{marker}\', \'w\').write(str(time.time()))"'}}})
        step = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'gate-run', self.receive, self.sink, engine='hatchet'))
        await until(lambda: self.record('gate-run', 'go').get('waiting_reason') == 'full_slots', message='gate full_slots')
        await asyncio.sleep(1.5)
        self.assertEqual([j.started for j in app['jobs'].values()].count(True), 1)  # only one engine's work starts
        self.assertFalse(marker.exists())
        self.assertEqual(self.record('gate-run', 'go')['engine'], 'hatchet')
        released = time.time()
        await inbox.put({'type': 'done'})
        self.assertEqual((await asyncio.wait_for(card, 30))['state'], 'done')
        await asyncio.wait_for(step, 60)
        self.assertGreater(float(marker.read_text()), released)
        self.assertEqual((self.record('gate-run', 'go')['state'], self.record('gate-card', 'card')['state']), ('done', 'done'))


def stdio_bridge_env(root, registry):
    return {**os.environ, 'FLOW_WORKERS_FILE': str(registry), 'FLOW_WORKSPACES': str(root/'canonical'), 'PYTHONUTF8': '1'}


class FlowIntegrityTests(ServiceFixture):
    def flow(self, marker):
        counter = (self.root/'attempts.txt').as_posix()
        fail_once = ("import pathlib,sys; p=pathlib.Path(r'" + counter + "'); n=int(p.read_text()) if p.exists() else 0; "
                     "p.write_text(str(n+1)); pathlib.Path('built.txt').write_text('artifact from a'); sys.exit(1 if n==0 else 0)")
        return parse({'name': 'multi-pc', 'worker': {'pool': 'build'}, 'steps': {
            'a': {'kind': 'program', 'run': f'python -c "{fail_once}"', 'retries': 1, 'worker': {'tags': ['cpu']}},
            'b': {'kind': 'agent', 'agent': 'demo', 'prompt': 'Review {steps.a.output}', 'review': True, 'needs': ['a']}}})

    async def test_multi_pc_flow_per_engine_with_overrides_retries_and_input(self):
        from flowkit.distributed import execute_step
        _, cpu = await self.service('pc-a', token='adapter-secret-token')
        _, review = await self.service('pc-b', token='adapter-secret-token')
        write_registry(self.registry, [('pc-a', cpu.make_url(''), ['pools: [build]', 'tags: [cpu]', 'token_env: TEST_WORKER_TOKEN']),
                                       ('pc-b', review.make_url(''), ['pools: [review]', 'token_env: TEST_WORKER_TOKEN'])])
        for engine in ('hatchet', 'prefect'):
            with self.subTest(engine):
                (self.root/'attempts.txt').unlink(missing_ok=True)
                flow, run = self.flow(None), f'{engine}-run'
                execution = {'steps': {'b': {'id': 'pc-b'}}}  # run-level override of step b
                context = {'input': {}, 'steps': {}, '_flow_execution': execution}
                with self.assertRaisesRegex(RuntimeError, 'exited with code 1'):
                    await execute_step(flow, 'a', context, run, self.receive, self.sink, engine=engine)
                first = self.record(run, 'a')
                self.assertEqual(first['state'], 'failed')
                output = await execute_step(flow, 'a', context, run, self.receive, self.sink, engine=engine)  # the engine's retry
                retried = self.record(run, 'a')
                self.assertNotEqual(retried['job'], first['job'])
                self.assertEqual((retried['state'], retried['worker']['id'], retried['engine']), ('done', 'pc-a', engine))
                inbox, events = asyncio.Queue(), []
                async def review_sink(event):
                    events.append(event)
                    if event.get('type') == 'approval': await inbox.put({'type': 'approval', 'id': event['id'], 'optionId': 'deny'})
                    if event.get('type') == 'status' and event.get('state') == 'awaiting_review': await inbox.put({'type': 'done'})
                await asyncio.wait_for(execute_step(flow, 'b', {**context, 'steps': {'a': {'output': output}}}, run,
                                                    inbox.get, review_sink, engine=engine), 60)
                record = self.record(run, 'b')
                self.assertEqual((record['state'], record['worker']['id'], record['engine']), ('done', 'pc-b', engine))
                self.assertEqual(record['requested'], {'id': 'pc-b'})
                self.assertIn('approval', [e.get('type') for e in events])
                text = json.dumps([self.record(run, s) for s in ('a', 'b')]) + json.dumps(self.events + events)
                self.assertNotIn('adapter-secret-token', text)
                self.assertNotIn(str(cpu.make_url('')).rstrip('/'), text)

    async def test_trigger_bridge_places_through_the_shared_gate(self):
        _, server = await self.service('pc-a')
        write_registry(self.registry, [('pc-a', server.make_url(''), ['pools: [build]'])])
        flow = parse({'name': 'trigger-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(\'bridged\')"'}}})
        from flowkit.activation import Store
        import yaml
        store = Store(self.root/'drafts', self.root/'state')
        rev = store.write_snapshot(flow.name, yaml.safe_dump(flow.to_json()))
        definition = {**flow.to_json(), '_asset_snapshot': store.asset_reference(flow.name, rev)}
        process = await asyncio.create_subprocess_exec(sys.executable, '-u', '-m', 'flowkit.stdio', cwd=AGENTS,
            env={**stdio_bridge_env(self.root, self.registry), 'FLOW_STATE_DIR': str(self.root/'state')}, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        start = {'type': 'start', 'flow': definition, 'step': 'go', 'context': {'input': {}, 'steps': {}}, 'runId': 'run_trigger1'}
        process.stdin.write((json.dumps(start) + '\n').encode()); await process.stdin.drain()
        stdout, stderr = await asyncio.wait_for(process.communicate(), 90)
        self.assertEqual(process.returncode, 0, stderr.decode(errors='replace'))
        lines = [json.loads(line) for line in stdout.decode().splitlines() if line.strip()]
        self.assertIn('bridged', lines[-1]['output']['stdout'])
        record = self.record('run_trigger1', 'go')
        self.assertEqual((record['state'], record['worker']['id'], record['engine']), ('done', 'pc-a', 'trigger'))

    async def test_parallel_branch_cannot_run_a_newer_revision_while_old_run_in_flight(self):
        from flowkit.distributed import execute_step
        app, server = await self.service('pc-a', slots=2)
        write_registry(self.registry, [('pc-a', server.make_url(''), ['pools: [build]', 'slots: 2'])])
        gate_file = self.root/'release.txt'
        old = parse({'name': 'revised', 'worker': {'pool': 'build'}, 'steps': {
            'slow': {'kind': 'program', 'run': f'python -c "import pathlib,time; [time.sleep(.1) for _ in iter(lambda: pathlib.Path(r\'{gate_file}\').exists(), True)]"'},
            'fast': {'kind': 'program', 'run': 'python -c "print(\'old fast\')"'}}})
        new = parse({**old.to_json(), 'steps': {**old.to_json()['steps'], 'fast': {'kind': 'program', 'run': 'python -c "print(\'NEW fast\')"'}}})
        slow = asyncio.create_task(execute_step(old, 'slow', {'input': {}, 'steps': {}}, 'rev-run', self.receive, self.sink, engine='hatchet'))
        await until(lambda: self.record('rev-run', 'slow').get('state') == 'running', message='old revision running')
        # The engine worker was updated meanwhile: the old run's other branch now arrives with the new revision.
        with self.assertRaisesRegex(ValueError, 'definition has changed'):
            await execute_step(new, 'fast', {'input': {}, 'steps': {}}, 'rev-run', self.receive, self.sink, engine='hatchet')
        self.assertEqual(len(app['jobs']), 1)  # nothing was placed for the new revision
        self.assertFalse((self.canonical/'rev-run'/'.flow-artifacts'/'fast.json').exists())
        self.assertEqual((await execute_step(old, 'fast', {'input': {}, 'steps': {}}, 'rev-run', self.receive, self.sink))['stdout'].strip(), 'old fast')
        gate_file.write_text('go')
        await asyncio.wait_for(slow, 30)

    async def test_old_revision_executes_old_assets_after_draft_changes(self):
        from flowkit.activation import Store
        from flowkit.distributed import execute_step
        _, server = await self.service('pc-a')
        write_registry(self.registry, [('pc-a', server.make_url(''), ['pools: [build]'])])
        drafts = self.root/'drafts'; drafts.mkdir()
        script = drafts/'task.py'; script.write_text("print('old asset')", encoding='utf-8')
        store = Store(drafts, self.root/'state')
        rev = store.write_snapshot('asset-flow', 'name: asset-flow\nworker: {pool: build}\nsteps:\n  go: {kind: program, run: "python {flow_dir}/task.py"}\n')
        script.write_text("print('new asset')", encoding='utf-8')
        flow = store.load({'asset-flow': rev})[0]['asset-flow']
        with patch.dict(os.environ, {'FLOW_STATE_DIR': str(self.root/'state')}):
            output = await execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'asset-run', self.receive, self.sink, engine='prefect')
        self.assertEqual(output['stdout'].strip(), 'old asset')

    async def test_failed_update_keeps_old_run_on_its_revision(self):
        from flowkit.activation import Store
        from flowkit.distributed import execute_step
        app, server = await self.service('pc-a')
        write_registry(self.registry, [('pc-a', server.make_url(''), ['pools: [build]'])])
        store = Store(self.root/'drafts', self.root/'state')
        state_env = patch.dict(os.environ, {'FLOW_STATE_DIR': str(self.root/'state')})
        state_env.start(); self.addCleanup(state_env.stop)
        v1 = 'name: updated\nworker: {pool: build}\nsteps:\n  a: {kind: program, run: "python -c \\"print(1)\\""}\n  b: {kind: program, needs: [a], run: "python -c \\"print(\'v1\')\\""}\n'
        v2 = v1.replace("'v1'", "'v2'")
        rev1, rev2 = store.write_snapshot('updated', v1), store.write_snapshot('updated', v2)
        manifest = {'version': 1, 'flows': {'updated': {'revision': rev1, 'engines': {'hatchet': {'revision': rev1, 'state': 'active', 'error': None}}}}}
        store.write_manifest(manifest)
        served = lambda: store.load_active('hatchet')[0]['updated']
        await execute_step(served(), 'a', {'input': {}, 'steps': {}}, 'upd-run', self.receive, self.sink, engine='hatchet')
        # Activating v2 failed for this engine: it keeps serving v1, so the in-flight run finishes on v1.
        manifest['flows']['updated'].update(revision=rev2)
        manifest['flows']['updated']['engines']['hatchet'].update(state='failed', error='worker did not start')
        store.write_manifest(manifest)
        result = await execute_step(served(), 'b', {'input': {}, 'steps': {'a': {'output': None}}}, 'upd-run', self.receive, self.sink, engine='hatchet')
        self.assertEqual(result['stdout'].strip(), 'v1')
        # Once v2 is served, the old run never mixes it in: its steps fail clearly, nothing is placed.
        manifest['flows']['updated']['engines']['hatchet'].update(revision=rev2, state='active', error=None)
        store.write_manifest(manifest)
        jobs = len(app['jobs'])
        with self.assertRaisesRegex(ValueError, 'definition has changed'):
            await execute_step(served(), 'b', {'input': {}, 'steps': {'a': {'output': None}}}, 'upd-run', self.receive, self.sink, engine='hatchet')
        self.assertEqual(len(app['jobs']), jobs)

    async def test_branch_conflict_across_pcs_fails_before_placement(self):
        from flowkit.distributed import execute_step
        app_a, a = await self.service('pc-a')
        app_b, b = await self.service('pc-b')
        write_registry(self.registry, [('pc-a', a.make_url(''), ['pools: [build]']), ('pc-b', b.make_url(''), ['pools: [build]'])])
        write = lambda text: f'python -c "import pathlib; pathlib.Path(\'shared.txt\').write_text(\'{text}\')"'
        flow = parse({'name': 'conflict', 'worker': {'pool': 'build'}, 'steps': {
            'base': {'kind': 'program', 'run': write('base')},
            'left': {'kind': 'program', 'needs': ['base'], 'run': write('left'), 'worker': {'id': 'pc-a'}},
            'right': {'kind': 'program', 'needs': ['base'], 'run': write('right'), 'worker': {'id': 'pc-b'}},
            'join': {'kind': 'program', 'needs': ['left', 'right'], 'run': 'python -c "print(1)"'}}})
        for sid in ('base', 'left', 'right'):
            await execute_step(flow, sid, {'input': {}, 'steps': {}}, 'conflict-run', self.receive, self.sink)
        placed = len(app_a['jobs']) + len(app_b['jobs'])
        with self.assertRaisesRegex(ValueError, 'Workspace conflict for shared.txt'):
            await execute_step(flow, 'join', {'input': {}, 'steps': {}}, 'conflict-run', self.receive, self.sink)
        self.assertEqual(len(app_a['jobs']) + len(app_b['jobs']), placed)

    async def test_stale_attempt_never_publishes(self):
        from flowkit.distributed import execute_step
        result_waiting, deliver_result = asyncio.Event(), asyncio.Event()
        @web.middleware
        async def hold_old_result(request, handler):
            response = await handler(request)
            if request.method == 'GET' and request.path.startswith('/jobs/') and response.status == 200 and json.loads(response.body).get('state') == 'done':
                result_waiting.set()
                await deliver_result.wait()
            return response
        _, a = await self.service('pc-a', middlewares=[hold_old_result])
        _, b = await self.service('pc-b')
        write_registry(self.registry, [('pc-a', a.make_url(''), ['pools: [build]']), ('pc-b', b.make_url(''), ['pools: [build]'])])
        script = "import os,pathlib; pathlib.Path('result.txt').write_text(os.environ.get('SLOW','0'))"
        flow = parse({'name': 'fenced', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': f'python -c "{script}"'}}})
        selection = lambda wid: {'input': {}, 'steps': {}, '_flow_execution': {'steps': {'go': {'id': wid}}}}
        old = current_attempt = None
        try:
            with patch.dict(os.environ, {'SLOW': '1'}):
                old = asyncio.create_task(execute_step(flow, 'go', selection('pc-a'), 'fence-run', self.receive, self.sink, engine='prefect'))
                await asyncio.wait_for(result_waiting.wait(), 30)
            stale_job = self.record('fence-run', 'go')['job']
            fence = self.canonical/'fence-run'/'.flow-attempts'/'go.json'
            current_attempt = asyncio.create_task(execute_step(flow, 'go', selection('pc-b'), 'fence-run', self.receive, self.sink, engine='prefect'))
            await until(lambda: json.loads(fence.read_text(encoding='utf-8')) != stale_job, message='replacement claimed the step')
            # Deliver a successful old result only after it is stale. Otherwise its poll can
            # legitimately observe cancellation before the replacement has claimed the fence.
            deliver_result.set()
            await asyncio.wait_for(current_attempt, 30)
            current = self.record('fence-run', 'go')
            with self.assertRaisesRegex(RuntimeError, 'superseded'):
                await asyncio.wait_for(old, 30)
            self.assertEqual(self.record('fence-run', 'go'), current)  # the stale attempt wrote nothing
            self.assertNotEqual(current['job'], stale_job)
            published = json.loads((self.canonical/'fence-run'/'.flow-artifacts'/'go.json').read_text(encoding='utf-8'))
            self.assertEqual(base64.b64decode(published['result.txt']['data']), b'0')  # the current attempt's file
        finally:
            deliver_result.set()
            tasks = [task for task in (old, current_attempt) if task is not None]
            for task in tasks:
                if not task.done(): task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def test_restart_reuses_results_and_places_only_the_restarted_subtree(self):
        from flowkit.distributed import execute_step, set_selection
        from flowkit.restart import build_plan
        app_a, a = await self.service('pc-a')
        app_b, b = await self.service('pc-b')
        write_registry(self.registry, [('pc-a', a.make_url(''), ['pools: [build]']), ('pc-b', b.make_url(''), ['pools: [build]'])])
        flow = parse({'name': 'restartable', 'worker': {'pool': 'build'}, 'steps': {
            'a': {'kind': 'program', 'run': 'python -c "import pathlib; pathlib.Path(\'a.txt\').write_text(\'a\')"', 'worker': {'id': 'pc-a'}},
            'b': {'kind': 'program', 'needs': ['a'], 'run': 'python -c "import pathlib; print(pathlib.Path(\'a.txt\').read_text())"', 'worker': {'id': 'pc-a'}}}})
        context = {'input': {}, 'steps': {}}
        first = await execute_step(flow, 'a', context, 'source-run', self.receive, self.sink)
        await execute_step(flow, 'b', {**context, 'steps': {'a': {'output': first}}}, 'source-run', self.receive, self.sink)
        plan = build_plan(flow, 'b', {'a': {'column': 'done'}, 'b': {'column': 'done'}}, 'source-run')
        placed = len(app_a['jobs'])
        restart = {**context, '_flow_restart': plan}
        self.assertEqual(await execute_step(flow, 'a', restart, 'restart-run', self.receive, self.sink), first)  # reused, not placed
        self.assertEqual(len(app_a['jobs']), placed)
        set_selection('restart-run', 'b', {'id': 'pc-b'})
        output = await execute_step(flow, 'b', {**restart, 'steps': {'a': {'output': first}}}, 'restart-run', self.receive, self.sink)
        self.assertEqual(output['stdout'].strip(), 'a')  # the source run's artifact reached the other PC
        self.assertEqual(self.record('restart-run', 'b')['worker']['id'], 'pc-b')
        self.assertEqual(len(app_b['jobs']), 1)

    async def test_authenticated_artifacts_on_isolated_remote_workspaces(self):
        from flowkit.distributed import execute_step
        from flowkit.workers import catalog
        app_a, a = await self.service('pc-a', token='adapter-secret-token')
        _, b = await self.service('pc-b', token='adapter-secret-token')
        write_registry(self.registry, [('pc-a', a.make_url(''), ['pools: [build]', 'token_env: TEST_WORKER_TOKEN']),
                                       ('pc-b', b.make_url(''), ['pools: [build]', 'token_env: TEST_WORKER_TOKEN'])])
        cwd = 'python -c "import os,pathlib; pathlib.Path(\'where.txt\').write_text(os.getcwd()); print(os.getcwd())"'
        flow = parse({'name': 'isolated', 'worker': {'pool': 'build'}, 'steps': {
            'a': {'kind': 'program', 'run': cwd, 'worker': {'id': 'pc-a'}},
            'b': {'kind': 'program', 'needs': ['a'], 'run': 'python -c "import os,pathlib; print(pathlib.Path(\'where.txt\').read_text()); print(os.getcwd())"', 'worker': {'id': 'pc-b'}}}})
        out_a = await execute_step(flow, 'a', {'input': {}, 'steps': {}}, 'iso-run', self.receive, self.sink)
        out_b = await execute_step(flow, 'b', {'input': {}, 'steps': {'a': {'output': out_a}}}, 'iso-run', self.receive, self.sink)
        seen, where_b = (Path(line).resolve() for line in out_b['stdout'].strip().splitlines())
        where_a, root = Path(out_a['stdout'].strip()).resolve(), self.root.resolve()
        self.assertTrue(where_a.is_relative_to(root/'pc-a'), where_a)  # each PC runs in its own job workspace
        self.assertTrue(where_b.is_relative_to(root/'pc-b'), where_b)
        self.assertEqual(seen, where_a)  # the artifact crossed PCs through the coordinator
        for where in (where_a, where_b): self.assertFalse(where.is_relative_to(self.canonical.resolve()))
        async with ClientSession() as client:
            self.assertEqual((await client.get(a.make_url('/jobs/' + next(iter(app_a['jobs']))))).status, 401)
            self.assertEqual((await client.post(a.make_url('/jobs'), json={})).status, 401)
        public = json.dumps(await catalog()) + json.dumps([self.record('iso-run', s) for s in 'ab']) + json.dumps(self.events)
        self.assertNotIn('adapter-secret-token', public)
        self.assertNotIn('127.0.0.1', public)

    async def test_wait_beyond_five_minutes_does_not_expire(self):
        # Simulated clock: the shared wait has no deadline of its own, only the step's timeout_minutes.
        from flowkit import distributed
        from flowkit.distributed import execute_step
        write_registry(self.registry, [('pc-a', None, ['pools: [build]'])])
        offset = {'value': 0.0}
        clock = SimpleNamespace(time=time.time, monotonic=lambda: time.monotonic() + offset['value'])
        flow = parse({'name': 'patient', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(\'late\')"', 'timeout_minutes': 20}}})
        with patch.object(distributed, 'time', clock):
            task = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'late-run', self.receive, self.sink, engine='hatchet'))
            await until(lambda: self.record('late-run', 'go').get('waiting_reason') == 'offline', message='waiting')
            offset['value'] = 6 * 60
            await asyncio.sleep(.5)
            self.assertFalse(task.done())
            _, server = await self.service('pc-a')
            write_registry(self.registry, [('pc-a', server.make_url(''), ['pools: [build]'])])
            self.assertIn('late', (await asyncio.wait_for(task, 30))['stdout'])
        self.assertEqual(self.record('late-run', 'go')['state'], 'done')


class LocalRevisionTests(unittest.IsolatedAsyncioTestCase):
    async def test_local_flow_run_pins_its_revision(self):
        from flowkit.executor import run_step
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {'FLOW_WORKSPACES': tmp}):
            old = parse({'name': 'local-rev', 'steps': {'a': {'kind': 'program', 'run': 'echo a'}, 'b': {'kind': 'program', 'run': 'echo b'}}})
            new = parse({'name': 'local-rev', 'steps': {'a': {'kind': 'program', 'run': 'echo a'}, 'b': {'kind': 'program', 'run': 'echo NEW'}}})
            async def sink(event): pass
            async def receive(): await asyncio.Event().wait()
            await run_step(old, 'a', {'input': {}, 'steps': {}}, 'local-run', receive, sink)
            with self.assertRaisesRegex(ValueError, 'definition has changed'):
                await run_step(new, 'b', {'input': {}, 'steps': {}}, 'local-run', receive, sink)


if __name__ == '__main__': unittest.main()
