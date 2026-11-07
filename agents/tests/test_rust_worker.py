"""The Rust worker as the oracle sees it: the real Python coordinator code drives the agent-worker binary as a PC.

Set AGENT_WORKER_BIN to the binary (default: worker/target/{release,debug}/agent-worker). Skipped when there is none.
"""
import asyncio
import json
import base64
import hashlib
import os
import socket
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
import uuid
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
from flowkit.definition import parse
from agents.tests.test_fleet_matching import FleetFixture, info, until

ROOT = Path(__file__).resolve().parents[2]
TOKEN = 'rust-oracle-token-0123456789abcdefghij'
EXE = '.exe' if os.name == 'nt' else ''


# Built by worker/ci/build-confluence-runtime.py; without one the Confluence tests are skipped.
RUNTIME_CANDIDATES = (os.environ.get('AGENT_WORKER_CONFLUENCE_RUNTIME'),
                      ROOT/'dist'/'worker'/f"confluence-runtime-{'windows' if os.name == 'nt' else 'linux'}.tar.gz")
RUNTIME = next((Path(p).resolve() for p in RUNTIME_CANDIDATES if p and Path(p).exists()), None)
RUNTIME_ARCHIVE = next((Path(p).resolve() for p in RUNTIME_CANDIDATES if p and Path(p).is_file()), None)
CONFLUENCE_TOKEN = 'rust-confluence-fixture-secret'
CONFLUENCE_USER = 'rust-worker@example.invalid'
# Both consumer routes include the same cold runtime unpack and real source job.
CONFLUENCE_JOB_TIMEOUT = 120


def find_binary():
    candidates = [os.environ.get('AGENT_WORKER_BIN')] + [str(ROOT / 'worker' / 'target' / kind / f'agent-worker{EXE}') for kind in ('debug', 'release')]
    return next((c for c in candidates if c and Path(c).is_file()), None)


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


class Service:
    """One agent-worker process serving on loopback, shared by the tests (a fresh gate needs a few seconds to warm up)."""
    instance = None

    def __init__(self, server=None):
        from flowkit.executor import _own
        self.dir = tempfile.TemporaryDirectory()
        home = Path(self.dir.name)
        self.root = home/'runtime' if server else home
        self.port = free_port()
        self.jobs = set()
        # The agents the tests drive: real JSON-RPC subprocesses, without model credits.
        here = Path(__file__).resolve().parent
        env = {**os.environ, 'AGENT_WORKER_TEST_FAST': '1', 'FLOW_WORKER_TOKEN': TOKEN, 'PYTHONUTF8': '1',
               'AGENT_WORKER_ACP_OMP': json.dumps([sys.executable, str(here/'fake_acp_agent.py')]),
               'AGENT_WORKER_ACP_PI': json.dumps([sys.executable, str(here/'fake_acp_terminal_agent.py')]),
               'AGENT_WORKER_ACP_CODEX': json.dumps([sys.executable, '-c', 'raise SystemExit("codex cannot start")']),
               'RUST_CONFLUENCE_TOKEN': CONFLUENCE_TOKEN, 'RUST_CONFLUENCE_USER': CONFLUENCE_USER,
               'CME_EXPORT__OUTPUT_PATH': str(home/'wrong-export-location')}
        if server:
            # An ephemeral installed-runner fixture: no install, registration, startup entry or real fleet.
            (home/'worker.json').write_text(json.dumps({'name': 'rust-pc', 'uuid': UUID, 'server': server,
                                                       'port': self.port, 'slots': 2}), encoding='utf-8')
            (home/'token').write_text(TOKEN, encoding='utf-8')
            env.pop('AGENT_WORKER_CONFLUENCE_RUNTIME', None)
            env.update(PYTHONHOME=str(home/'not-a-python'), PYTHONPATH=str(home/'not-the-exporter'))
            command = [find_binary(), '--home', str(home), 'run']
        else:
            if RUNTIME: env['AGENT_WORKER_CONFLUENCE_RUNTIME'] = str(RUNTIME)
            command = [find_binary(), 'serve', '--id', 'rust-pc', '--uuid', UUID, '--port', str(self.port), '--slots', '2', '--root', str(self.root)]
        self.log = open(home/'service.log', 'wb')
        try:
            self.process = subprocess.Popen(command, env=env, stdout=subprocess.DEVNULL, stderr=self.log, start_new_session=os.name != 'nt')
        except BaseException:
            self.log.close()
            self.dir.cleanup()
            raise
        self.tree = _own(self.process.pid)
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                health = self.get('/health')
                if health['id'] == 'rust-pc' and health.get('uuid') == UUID:
                    # Source-job fixtures exercise import semantics, not admission under unrelated host load.
                    status, body = self.post('/capacity/settings', {'slots': 2, 'cpu_limit': 100, 'ram_limit': 100},
                                             {'X-Flow-Gate-ID': health['capacity']['gate_id'], 'X-Flow-Settings-Revision': str(time.time_ns())})
                    if status != 200:
                        self.stop()
                        raise RuntimeError(f'Could not configure fixture capacity: {body}')
                    return
            except OSError: time.sleep(.1)
        self.stop()
        raise RuntimeError('the Rust worker did not start')

    def get(self, path, token=TOKEN):
        request = urllib.request.Request(f'http://127.0.0.1:{self.port}{path}', headers={'Authorization': f'Bearer {token}'} if token else {})
        with urllib.request.urlopen(request, timeout=5) as response: return json.loads(response.read())

    def post(self, path, body, headers=None, token=TOKEN):
        request = urllib.request.Request(f'http://127.0.0.1:{self.port}{path}', data=json.dumps(body).encode(), method='POST',
                                         headers={'Content-Type': 'application/json', **({'Authorization': f'Bearer {token}'} if token else {}), **(headers or {})})
        try:
            with urllib.request.urlopen(request, timeout=10) as response: return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc: return exc.code, json.loads(exc.read() or b'null')

    def cancel(self, job_id):
        request = urllib.request.Request(f'http://127.0.0.1:{self.port}/jobs/{job_id}', method='DELETE',
                                         headers={'Authorization': f'Bearer {TOKEN}'})
        with urllib.request.urlopen(request, timeout=15) as response: return json.loads(response.read())

    def stop(self):
        from flowkit.executor import _kill
        for job_id in list(self.jobs):
            try: self.cancel(job_id)
            except OSError: pass
        self.process.terminate()
        try: self.process.wait(10)
        except subprocess.TimeoutExpired:
            _kill(self.process, self.tree)
            self.tree = None
            self.process.wait(10)
        finally:
            _kill(self.process, self.tree)
            self.log.close()
        try: self.dir.cleanup()
        except OSError: pass


UUID = '3f2b8a1e-5c4d-4e6f-9a7b-0c1d2e3f4a5b'


def service():
    if Service.instance is None: Service.instance = Service()
    return Service.instance


def tearDownModule():
    if Service.instance: Service.instance.stop()


@unittest.skipUnless(find_binary(), 'build the worker first: cargo build in worker/')
class RustWorkerTests(FleetFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.pc = service()
        env = patch.dict(os.environ, {'FLOW_WORKERS_ENV_FILE': str(self.root/'workers.env'), 'RUST_PC_TOKEN': TOKEN})
        env.start(); self.addCleanup(env.stop)
        self.registry.write_text('workers:\n- id: rust-pc\n  pools: [build]\n  url: http://127.0.0.1:%d\n  token_env: RUST_PC_TOKEN\n  slots: 2\n  cpu_limit: 100\n  ram_limit: 100\n  settings_revision: %d\n'
                                 % (self.pc.port, time.time_ns()))

    def flow(self, run, **step):
        return parse({'name': 'rust-flow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': run, **step}}})

    async def run_step(self, flow, run_id, timeout=60, context=None):
        from flowkit.distributed import execute_step
        return await asyncio.wait_for(execute_step(flow, 'go', {'input': {}, 'steps': {}, **(context or {})}, run_id, self.receive, self.sink), timeout)

    # ------------------------------------------------------------------ protocol
    def test_health_names_the_worker_and_its_capacity(self):
        health = self.pc.get('/health')
        self.assertEqual((health['id'], health['slots'], health['active'], health['draining']), ('rust-pc', 2, 0, False))
        self.assertIn('gate_id', health['capacity'])
        self.assertEqual(health['capacity']['window_seconds'], 10)

    def test_every_route_needs_the_token(self):
        for token in (None, 'wrong'):
            with self.assertRaises(urllib.error.HTTPError) as caught: self.pc.get('/health', token=token)
            self.assertEqual(caught.exception.code, 401)

    def test_capabilities_report_installed_and_absent_software(self):
        inventory = self.pc.get('/capabilities')
        self.assertEqual(set(inventory), {'agents', 'commands', 'refreshed_at'})
        status, refreshed = self.pc.post('/capabilities/refresh', {'commands': ['python', 'definitely-not-a-program-xyz']})
        self.assertEqual(status, 200)
        self.assertEqual(refreshed['commands']['definitely-not-a-program-xyz']['status'], 'absent')
        self.assertEqual(self.pc.post('/capabilities/refresh', {'commands': ['a\nb']})[0], 400)
        self.assertEqual(refreshed['agents']['demo']['status'], 'installed')
        self.assertEqual(refreshed['agents']['omp']['status'], 'installed')

    def test_limits_are_pushed_with_a_revision_and_stale_pushes_refused(self):
        gate = self.pc.get('/health')['capacity']['gate_id']
        revision = str(time.time_ns())
        status, _ = self.pc.post('/capacity/settings', {'slots': 2, 'cpu_limit': 100, 'ram_limit': 100}, {'X-Flow-Gate-ID': gate, 'X-Flow-Settings-Revision': revision})
        self.assertEqual(status, 200)
        status, body = self.pc.post('/capacity/settings', {'slots': 1, 'cpu_limit': 50, 'ram_limit': 50}, {'X-Flow-Gate-ID': gate, 'X-Flow-Settings-Revision': str(int(revision) - 5)})
        self.assertEqual((status, body['error']), (400, 'Stale worker settings revision'))
        self.assertEqual(self.pc.post('/capacity/settings', {'slots': 2, 'cpu_limit': 100, 'ram_limit': 100}, {'X-Flow-Gate-ID': 'other', 'X-Flow-Settings-Revision': str(int(revision) + 1)})[0], 400)
        self.assertEqual(self.pc.post('/capacity/settings', {'slots': 0, 'cpu_limit': 100, 'ram_limit': 100})[0], 400)
        self.assertEqual(self.pc.post('/capacity/settings', {'slots': 2, 'cpu_limit': 100})[0], 400)

    def test_drain_validates_its_body(self):
        self.assertEqual(self.pc.post('/drain', {'drain': 'yes'})[0], 400)
        self.assertEqual(self.pc.post('/drain', {})[0], 400)
        self.assertEqual(self.pc.post('/drain', {'drain': False})[1]['draining'], False)

    # ------------------------------------------------------------------ jobs through the coordinator's own code
    async def test_a_program_step_runs_through_placement_and_returns_its_output(self):
        output = await self.run_step(self.flow('python -c "print(\'from rust\')"'), 'rust-run-1')
        self.assertEqual(output, {'stdout': 'from rust'})
        self.assertEqual(info(self.root, 'rust-run-1', 'go')['state'], 'done')
        self.assertEqual([e['worker']['id'] for e in self.events if e.get('type') == 'worker'], ['rust-pc'])
        logs = [e for e in self.events if e.get('type') == 'log']
        self.assertEqual([(e['stream'], e['text']) for e in logs], [('stdout', 'from rust')])

    async def test_the_step_receives_its_context_and_may_return_json_through_flow_output(self):
        code = ("import json, os, sys; ctx = json.load(sys.stdin); assert os.environ['FLOW_STEP'] == 'go'; "
                "json.dump({'topic': ctx['input']['topic'], 'env_ok': json.loads(os.environ['FLOW_CONTEXT']) == ctx}, open(os.environ['FLOW_OUTPUT'], 'w'))")
        flow = self.flow('python -c "%s"' % code.replace('"', "'") if os.name == 'nt' else "python -c %s" % json.dumps(code))
        output = await self.run_step(flow, 'rust-run-ctx', context={'input': {'topic': 'edge caching'}})
        self.assertEqual(output, {'topic': 'edge caching', 'env_ok': True})

    async def test_template_values_reach_the_command_quoted(self):
        flow = parse({'name': 'tpl', 'worker': {'pool': 'build'}, 'input': {'who': "it's a name"}, 'steps': {'go': {'kind': 'program',
                      'run': 'python -c "import sys; print(sys.argv[1])" {{ input.who }}'}}})
        output = await self.run_step(flow, 'rust-run-tpl', context={'input': {'who': "it's a name"}})
        self.assertEqual(output, {'stdout': "it's a name"})

    async def test_a_program_card_with_a_repository_and_an_upload_runs_on_this_worker(self):
        from flowkit import launch_context
        from flowkit.distributed import execute_session
        origin = self.root/'origin'
        origin.mkdir()
        git = lambda *args: subprocess.run(['git', *args], cwd=origin, check=True, capture_output=True)
        git('init', '-b', 'main')
        (origin/'a.txt').write_text('from git', encoding='utf-8')
        git('add', '.')
        git('-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-m', 'a')
        import base64
        saved = launch_context.save_upload('notes.txt', base64.b64encode(b'from the form').decode())
        read = "python -c \"from pathlib import Path as P; print(P('context/files/notes.txt').read_text(), P('repos/origin/a.txt').read_text())\""
        config = {'goal': read, 'agent': 'program', 'worker': {'pools': ['other', 'build']}, 'programInput': 'typed',
                  'context': {'files': [{'id': saved['id'], 'name': 'notes.txt', 'source': 'x', 'target': 'context/files/notes.txt'}],
                              'repositories': [{'url': str(origin), 'target': 'repos/origin', 'shallow': True}]}}
        result = await asyncio.wait_for(execute_session(config, 'rust-card', self.receive, self.sink, engine='hatchet'), 60)
        self.assertEqual(result['state'], 'done')
        self.assertIn('from the form from git', result['output']['stdout'])
        tools = {e['id']: e for e in self.events if e.get('type') == 'tool'}
        self.assertEqual((tools['context:repositories']['status'], tools['program']['status']), ('ok', 'ok'))
        self.assertEqual(self.events[-1]['state'], 'done')

    async def test_a_failing_step_fails_with_its_exit_code_and_last_output(self):
        with self.assertRaisesRegex(RuntimeError, r"exited with code 3: .*boom"):
            await self.run_step(self.flow('python -c "import sys; print(\'boom\'); sys.exit(3)"'), 'rust-run-fail')

    async def test_workspace_files_travel_in_and_changed_files_come_back(self):
        from flowkit.artifacts import snapshot
        flow = self.flow('python -c "import pathlib; p = pathlib.Path(\'out.txt\'); p.write_text(\'made on the PC\'); print(len(list(pathlib.Path(\'.\').glob(\'*\'))))"')
        output = await self.run_step(flow, 'rust-run-files')
        self.assertEqual(output['stdout'], '1')
        root = self.root / 'canonical' / 'rust-run-files'
        artifact = json.loads((root / '.flow-artifacts' / 'go.json').read_text())
        import base64
        self.assertEqual(base64.b64decode(artifact['out.txt']['data']), b'made on the PC')

    async def test_cancelling_stops_the_step_on_the_pc(self):
        from flowkit.distributed import execute_step, mark_cancelled
        marker = self.root / 'survived.txt'
        flow = self.flow('python -c "import time, pathlib; time.sleep(30); pathlib.Path(r\'%s\').write_text(\'x\')"' % marker.as_posix(), timeout_minutes=2)
        run = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'rust-run-cancel', self.receive, self.sink))
        try:
            await until(lambda: info(self.root, 'rust-run-cancel', 'go').get('state') == 'running', timeout=40, message='the step running on the PC')
            await asyncio.sleep(1)
            self.assertTrue(mark_cancelled('rust-run-cancel'))
            run.cancel()
            with self.assertRaises(asyncio.CancelledError): await run
            await asyncio.sleep(2)
            health = self.pc.get('/health')
            self.assertEqual(health['active'], 0, 'the cancelled job no longer counts as running')
            self.assertEqual(health['capacity']['leases'], 0, 'and holds no capacity')
            self.assertFalse(marker.exists())
        finally:
            if not run.done(): run.cancel()
            await asyncio.gather(run, return_exceptions=True)

    async def test_a_draining_worker_takes_no_new_work_and_the_step_waits_then_runs(self):
        self.assertEqual(self.pc.post('/drain', {'drain': True})[1]['draining'], True)
        try:
            run = asyncio.create_task(self.run_step(self.flow('python -c "print(\'after the update\')"'), 'rust-run-drain'))
            await until(lambda: info(self.root, 'rust-run-drain', 'go').get('waiting_reason') in ('offline', 'updating'), timeout=30, message='waiting while the PC updates')
            self.assertFalse(run.done())
            self.pc.post('/drain', {'drain': False})
            output = await asyncio.wait_for(run, 60)
            self.assertEqual(output['stdout'], 'after the update')
        finally:
            self.pc.post('/drain', {'drain': False})

    async def test_a_job_id_that_was_cancelled_can_never_run_again(self):
        job_id = '11111111-2222-4333-8444-555555555555'
        gate = self.pc.get('/health')['capacity']['gate_id']
        request = urllib.request.Request(f'http://127.0.0.1:{self.pc.port}/jobs/{job_id}', method='DELETE', headers={'Authorization': f'Bearer {TOKEN}', 'X-Flow-Gate-ID': gate})
        with urllib.request.urlopen(request, timeout=5) as response: self.assertEqual(json.loads(response.read()), {'ok': True, 'terminated': True})
        config = {'id': job_id, 'gate_id': gate, 'files': {}, 'assets': {}, 'start': {'flow': {'steps': {'go': {'kind': 'program', 'run': 'echo hi'}}}, 'step': 'go', 'context': {}, 'runId': 'r1'}}
        status, body = self.pc.post('/jobs', config)
        self.assertEqual((status, body['error']), (400, 'A terminated job ID cannot be reused'))

    async def test_unsafe_submissions_are_refused(self):
        gate = self.pc.get('/health')['capacity']['gate_id']
        base = {'id': '22222222-2222-4333-8444-555555555555', 'gate_id': gate, 'files': {}, 'assets': {}, 'start': {'flow': {'steps': {'go': {'kind': 'program', 'run': 'echo hi'}}}, 'step': 'go', 'context': {}, 'runId': 'r1'}}
        self.assertEqual(self.pc.post('/jobs', {**base, 'id': 'not-a-uuid'})[0], 400)
        self.assertEqual(self.pc.post('/jobs', {**base, 'gate_id': 'other'})[1]['error'], 'Worker capacity gate identity changed')
        self.assertEqual(self.pc.post('/jobs', {**base, 'start': {**base['start'], 'runId': '../x'}})[1]['error'], 'Invalid run id')
        self.assertEqual(self.pc.post('/jobs', {**base, 'start': {**base['start'], 'step': 'nope'}})[1]['error'], 'Unknown step')
        self.assertEqual(self.pc.post('/jobs', {**base, 'files': {'../evil': {'data': '', 'mode': 420}}})[0], 400)
        for session in ({'goal': 'x', 'workspace': 'C:/somewhere'}, {'goal': 'x', 'resumeSessionId': 'y'}, {'goal': ' '}, {'goal': 'x', 'agent': 'nope'}, {'goal': 'x', 'other': 1}):
            self.assertEqual(self.pc.post('/jobs', {**base, 'start': {'session': session, 'runId': 'r1'}})[0], 400, session)
        self.assertEqual(self.pc.post('/jobs', {**base, 'start': {'attached': 'x', 'runId': 'r1'}})[0], 400)

    # ------------------------------------------------------------------ agents
    def human(self, inbox, events, approve='allow', answers=None, say=None):
        """Someone at the board: answers what is asked, then marks the card done."""
        said = []
        async def sink(event):
            events.append(event)
            if event.get('type') == 'approval': await inbox.put({'type': 'approval', 'id': event['id'], 'optionId': approve})
            if event.get('type') == 'question': await inbox.put({'type': 'answer', 'id': event['id'], 'answers': answers or {'style': 'brief'}})
            if event.get('type') == 'status' and event.get('state') == 'awaiting_review':
                if say and not said:
                    said.append(True)
                    await inbox.put({'type': 'message', 'text': say})
                else:
                    await inbox.put({'type': 'done'})
        return sink

    async def card(self, run_id, goal='Fix the cart', agent='demo', timeout=60, **options):
        from flowkit.distributed import execute_session
        inbox, events = asyncio.Queue(), []
        handlers = {k: options.pop(k) for k in ('approve', 'answers', 'say') if k in options}
        config = {'goal': goal, 'agent': agent, 'workspace': '', 'simulateFailures': 0, 'resumeSessionId': '', 'worker': {'pools': ['build']}, **options}
        result = await asyncio.wait_for(execute_session(config, run_id, inbox.get, self.human(inbox, events, **handlers), engine='hatchet'), timeout)
        return result, events

    async def test_a_demo_card_runs_here_with_permission_question_and_review(self):
        result, events = await self.card('rust-demo', say='thanks')
        self.assertEqual((result['state'], result['agent']), ('done', 'demo'))
        self.assertIn('rust-pc', info(self.root, 'rust-demo', 'card')['worker']['id'])
        states = [e['state'] for e in events if e['type'] == 'status']
        for state in ('starting', 'working', 'approval', 'question', 'awaiting_review', 'done'): self.assertIn(state, states)
        self.assertEqual(states[-1], 'done')
        kinds = {e['type'] for e in events}
        self.assertLessEqual({'user', 'agent', 'approval', 'approval_resolved', 'question', 'answer', 'tool'}, kinds)
        self.assertTrue(any(e['type'] == 'user' and e.get('text') == 'thanks' for e in events), 'a follow-up message is a new turn')
        self.assertIn('Demo turn 2 finished', result['text'])
        self.assertTrue(Path(result['workspace'], 'cart.py').is_file() or result['workspace'], 'a blank card works in the demo project')

    async def test_a_wrong_answer_is_refused_and_a_denied_permission_ends_the_turn(self):
        result, events = await self.card('rust-deny', approve='deny')
        self.assertIn("won't run that tool", result['text'])
        self.assertEqual([e['decision'] for e in events if e['type'] == 'approval_resolved'], ['deny'])
        from flowkit.distributed import execute_session
        inbox, seen = asyncio.Queue(), []
        async def sink(event):
            seen.append(event)
            if event['type'] == 'approval': await inbox.put({'type': 'approval', 'id': event['id'], 'optionId': 'allow'})
            if event['type'] == 'question':
                await inbox.put({'type': 'answer', 'id': event['id'], 'answers': {'style': 'not a choice'}})
                await inbox.put({'type': 'answer', 'id': event['id'], 'answers': {'style': 'brief'}})
            if event['type'] == 'status' and event['state'] == 'awaiting_review': await inbox.put({'type': 'done'})
        await asyncio.wait_for(execute_session({'goal': 'x', 'agent': 'demo', 'worker': {'pools': ['build']}}, 'rust-answer', inbox.get, sink, engine='hatchet'), 60)
        self.assertTrue(any(e['type'] == 'error' and 'Please check your answer' in e['message'] for e in seen))
        self.assertEqual([e['answers'] for e in seen if e['type'] == 'answer'], [{'style': 'brief'}])

    async def test_failed_turns_retry_then_get_stuck_until_continued(self):
        result, events = await self.card('rust-retry', simulateFailures=1)
        self.assertEqual(result['state'], 'done')
        errors = [e for e in events if e['type'] == 'error']
        self.assertEqual((errors[0]['message'], errors[0]['will_retry']), ('Simulated failure (demo switch)', True))
        self.assertTrue(any(e['type'] == 'status' and e['state'] == 'retrying' for e in events))
        from flowkit.distributed import execute_session
        inbox, seen = asyncio.Queue(), []
        async def sink(event):
            seen.append(event)
            if event['type'] == 'status' and event['state'] == 'stuck': await inbox.put({'type': 'done'})
        await asyncio.wait_for(execute_session({'goal': 'x', 'agent': 'codex', 'worker': {'pools': ['build']}}, 'rust-stuck', inbox.get, sink, engine='hatchet'), 60)
        self.assertEqual([e['will_retry'] for e in seen if e['type'] == 'error'], [True, True, False])
        self.assertIn('codex cannot start', [e for e in seen if e['type'] == 'error'][0]['message'])

    async def test_an_interrupt_stops_the_turn_and_the_card_can_continue(self):
        from flowkit.distributed import execute_session
        inbox, seen = asyncio.Queue(), []
        async def sink(event):
            seen.append(event)
            if event['type'] == 'approval': await inbox.put({'type': 'interrupt'})
            if event['type'] == 'status' and event['state'] == 'awaiting_input': await inbox.put({'type': 'done'})
        result = await asyncio.wait_for(execute_session({'goal': 'x', 'agent': 'demo', 'worker': {'pools': ['build']}}, 'rust-interrupt', inbox.get, sink, engine='hatchet'), 60)
        self.assertEqual(result['state'], 'done')
        self.assertEqual([e['decision'] for e in seen if e['type'] == 'approval_resolved'], ['cancelled'])

    async def test_a_real_acp_process_asks_for_permission_and_answers(self):
        result, events = await self.card('rust-acp', agent='omp', approve='once', answers={'style': 'short'})
        self.assertEqual(result['text'], 'It works.')
        self.assertEqual([e['decision'] for e in events if e['type'] == 'approval_resolved'], ['allow'])
        self.assertEqual([e['commands'][0]['name'] for e in events if e['type'] == 'commands' and e['commands']], ['test'])
        self.assertEqual([e['sessionId'] for e in events if e['type'] == 'acp_state' and e.get('sessionId')][-1], 'test-session')

    async def test_acp_terminals_tools_plans_usage_modes_and_settings(self):
        from flowkit.distributed import execute_session
        inbox, seen = asyncio.Queue(), []
        changed = []
        async def sink(event):
            seen.append(event)
            if event['type'] == 'approval': await inbox.put({'type': 'approval', 'id': event['id'], 'optionId': 'allow'})
            if event['type'] == 'status' and event['state'] == 'awaiting_review':
                if not changed:
                    changed.append(True)
                    await inbox.put({'type': 'mode', 'id': 'b'})
                    await inbox.put({'type': 'config', 'id': 'model', 'value': 'y'})
                    await inbox.put({'type': 'done'})
        result = await asyncio.wait_for(execute_session({'goal': 'go', 'agent': 'pi', 'worker': {'pools': ['build']}}, 'rust-term', inbox.get, sink, engine='hatchet'), 60)
        self.assertEqual(result['text'], 'Part one, part two.', 'chunks of one message are one message')
        terminal = [e for e in seen if e['type'] == 'tool' and e['id'].startswith('terminal:')]
        self.assertEqual(terminal[-1]['status'], 'ok')
        self.assertIn('from the terminal', terminal[-1]['output'])
        self.assertTrue(any(e['type'] == 'approval' and e['summary'] == 'Run terminal command' for e in seen))
        tools = [e for e in seen if e['type'] == 'tool' and e['id'] == 't1']
        self.assertEqual((tools[0]['name'], tools[0]['summary'], tools[0]['status']), ('edit', 'Edit a file', 'running'))
        self.assertEqual((tools[1]['name'], tools[1]['summary'], tools[1]['status'], tools[1]['output']), ('edit', 'Edit a file', 'ok', 'wrote x'), 'an update repeats what the tool was')
        self.assertTrue(any(e['type'] == 'plan' for e in seen))
        self.assertTrue(any(e['type'] == 'acp_state' and e.get('usage') == {'used': 5, 'size': 10} for e in seen))

    async def test_an_interrupt_reaches_a_real_agent_through_acp(self):
        from flowkit.distributed import execute_session
        inbox, seen = asyncio.Queue(), []
        async def sink(event):
            seen.append(event)
            if event['type'] == 'status' and event['state'] == 'working': asyncio.get_running_loop().call_later(1, inbox.put_nowait, {'type': 'interrupt'})
            if event['type'] == 'status' and event['state'] == 'awaiting_input': await inbox.put({'type': 'done'})
        result = await asyncio.wait_for(execute_session({'goal': 'wait', 'agent': 'pi', 'worker': {'pools': ['build']}}, 'rust-acp-int', inbox.get, sink, engine='hatchet'), 60)
        self.assertEqual(result['state'], 'done')

    async def test_cancelling_a_card_ends_its_agent_and_frees_the_slot(self):
        from flowkit.distributed import execute_session, mark_cancelled
        async def never(): await asyncio.Event().wait()
        seen = []
        async def sink(event): seen.append(event)
        run = asyncio.create_task(execute_session({'goal': 'wait', 'agent': 'pi', 'worker': {'pools': ['build']}}, 'rust-cancel', never, sink, engine='hatchet'))
        await until(lambda: any(e['type'] == 'status' and e['state'] == 'working' for e in seen), timeout=30, message='the agent to work')
        run.cancel()
        await asyncio.gather(run, return_exceptions=True)
        await until(lambda: self.pc.get('/health')['active'] == 0, timeout=30, message='the slot to be free')

    async def test_a_flow_agent_step_finishes_with_its_answer_for_the_next_step(self):
        flow = parse({'name': 'rust-agent-flow', 'worker': {'pool': 'build'}, 'input': {'topic': 'caching'}, 'steps': {
            'think': {'kind': 'agent', 'agent': 'demo', 'prompt': 'Write about {{ input.topic }}'},
            'after': {'kind': 'program', 'needs': ['think'], 'run': 'python -c "print(1)"'}}})
        from flowkit.distributed import execute_step
        import base64
        from flowkit import launch_context
        saved = launch_context.save_upload('notes.txt', base64.b64encode(b'hello').decode())
        attached = {'files': [{'id': saved['id'], 'name': 'notes.txt', 'source': 'x', 'target': 'context/files/notes.txt'}]}
        output = await asyncio.wait_for(execute_step(flow, 'think', {'input': {'topic': 'caching'}, 'steps': {}, '_flow_context': attached}, 'rust-flow-agent', self.receive, self.sink, engine='hatchet'), 60)
        self.assertIn('Write about caching', output)
        self.assertEqual(self.events[-1]['type'], 'step')
        self.assertEqual((self.events[-1]['state'], self.events[-1]['kind']), ('done', 'agent'))
        self.assertTrue(any(e['type'] == 'tool' and 'context/files/notes.txt' in e.get('output', '') for e in self.events), 'the note reaches the prompt')

    # ------------------------------------------------------------------ environment and Confluence
    def repository(self):
        origin = self.root/'origin'
        origin.mkdir()
        git = lambda *args: subprocess.run(['git', *args], cwd=origin, check=True, capture_output=True)
        git('init', '-b', 'main')
        (origin/'hello.txt').write_text('repo context', encoding='utf-8')
        git('add', '.')
        git('-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-m', 'a')
        git('checkout', '-b', 'feature')
        (origin/'feature.txt').write_text('on the branch', encoding='utf-8')
        git('add', '.')
        git('-c', 'user.name=t', '-c', 'user.email=t@example.com', 'commit', '-m', 'b')
        git('checkout', 'main')
        return str(origin)

    async def confluence(self):
        from aiohttp.test_utils import TestServer
        from agents.tests.confluence_fixture import confluence_app
        app = confluence_app()
        server = TestServer(app)
        await server.start_server()
        self.addAsyncCleanup(server.close)
        self.confluence_requests = app['request_log']
        return app, str(server.make_url('')).rstrip('/')

    async def confluence_diagnostics(self, run_id, pc=None, job_id=None, events=None):
        pc = pc or self.pc
        records = {key: info(self.root, run_id, key) for key in ('context-confluence', 'card', 'setup_environment')}
        job_ids = {record['job'] for record in records.values() if record.get('job')}
        if job_id: job_ids.add(job_id)
        def snapshot():
            data = {'process_exit': pc.process.poll(), 'records': records, 'jobs': {}}
            for key, path in [('health', '/health'), *[(job, '/jobs/'+job) for job in sorted(job_ids)]]:
                try: result = pc.get(path)
                except Exception as error: result = {'diagnostic_error': str(error)}
                if key == 'health': data[key] = result
                else: data['jobs'][key] = result
            try:
                with open(pc.log.name, 'rb') as log:
                    log.seek(0, 2)
                    log.seek(max(0, log.tell()-12000))
                    data['worker_stderr'] = log.read().decode('utf-8', 'replace')
            except OSError as error: data['worker_stderr'] = str(error)
            cache = Path(pc.dir.name)/'confluence-runtime'
            data['runtime_cache'] = {'path': str(cache), 'entries': {}}
            try:
                if cache.is_dir():
                    for entry in cache.iterdir():
                        if entry.is_dir():
                            markers = {'python': entry/('python/python.exe' if os.name == 'nt' else 'python/bin/python3'),
                                       'runner': entry/'agents/flowkit/confluence_runner.py',
                                       'exporter': entry/'agents/flowkit/confluence_export.py'}
                            data['runtime_cache']['entries'][entry.name] = {
                                **{name: path.is_file() for name, path in markers.items()}, 'packages': (entry/'packages').is_dir()}
                        else:
                            data['runtime_cache']['entries'][entry.name] = {'bytes': entry.stat().st_size}
            except OSError as error: data['runtime_cache']['diagnostic_error'] = str(error)
            return data
        data = await asyncio.to_thread(snapshot)
        data['fixture_requests'] = self.confluence_requests[-60:]
        data['consumer_events'] = (self.events if events is None else events)[-30:]
        text = json.dumps(data, ensure_ascii=False, default=str)
        for secret in (TOKEN, CONFLUENCE_TOKEN, CONFLUENCE_USER):
            text = text.replace(secret, '<REDACTED>')
        return 'Confluence worker diagnostics: '+text

    async def source_job(self, item, run_id, files=None, pc=None):
        pc = pc or self.pc
        job_id = str(uuid.uuid4())
        status, body = pc.post('/jobs', {'id': job_id, 'start': {'runId': run_id, 'attached': {'confluence': [item]}},
                                        'files': files or {}, 'assets': {}})
        self.assertEqual(status, 200, body)
        pc.jobs.add(job_id)
        try:
            await until(lambda: pc.get(f'/jobs/{job_id}')['state'] in ('done', 'failed', 'cancelled'),
                        timeout=CONFLUENCE_JOB_TIMEOUT, message='the real Confluence source job to finish')
            result = pc.get(f'/jobs/{job_id}')
        except BaseException as exc:
            if isinstance(exc, Exception):
                exc.add_note(await self.confluence_diagnostics(run_id, pc=pc, job_id=job_id))
            try:
                pc.cancel(job_id)
                pc.jobs.discard(job_id)
            except OSError:
                pass
            raise
        pc.jobs.discard(job_id)
        return result, pc.root/job_id/'workspaces'/run_id

    async def assert_confluence_parity(self, item, target):
        from flowkit.confluence_export import MANIFEST, import_confluence
        oracle = self.root/'python-exporter'/str(uuid.uuid4())
        with patch.dict(os.environ, {'RUST_CONFLUENCE_TOKEN': CONFLUENCE_TOKEN, 'RUST_CONFLUENCE_USER': CONFLUENCE_USER}):
            await import_confluence(item, oracle)
        actual = {p.relative_to(target).as_posix(): p.read_bytes() for p in target.rglob('*') if p.is_file()}
        expected = {p.relative_to(oracle).as_posix(): p.read_bytes() for p in oracle.rglob('*') if p.is_file()}
        # The lockfile records invocation-specific exporter metadata. Markdown, attachments and index must match byte-for-byte.
        ignored = {MANIFEST, 'export.lock.json'}
        self.assertEqual({k: v for k, v in actual.items() if k not in ignored},
                         {k: v for k, v in expected.items() if k not in ignored})
        manifest, reference = json.loads(actual[MANIFEST]), json.loads(expected[MANIFEST])
        self.assertEqual(manifest['source'], reference['source'])
        self.assertEqual(manifest['pages'], reference['pages'])
        self.assertEqual(manifest['pages'], json.loads(actual['index.json']))
        self.assertEqual(manifest['files'], {name: hashlib.sha256(data).hexdigest() for name, data in actual.items() if name != MANIFEST})
        for data in actual.values():
            self.assertNotIn(CONFLUENCE_TOKEN.encode(), data)
            self.assertNotIn(CONFLUENCE_USER.encode(), data)

    @unittest.skipUnless(RUNTIME, 'build the Confluence runtime first: worker/ci/build-confluence-runtime.py')
    async def test_a_confluence_tree_attached_to_a_card_is_markdown_on_this_worker(self):
        app, base = await self.confluence()
        app['expected_authorization'] = f'Bearer {CONFLUENCE_TOKEN}'
        item = {'base_url': base, 'page_id': '1', 'target': 'context/confluence/1', 'token_env': 'RUST_CONFLUENCE_TOKEN'}
        # Markdown exceeds the shared 40-line stdout fallback; consumers use FLOW_OUTPUT for complete data.
        read = ("python -c \"import json, os; from pathlib import Path as P; w=P('context/confluence/1'); "
                "text=(w/'1.md').read_text(encoding='utf-8')+(w/'2.md').read_text(encoding='utf-8'); "
                "text+=str(len((w/'attachments/1/att10.png').read_bytes()))+str(sorted(p.name for p in w.glob('*.md'))); "
                "P(os.environ['FLOW_OUTPUT']).write_text(json.dumps({'stdout': text}), encoding='utf-8')\"")
        config = {'goal': read, 'agent': 'program', 'worker': {'pools': ['build']},
                  'context': {'confluence': [item]}}
        from flowkit.distributed import execute_session
        run = asyncio.create_task(execute_session(config, 'rust-wiki', self.receive, self.sink, engine='hatchet'))
        try:
            result = await asyncio.wait_for(asyncio.shield(run), CONFLUENCE_JOB_TIMEOUT)
        except Exception as exc:
            exc.add_note(await self.confluence_diagnostics('rust-wiki'))
            raise
        finally:
            if not run.done():
                run.cancel()
            await asyncio.gather(run, return_exceptions=True)
        self.assertEqual(result['state'], 'done', result)
        output = result['output']['stdout']
        self.assertIn('**content 1**', output)
        self.assertIn('[Relative child](2.md)', output)
        self.assertIn('attachments/1/att10.png', output)
        self.assertIn('| Name', output)
        self.assertIn('print(42)', output)
        self.assertIn("['1.md', '2.md', '3.md']", output)
        self.assertIn('68', output, 'the attachment bytes')
        self.assertIn('Macro body with', output)
        self.assertIn('Expanded macro details.', output)
        self.assertIn('café שלום 🚀', output)
        self.assertIn('[Child section](2.md#section)', output)
        self.assertIn(f'[Outside tree]({base}/confluence/spaces/TEST/pages/4/Outside)', output)
        self.assertIn('[literal](/confluence/spaces/TEST/pages/2/Child)', output, 'code examples must not be rewritten')
        self.assertIn('Bearer', app['auth_schemes'])
        self.assertTrue(any('content/search?start=1' in request for request in app['requests']))
        job = info(self.root, 'rust-wiki', 'card')['job']
        await self.assert_confluence_parity(item, self.pc.root/job/'workspaces'/'rust-wiki'/item['target'])
        self.assertFalse((Path(self.pc.dir.name)/'wrong-export-location').exists())

    @unittest.skipUnless(RUNTIME, 'build the Confluence runtime first: worker/ci/build-confluence-runtime.py')
    async def test_confluence_options_and_basic_credentials_match_the_python_exporter(self):
        from agents.tests.confluence_fixture import PNG, TEXT
        app, base = await self.confluence()
        app['expected_authorization'] = 'Basic ' + base64.b64encode(f'{CONFLUENCE_USER}:{CONFLUENCE_TOKEN}'.encode()).decode()
        for name, options in (('single', {'include_descendants': False}),
                              ('all', {'attachments': 'all'}), ('disabled', {'attachments': 'disabled'})):
            with self.subTest(mode=name):
                item = {'base_url': base, 'page_id': '1', 'target': 'wiki',
                        'token_env': 'RUST_CONFLUENCE_TOKEN', 'username_env': 'RUST_CONFLUENCE_USER', **options}
                result, workspace = await self.source_job(item, 'rust-wiki-'+name)
                self.assertEqual(result['state'], 'done', result)
                target = workspace/'wiki'
                await self.assert_confluence_parity(item, target)
                pages = json.loads((target/'index.json').read_text(encoding='utf-8'))
                self.assertEqual({p['id'] for p in pages}, {'1'} if name == 'single' else {'1', '2', '3'})
                if name == 'disabled':
                    self.assertFalse(any(p.is_file() for p in (target/'attachments').rglob('*')))
                    self.assertTrue(all(not p['attachments'] for p in pages))
                else:
                    self.assertEqual((target/'attachments/1/att10.png').read_bytes(), PNG)
                    self.assertEqual((target/'attachments/1/att11.txt').read_bytes(), TEXT)
                    if name == 'all': self.assertEqual((target/'attachments/2/att20.png').read_bytes(), PNG)
        self.assertIn('Basic', app['auth_schemes'])

    @unittest.skipUnless(RUNTIME, 'build the Confluence runtime first: worker/ci/build-confluence-runtime.py')
    async def test_failed_confluence_refresh_keeps_every_previous_export_file(self):
        from flowkit.artifacts import snapshot
        app, base = await self.confluence()
        item = {'base_url': base, 'page_id': '1', 'target': 'wiki', 'token_env': 'RUST_CONFLUENCE_TOKEN'}
        result, workspace = await self.source_job(item, 'rust-wiki-refresh-good')
        self.assertEqual(result['state'], 'done', result)
        files = snapshot(workspace)
        saved = snapshot(workspace/'wiki')
        for name in ('page', 'attachment', 'credential'):
            with self.subTest(failure=name):
                app['broken_page'] = '3' if name == 'page' else None
                app['broken_attachment'] = 'icon.png' if name == 'attachment' else None
                source = {**item, **({'token_env': 'MISSING_RUST_CONFLUENCE_TOKEN'} if name == 'credential' else {})}
                failed, refreshed = await self.source_job(source, 'rust-wiki-refresh-'+name, files=files)
                self.assertEqual(failed['state'], 'failed', failed)
                self.assertEqual(snapshot(refreshed/'wiki'), saved, 'a failed refresh must not remove or alter the good import')
                self.assertFalse(list(refreshed.glob('.confluence-stage-*')))
                if name == 'credential': self.assertIn('Missing worker credential', failed['error'])
                self.assertNotIn(CONFLUENCE_TOKEN, json.dumps(failed))

    @unittest.skipUnless(RUNTIME, 'build the Confluence runtime first: worker/ci/build-confluence-runtime.py')
    async def test_a_flow_environment_prepares_repairs_validates_and_hands_on_its_files(self):
        app, base = await self.confluence()
        origin = self.repository()
        source = self.root/'requirements.txt'
        source.write_text('from a file', encoding='utf-8')
        flow = parse({'name': 'rust-setup', 'worker': {'pool': 'build'}, 'environment': {'agent': 'demo',
            'repositories': [{'url': origin, 'target': 'repos/main'}, {'url': origin, 'ref': 'feature', 'target': 'repos/feature'}],
            'files': [{'source': str(source), 'target': 'context/requirements.txt'}],
            'confluence': [{'base_url': base, 'page_id': '1', 'target': 'context/wiki', 'token_env': 'RUST_CONFLUENCE_TOKEN'}]},
            'steps': {'read': {'kind': 'program', 'requires_command': 'python', 'run':
                "python -c \"from pathlib import Path as P; print(P('repos/main/hello.txt').read_text(), P('repos/feature/feature.txt').read_text(), P('context/requirements.txt').read_text(), P('context/wiki/index.json').is_file()); print(P('context/wiki/2.md').read_text(encoding='utf-8')); print(len(P('context/wiki/attachments/2/att20.png').read_bytes()))\""}}})
        from flowkit.distributed import execute_step
        inbox, seen, reviews, saved, rejected = asyncio.Queue(), [], [], {}, []
        remote_error = 'Confluence page tree, page versions or attachments changed; re-import the source'
        local_error = 'Confluence Markdown or attachments are missing or have changed; re-import the source'
        changes = [('page version', remote_error), ('root attachment version', remote_error),
                   ('descendant attachment version', remote_error), ('page tree', remote_error),
                   ('missing attachment', local_error), ('changed Markdown', local_error)]
        validation_start = 0
        async def sink(event):
            nonlocal validation_start
            seen.append(event)
            if event.get('type') == 'status' and event.get('state') == 'awaiting_review':
                job = info(self.root, 'rust-setup-run', 'setup_environment')['job']
                wiki = self.pc.root/job/'workspaces'/'rust-setup-run'/'context/wiki'
                review = len(reviews)
                if review:
                    self.assertLessEqual(review, len(changes), 'validation still failed after every source was repaired')
                    name, expected = changes[review-1]
                    errors = [e['message'] for e in seen[validation_start:] if e.get('type') == 'error']
                    self.assertTrue(errors, f'{name}: validation must reject the incomplete source')
                    self.assertTrue(all('context/wiki: '+expected in message for message in errors), (name, errors))
                    self.assertNotEqual(info(self.root, 'rust-setup-run', 'setup_environment').get('state'), 'done', name)
                    self.assertFalse(info(self.root, 'rust-setup-run', 'read'), f'{name}: the dependent must remain blocked')
                    rejected.append(name)
                reviews.append(review)
                # Each premature "done" must keep dependents blocked for a different real inconsistency.
                if review == 0:
                    saved['attachment'] = (wiki/'attachments/2/att20.png').read_bytes()
                    saved['page'] = (wiki/'2.md').read_bytes()
                    app['versions']['2'] = 9
                elif review == 1:
                    app['versions']['2'] = 1
                    app['attachment_versions']['att10'] = 9
                elif review == 2:
                    app['attachment_versions']['att10'] = 1
                    app['attachment_versions']['att20'] = 9
                elif review == 3:
                    app['attachment_versions']['att20'] = 1
                    app['descendants'] = ['2']
                elif review == 4:
                    app['descendants'] = ['2', '3']
                    (wiki/'attachments/2/att20.png').unlink()
                elif review == 5:
                    (wiki/'attachments/2/att20.png').write_bytes(saved['attachment'])
                    (wiki/'2.md').write_bytes(saved['page'] + b'\nchanged locally\n')
                else:
                    (wiki/'2.md').write_bytes(saved['page'])
                validation_start = len(seen)
                await inbox.put({'type': 'done'})
        try:
            output = await asyncio.wait_for(execute_step(flow, 'setup_environment', {'input': {}, 'steps': {}}, 'rust-setup-run', inbox.get, sink, engine='hatchet'), 180)
        except Exception as exc:
            exc.add_note(await self.confluence_diagnostics('rust-setup-run', events=seen))
            raise
        self.assertEqual(output['validated'], True)
        self.assertTrue(all(s['ok'] for s in output['sources']), output)
        self.assertEqual(rejected, [name for name, _ in changes], 'each distinct inconsistency must block validation until repaired')
        rows = {e['id']: e for e in seen if e.get('type') == 'tool' and e['id'].startswith('setup:')}
        self.assertEqual(set(rows), {'setup:repos/main', 'setup:repos/feature', 'setup:context/requirements.txt', 'setup:context/wiki'})
        read = await asyncio.wait_for(execute_step(flow, 'read', {'input': {}, 'steps': {'setup_environment': {'output': output}}}, 'rust-setup-run', self.receive, self.sink, engine='hatchet'), 60)
        self.assertIn('repo context on the branch from a file True', read['stdout'])
        self.assertIn('Macro body with', read['stdout'])
        self.assertTrue(read['stdout'].endswith('68'), read)

    @unittest.skipUnless(RUNTIME_ARCHIVE, 'build the Confluence runtime archive first: worker/ci/build-confluence-runtime.py')
    async def test_installed_pc_downloads_and_repairs_its_runtime_through_the_authenticated_board_relay(self):
        from aiohttp import web
        from aiohttp.test_utils import TestServer
        from board import relay_proxy
        from flowkit import update
        from flowkit.relay import make_app
        app, base = await self.confluence()
        item = {'base_url': base, 'page_id': '1', 'target': 'wiki', 'token_env': 'RUST_CONFLUENCE_TOKEN'}
        app['expected_authorization'] = f'Bearer {CONFLUENCE_TOKEN}'
        binaries = self.root/'runtime-binaries'
        binaries.mkdir()
        os_name = 'windows' if os.name == 'nt' else 'linux'
        archive = binaries/f'confluence-runtime-{os_name}.tar.gz'
        try: os.link(RUNTIME_ARCHIVE, archive)
        except OSError: shutil.copyfile(RUNTIME_ARCHIVE, archive)
        with RUNTIME_ARCHIVE.open('rb') as source:
            version = hashlib.file_digest(source, 'sha256').hexdigest()[:16]
        environment = patch.dict(os.environ, {'FLOW_WORKER_BINARIES': str(binaries), 'FLOW_STATE_DIR': str(self.root/'relay-state')})
        environment.start()
        self.addCleanup(environment.stop)
        relay = make_app(token_for=lambda key: TOKEN if key == UUID else None)
        update.add_routes(relay)
        transfers, fault = [], {'version': None}

        @web.middleware
        async def runtime_requests(request, handler):
            status = 500
            try:
                response = await handler(request)
                status = response.status
                if fault['version'] and request.path.endswith('/update/confluence-runtime') and status == 200:
                    response.headers['X-Runtime-Version'] = fault['version']
                return response
            except web.HTTPException as exc:
                status = exc.status
                raise
            finally:
                if request.path.endswith('/update/confluence-runtime'):
                    transfers.append({'status': status, 'query': dict(request.query),
                                      'authorized': request.headers.get('Authorization') == f'Bearer {TOKEN}'})

        relay.middlewares.append(runtime_requests)
        upstream = TestServer(relay)
        await upstream.start_server()
        self.addAsyncCleanup(upstream.close)
        upstream_env = patch.dict(os.environ, {'FLOW_RELAY_URL': str(upstream.make_url('')).rstrip('/')})
        upstream_env.start()
        self.addCleanup(upstream_env.stop)
        board = web.Application()
        relay_proxy.install(board)
        front = TestServer(board)
        await front.start_server()
        self.addAsyncCleanup(front.close)
        endpoint = str(front.make_url(f'/relay/{UUID}/update/confluence-runtime?os={os_name}&arch=x86_64'))
        async with aiohttp.ClientSession() as client:
            for token in (None, 'wrong'):
                async with client.get(endpoint, headers={'Authorization': f'Bearer {token}'} if token else {}) as response:
                    self.assertEqual(response.status, 401)
            async with client.get(endpoint.replace(UUID, str(uuid.uuid4())), headers={'Authorization': f'Bearer {TOKEN}'}) as response:
                self.assertEqual(response.status, 401, 'a PC token may fetch only its own runtime route')
        transfers.clear()
        pc = Service(server=str(front.make_url('')).rstrip('/'))
        self.addCleanup(pc.stop)
        cache = Path(pc.dir.name)/'confluence-runtime'
        self.assertFalse(cache.exists(), 'ordinary runner startup must not download Python')

        first, first_workspace = await self.source_job(item, 'rust-runtime-first', pc=pc)
        self.assertEqual(first['state'], 'done', first)
        self.assertEqual(transfers, [{'status': 200, 'query': {'os': os_name, 'arch': 'x86_64', 'have': ''}, 'authorized': True}])
        current = cache/'current'
        self.assertEqual(current.read_text(encoding='utf-8'), version)
        runtime = cache/version
        interpreter = runtime/('python/python.exe' if os.name == 'nt' else 'python/bin/python3')
        self.assertTrue(interpreter.is_file())
        await self.assert_confluence_parity(item, first_workspace/'wiki')

        reused, _ = await self.source_job(item, 'rust-runtime-cached', pc=pc)
        self.assertEqual(reused['state'], 'done', reused)
        self.assertEqual(transfers[-1], {'status': 304, 'query': {'os': os_name, 'arch': 'x86_64', 'have': version}, 'authorized': True})

        (runtime/'agents/flowkit/confluence_runner.py').unlink()
        repaired, _ = await self.source_job(item, 'rust-runtime-repaired', pc=pc)
        self.assertEqual(repaired['state'], 'done', repaired)
        self.assertEqual(transfers[-1]['status'], 200)
        self.assertEqual(transfers[-1]['query']['have'], '', 'an incomplete cache must be downloaded again, not accepted as current')
        self.assertTrue((runtime/'agents/flowkit/confluence_runner.py').is_file())

        unavailable = archive.with_suffix('.unavailable')
        archive.rename(unavailable)
        try:
            cached, cached_workspace = await self.source_job(item, 'rust-runtime-unavailable', pc=pc)
            self.assertEqual(cached['state'], 'done', cached)
            self.assertEqual(transfers[-1]['status'], 404)
            self.assertEqual((cached_workspace/'wiki/2.md').read_bytes(), (first_workspace/'wiki/2.md').read_bytes())
        finally:
            unavailable.rename(archive)

        current.unlink()
        fault['version'] = '0'*16
        rejected, _ = await self.source_job(item, 'rust-runtime-mismatch', pc=pc)
        self.assertEqual(rejected['state'], 'failed', rejected)
        self.assertIn('does not match its version', rejected['error'])
        self.assertFalse(current.exists(), 'a corrupt download must not be published as current')
        self.assertFalse(list(cache.glob('*.tar.gz')))
        self.assertFalse(list(cache.glob('.unpacking-*')))
        self.assertTrue(all(transfer['authorized'] for transfer in transfers))


if __name__ == '__main__':
    unittest.main()
