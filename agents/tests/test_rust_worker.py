"""The Rust worker as the oracle sees it: the real Python coordinator code drives the agent-worker binary as a PC.

Set AGENT_WORKER_BIN to the binary (default: worker/target/{release,debug}/agent-worker). Skipped when there is none.
"""
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
from flowkit.definition import parse
from agents.tests.test_fleet_matching import FleetFixture, info, until

ROOT = Path(__file__).resolve().parents[2]
TOKEN = 'rust-oracle-token-0123456789abcdefghij'
EXE = '.exe' if os.name == 'nt' else ''


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

    def __init__(self):
        self.dir = tempfile.TemporaryDirectory()
        self.port = free_port()
        env = {**os.environ, 'AGENT_WORKER_TEST_FAST': '1', 'FLOW_WORKER_TOKEN': TOKEN}
        self.process = subprocess.Popen([find_binary(), 'serve', '--id', 'rust-pc', '--uuid', UUID, '--port', str(self.port), '--slots', '2', '--root', self.dir.name],
                                        env=env, stdout=subprocess.DEVNULL, stderr=open(Path(self.dir.name) / 'service.log', 'wb'))
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                health = self.get('/health')
                if health['id'] == 'rust-pc' and health.get('uuid') == UUID: return
            except OSError: time.sleep(.1)
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

    def stop(self):
        self.process.terminate()
        try: self.process.wait(10)
        except subprocess.TimeoutExpired: self.process.kill()
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
        self.assertTrue(all(v['status'] == 'absent' for v in refreshed['agents'].values()), 'agent steps are not run by this worker yet')

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

    async def test_a_confluence_source_explains_that_this_worker_cannot_import_it(self):
        from flowkit.distributed import execute_session
        config = {'goal': 'python -c "print(1)"', 'agent': 'program', 'worker': {'pools': ['build']},
                  'context': {'confluence': [{'base_url': 'https://x.io/wiki', 'page_id': '1', 'target': 'context/confluence/1'}]}}
        with self.assertRaisesRegex(RuntimeError, 'does not import Confluence pages'):
            await asyncio.wait_for(execute_session(config, 'rust-wiki', self.receive, self.sink, engine='hatchet'), 60)

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
        self.assertEqual(self.pc.post('/jobs', {**base, 'start': {'session': {'goal': 'x'}, 'runId': 'r1'}})[0], 400)


if __name__ == '__main__':
    unittest.main()
