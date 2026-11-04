from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import unittest
from unittest.mock import patch
from flowkit.capabilities import infer_command, probe_agent, probe_command

class CapabilitiesTest(unittest.TestCase):
    def test_inference(self):
        self.assertEqual(infer_command('python script.py'), 'python')
        self.assertIsNone(infer_command('python a.py | cat'))
    def test_quoted_python_code_is_not_shell_control(self):
        self.assertEqual(infer_command('python -c "print(1); print(2)"'), 'python')
        self.assertIsNone(infer_command('python -c "print(1)"; echo unsafe'))
        self.assertIsNone(infer_command('python -c "print(1)" | other'))
    def test_python(self):
        self.assertEqual(probe_command('python')['status'], 'installed')
    def test_demo(self):
        self.assertEqual(probe_agent('demo')['status'], 'installed')
        self.assertEqual(probe_agent('demo')['auth_status'], 'unknown')
    def test_missing(self):
        with patch('shutil.which', return_value=None):
            self.assertEqual(probe_command('missing-executable')['status'], 'absent')

    def test_agent_resolution(self):
        with patch('agentdriver.acp_engine.agent_command', return_value=['missing-node', 'adapter.js']), patch('shutil.which', return_value=None):
            for name in ('codex', 'claude', 'opencode2', 'opencode', 'omp', 'pi'):
                self.assertEqual(probe_agent(name)['status'], 'absent')
        with patch('agentdriver.acp_engine.agent_command', side_effect=RuntimeError('missing adapter')):
            self.assertEqual(probe_agent('codex')['status'], 'absent')

    def test_round_trip_requirement_and_staleness(self):
        from flowkit.definition import parse
        from flowkit.capabilities import overdue
        flow = parse({'name':'sample', 'steps':{'run':{'kind':'program', 'run':'python a | cat', 'requires_command':'python'}}})
        self.assertEqual(flow.to_json()['steps']['run']['requires_command'], 'python')
        self.assertTrue(overdue({'refreshed_at':'2000-01-01T00:00:00+00:00'}))
        self.assertTrue(overdue({}))
import asyncio
import json
import tempfile
import uuid
from aiohttp.test_utils import TestClient, TestServer
from agents.tests.worker_fixture import make_app

class InventoryHTTPTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.client = TestClient(TestServer(make_app(root=self.tmp.name)))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        self.tmp.cleanup()

    async def test_concurrent_refresh_persists_both(self):
        responses = await asyncio.gather(*(self.client.post('/capabilities/refresh', json={'commands':[name]}) for name in ('first-tool', 'second-tool')))
        self.assertEqual([r.status for r in responses], [200, 200])
        saved = json.loads((Path(self.tmp.name) / 'capabilities.json').read_text())
        self.assertEqual(set(saved['commands']), {'first-tool', 'second-tool'})
        self.assertEqual(set((await (await self.client.get('/capabilities')).json())['commands']), set(saved['commands']))

    async def test_malformed_bodies(self):
        for body in ([], {'commands':'python'}, {'commands':['']}, {'commands':['bad\nname']}):
            response = await self.client.post('/capabilities/refresh', json=body)
            self.assertEqual(response.status, 400)
        response = await self.client.post('/capabilities/refresh', data='null', headers={'Content-Type':'application/json'})
        self.assertEqual(response.status, 400)
        response = await self.client.post('/capabilities/refresh', data='{', headers={'Content-Type':'application/json'})
        self.assertEqual(response.status, 400)

    async def test_job_records_requirement(self):
        flow = {'name':'example', 'steps':{'go':{'kind':'program', 'run':'python {{ input.value }}'}}}
        payload = {'id':str(uuid.uuid4()), 'start':{'runId':'run', 'step':'go', 'flow':flow}, 'files':{}, 'assets':{}}
        with patch('flowkit.worker_service.Job'):
            response = await self.client.post('/jobs', json=payload)
            self.assertEqual(response.status, 200)
        self.assertIn('python', json.loads((Path(self.tmp.name) / 'capabilities.json').read_text())['commands'])
        self.client.server.app['jobs'].clear()
        flow['steps']['go']['requires_command'] = 'declared-tool'
        payload['id'] = str(uuid.uuid4())
        with patch('flowkit.worker_service.Job'):
            response = await self.client.post('/jobs', json=payload)
            self.assertEqual(response.status, 200)
        self.assertIn('declared-tool', json.loads((Path(self.tmp.name) / 'capabilities.json').read_text())['commands'])
        self.client.server.app['jobs'].clear()

class CapabilityEdgeTest(unittest.TestCase):
    def test_template_and_environment_arguments(self):
        self.assertEqual(infer_command('python'), 'python')
        self.assertEqual(infer_command('python {{ input.value }}'), 'python')
        self.assertEqual(infer_command('python $FLOW_CONTEXT'), 'python')
        self.assertEqual(infer_command('"python" script.py'), 'python')
        self.assertIsNone(infer_command('{{ input.command }} arg'))
        self.assertIsNone(infer_command('$EXECUTABLE arg'))

    def test_overdue_malformed(self):
        from flowkit.capabilities import overdue
        for value in ({'refreshed_at': '2020-01-01'}, {'refreshed_at': 5}, {'refreshed_at':'9999-01-01T00:00:00+00:00'}, {}):
            self.assertTrue(overdue(value))

    def test_python_without_path(self):
        with patch('shutil.which', return_value=None):
            self.assertEqual(probe_command('python')['status'], 'installed')

class PythonExecutionTest(unittest.IsolatedAsyncioTestCase):
    async def test_python_forms_without_path(self):
        from flowkit.definition import parse
        from flowkit.executor import run_step
        with tempfile.TemporaryDirectory() as directory, patch.dict('os.environ', {'PATH':'', 'FLOW_WORKSPACES':directory, 'FLOW_MANAGED_JOB':'1'}):
            for command in ('python -V', '"python" -V'):
                flow = parse({'name':'sample', 'steps':{'run':{'kind':'program', 'run':command}}})
                async def sink(event): pass
                async def receive(): return {}
                result = await run_step(flow, 'run', {}, 'run', receive, sink)
                self.assertTrue(result['stdout'].startswith('Python '))

class CorruptInventoryTest(unittest.IsolatedAsyncioTestCase):
    async def test_bad_persisted_shapes_are_recovered(self):
        for contents in ('[]', '{"commands": []}', '{"refreshed_at": "2020-01-01"}'):
            with tempfile.TemporaryDirectory() as directory:
                (Path(directory) / 'capabilities.json').write_text(contents)
                client = TestClient(TestServer(make_app(root=directory)))
                await client.start_server()
                response = await client.get('/capabilities')
                self.assertEqual(response.status, 200)
                self.assertIn('refreshed_at', await response.json())
                await client.close()
