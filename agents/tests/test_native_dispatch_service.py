"""The real Python worker service's native-engine dispatch and execution contract."""
import asyncio
import base64
import sys
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

from aiohttp.test_utils import TestClient, TestServer

AGENTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AGENTS))
from flowkit.capacity import CapacityGate
from flowkit.worker_service import make_app

TOKEN = 'native-dispatch-service-token'
PROGRAM = (
    "python -c \"from pathlib import Path; p=Path('dispatch-count.txt'); "
    "n=int(p.read_text()) if p.exists() else 0; p.write_text(str(n+1)); "
    "import time; time.sleep(2); print('native-dispatch-output')\""
)


class NativeDispatchServiceTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        sampler = patch('agents.flowkit.capacity.MachineSampler.sample', return_value=(1.0, 1.0))
        sampler.start()
        self.addCleanup(sampler.stop)
        self.gate = CapacityGate(self.root / 'capacity.db', slots=2, cpu_limit=100, ram_limit=100)
        now = time.monotonic()
        for second in range(11):
            self.gate.observe(1.0, 1.0, now - 10 + second)
        app = make_app(worker_id='native-pc', root=self.root / 'jobs', slots=2, token=TOKEN, capacity=self.gate)
        self.client = TestClient(TestServer(app), headers={'Authorization': f'Bearer {TOKEN}'})
        await self.client.start_server()
        self.addAsyncCleanup(self.client.close)
        self.jobs = []

    async def asyncTearDown(self):
        for job_id in self.jobs:
            try:
                await self.request('DELETE', f'/jobs/{job_id}')
            except Exception:
                pass

    async def request(self, method, path, body=None, **kwargs):
        async with self.client.request(method, path, json=body, **kwargs) as response:
            payload = await response.json()
            return response.status, payload

    async def create_native_job(self, engine='hatchet'):
        health_status, health = await self.request('GET', '/health')
        self.assertEqual(health_status, 200)
        job_id = str(uuid.uuid4())
        config = {
            'id': job_id,
            'authorize': True,
            'native_engine': engine,
            'gate_id': health['capacity']['gate_id'],
            'settings_revision': health['capacity']['settings_revision'],
            'files': {},
            'assets': {},
            'start': {
                'flow': {'name': 'native-dispatch', 'steps': {'go': {'kind': 'program', 'run': PROGRAM}}},
                'step': 'go',
                'context': {'input': {}, 'steps': {}},
                'runId': 'native_dispatch',
            },
        }
        status, body = await self.request('POST', '/jobs', config)
        self.assertEqual((status, body), (200, {'id': job_id}))
        self.jobs.append(job_id)
        state = await self.wait_for(job_id, lambda item: isinstance(item.get('authorization'), str))
        self.assertEqual((state['state'], state['waiting_reason']), ('queued', 'awaiting_authorization'))
        return job_id, state['authorization'], health['capacity']

    async def job(self, job_id):
        status, body = await self.request('GET', f'/jobs/{job_id}')
        self.assertEqual(status, 200)
        return body

    async def wait_for(self, job_id, predicate, timeout=15):
        deadline = asyncio.get_running_loop().time() + timeout
        while True:
            state = await self.job(job_id)
            if predicate(state):
                return state
            if asyncio.get_running_loop().time() >= deadline:
                self.fail(f'timed out waiting for job {job_id}: {state}')
            await asyncio.sleep(0.05)

    async def test_exact_native_dispatch_replay_and_result_are_service_owned(self):
        job_id, nonce, _ = await self.create_native_job()
        status, body = await self.request('POST', f'/jobs/{job_id}/dispatch', {'engine': 'prefect', 'nonce': nonce})
        self.assertEqual((status, body['error']), (409, 'No native dispatch is pending for this job'))
        status, body = await self.request('POST', f'/jobs/{job_id}/dispatch', {'engine': 'hatchet', 'nonce': 'wrong'})
        self.assertEqual((status, body['error']), (409, 'No native dispatch is pending for this job'))
        status, body = await self.request('GET', '/native-dispatch?engine=prefect')
        self.assertEqual((status, body), (200, {'jobs': []}))

        status, body = await self.request('POST', f'/jobs/{job_id}/dispatch', {'engine': 'hatchet', 'nonce': nonce})
        self.assertEqual((status, body), (200, {'ok': True}))
        status, body = await self.request('GET', '/native-dispatch?engine=hatchet')
        self.assertEqual((status, body), (200, {'jobs': [{'id': job_id, 'nonce': nonce}]}))
        status, body = await self.request('POST', f'/jobs/{job_id}/dispatch/ack', {'nonce': 'wrong', 'run_id': 'engine-run-1'})
        self.assertEqual((status, body['error']), (409, 'No native dispatch is pending for this job'))
        status, body = await self.request('POST', f'/jobs/{job_id}/start', {'nonce': 'wrong'})
        self.assertEqual((status, body['error']), (409, 'No start authorization is pending for this job'))
        starts = await asyncio.gather(
            self.request('POST', f'/jobs/{job_id}/start', {'nonce': nonce}),
            self.request('POST', f'/jobs/{job_id}/start', {'nonce': nonce}),
        )
        self.assertEqual(sorted(status for status, _ in starts), [200, 409],
                         'concurrent scheduler callbacks must authorize only one local run')
        self.assertIn((200, {'ok': True}), starts)
        running = await self.wait_for(job_id, lambda item: item['state'] == 'running')
        self.assertEqual(running['worker']['id'], 'native-pc')
        self.assertEqual(
            await self.request('POST', f'/jobs/{job_id}/dispatch', {'engine': 'hatchet', 'nonce': nonce}),
            (200, {'ok': True}),
        )
        for run_id in ('engine-run-1', 'engine-run-1'):
            status, body = await self.request('POST', f'/jobs/{job_id}/dispatch/ack', {'nonce': nonce, 'run_id': run_id})
            self.assertEqual((status, body), (200, {'ok': True}))
        status, body = await self.request('POST', f'/jobs/{job_id}/dispatch/ack', {'nonce': nonce, 'run_id': 'other-run'})
        self.assertEqual((status, body['error']), (409, 'No native dispatch is pending for this job'))
        status, body = await self.request('GET', '/native-dispatch?engine=hatchet')
        self.assertEqual((status, body), (200, {'jobs': []}), 'an acknowledged scheduler run must not be submitted twice')

        result = await self.wait_for(job_id, lambda item: item['state'] in ('done', 'failed'))
        self.assertEqual(result['state'], 'done', result)
        self.assertEqual(result['worker']['id'], 'native-pc')
        self.assertIn('native-dispatch-output', result['output']['stdout'])
        self.assertEqual(base64.b64decode(result['files']['dispatch-count.txt']['data']), b'1')
        self.assertTrue(any(event.get('type') == 'worker' for event in result['events']))

    async def test_prefect_dispatch_runs_only_after_its_exact_local_admission(self):
        job_id, nonce, _ = await self.create_native_job('prefect')
        self.assertEqual((await self.request('GET', '/native-dispatch?engine=hatchet'))[1], {'jobs': []})
        self.assertEqual(
            await self.request('POST', f'/jobs/{job_id}/dispatch', {'engine': 'prefect', 'nonce': nonce}),
            (200, {'ok': True}),
        )
        self.assertEqual(
            (await self.request('GET', '/native-dispatch?engine=prefect'))[1],
            {'jobs': [{'id': job_id, 'nonce': nonce}]},
        )
        self.assertEqual(
            await self.request('POST', f'/jobs/{job_id}/start', {'nonce': nonce}),
            (200, {'ok': True}),
        )
        result = await self.wait_for(job_id, lambda item: item['state'] in ('done', 'failed'))
        self.assertEqual(result['state'], 'done', result)
        self.assertEqual(result['worker']['id'], 'native-pc')
        self.assertIn('native-dispatch-output', result['output']['stdout'])
        self.assertEqual(base64.b64decode(result['files']['dispatch-count.txt']['data']), b'1')

    async def test_cancelled_admission_rejects_a_late_scheduler_callback(self):
        job_id, nonce, _ = await self.create_native_job()
        self.assertEqual((await self.request('POST', f'/jobs/{job_id}/dispatch', {'engine': 'hatchet', 'nonce': nonce}))[0], 200)
        self.assertEqual((await self.request('POST', f'/jobs/{job_id}/dispatch/ack', {'nonce': nonce, 'run_id': 'engine-run-cancel'}))[0], 200)
        status, body = await self.request('DELETE', f'/jobs/{job_id}')
        self.assertEqual((status, body['terminated']), (200, True))
        status, _ = await self.request('POST', f'/jobs/{job_id}/start', {'nonce': nonce})
        self.assertEqual(status, 409)
        self.assertEqual((await self.job(job_id))['state'], 'cancelled')
        self.assertFalse((self.root / 'jobs' / job_id / 'workspaces' / 'native_dispatch' / 'dispatch-count.txt').exists())
        self.assertEqual((await self.request('GET', '/native-dispatch?engine=hatchet'))[1], {'jobs': []})

    async def test_changed_capacity_revision_fences_dispatch_and_start(self):
        job_id, nonce, capacity = await self.create_native_job()
        revision = capacity['settings_revision'] + 1
        status, _ = await self.request(
            'POST', '/capacity/settings',
            {'slots': 2, 'cpu_limit': 100, 'ram_limit': 100},
            headers={'X-Flow-Gate-ID': capacity['gate_id'], 'X-Flow-Settings-Revision': str(revision)},
        )
        self.assertEqual(status, 200)
        self.assertEqual((await self.request('GET', '/native-dispatch?engine=hatchet'))[1], {'jobs': []})
        status, _ = await self.request('POST', f'/jobs/{job_id}/dispatch', {'engine': 'hatchet', 'nonce': nonce})
        self.assertEqual(status, 409)
        status, _ = await self.request('POST', f'/jobs/{job_id}/start', {'nonce': nonce})
        self.assertEqual(status, 409)
        self.assertFalse((self.root / 'jobs' / job_id / 'workspaces' / 'native_dispatch' / 'dispatch-count.txt').exists())

    async def test_native_jobs_require_explicit_coordinator_authorization(self):
        job_id = str(uuid.uuid4())
        for config in ({'id': job_id, 'native_engine': 'hatchet'},
                       {'id': job_id, 'native_engine': 'hatchet', 'authorize': 'yes'}):
            status, _ = await self.request('POST', '/jobs', config)
            self.assertEqual(status, 400)
