import asyncio
import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp
from aiohttp.test_utils import TestServer
from flowkit import update
from flowkit.fleet_registry import REVOCATION_MESSAGE, save_worker, unregister_worker
from flowkit.relay import make_app

TOKEN = 'relay-revocation-token-123'
WORKER_UUID = '90b96c8e-0210-420c-a6b1-586d54d79a02'


async def until(predicate):
    deadline = asyncio.get_running_loop().time() + 5
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError('timed out waiting for relay queue')
        await asyncio.sleep(.01)


class RelayRevocationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'workers.yaml'
        self.env = patch.dict(os.environ, {
            'FLOW_WORKERS_FILE': str(self.path),
            'FLOW_WORKERS_ENV_FILE': str(Path(self.tmp.name) / 'workers.env'),
            'WORKER_TOKEN': TOKEN,
            'FLOW_HOST_TOKEN': 'host-secret',
        })
        self.env.start()
        self.addCleanup(self.env.stop)
        save_worker({'id': 'pc', 'uuid': WORKER_UUID, 'token_env': 'WORKER_TOKEN'}, create=True)
        self.server = TestServer(make_app(install=update.add_routes))
        await self.server.start_server()
        self.addAsyncCleanup(self.server.close)
        self.http = aiohttp.ClientSession()
        self.addAsyncCleanup(self.http.close)
        self.url = str(self.server.make_url('')).rstrip('/')
        self.auth = {'Authorization': f'Bearer {TOKEN}'}

    async def restart_relay(self):
        await self.server.close()
        self.server = TestServer(make_app(install=update.add_routes))
        await self.server.start_server()
        self.addAsyncCleanup(self.server.close)
        self.url = str(self.server.make_url('')).rstrip('/')

    async def test_revoked_token_is_distinguished_after_relay_restart(self):
        unregister_worker(relay_id=WORKER_UUID)
        await self.restart_relay()

        expected = {
            'error': 'worker_revoked',
            'message': REVOCATION_MESSAGE,
        }
        async with self.http.get(f'{self.url}/relay/{WORKER_UUID}/next?wait=0', headers=self.auth) as response:
            self.assertEqual((response.status, await response.json()), (410, expected))
        async with self.http.get(f'{self.url}/w/{WORKER_UUID}/health', headers=self.auth) as response:
            self.assertEqual((response.status, await response.json()), (410, expected))
        async with self.http.get(f'{self.url}/relay/{WORKER_UUID}/update/version?os=windows', headers=self.auth) as response:
            self.assertEqual((response.status, await response.json()), (410, expected))
        async with self.http.get(f'{self.url}/relay/{WORKER_UUID}/next?wait=0', headers={'Authorization': 'Bearer wrong-token'}) as response:
            self.assertEqual(response.status, 401)
        async with self.http.get(f'{self.url}/relay/unknown/next?wait=0', headers=self.auth) as response:
            self.assertEqual(response.status, 401)

    async def test_replacement_reuses_name_without_reviving_the_old_worker(self):
        new_uuid = '80b96c8e-0210-420c-a6b1-586d54d79a02'
        new_token = 'replacement-worker-token-456'
        unregister_worker(relay_id=WORKER_UUID)
        os.environ['WORKER_TOKEN'] = new_token
        save_worker({'id': 'pc', 'uuid': new_uuid, 'token_env': 'WORKER_TOKEN'}, create=True)

        async with self.http.get(f'{self.url}/relay/{WORKER_UUID}/next?wait=0', headers=self.auth) as response:
            self.assertEqual(response.status, 410)
        async with self.http.get(f'{self.url}/relay/{new_uuid}/next?wait=0',
                                 headers={'Authorization': f'Bearer {new_token}'}) as response:
            self.assertEqual(response.status, 204)

    async def test_self_unregister_closes_queued_calls_before_they_are_delivered(self):
        relay = self.server.app['relay']
        peer = relay.peer(WORKER_UUID)
        peer.last_poll = time.monotonic()
        pending = asyncio.create_task(self.http.get(f'{self.url}/w/{WORKER_UUID}/health', headers=self.auth))
        await until(lambda: bool(peer.pending))

        async with self.http.delete(f'{self.url}/relay/{WORKER_UUID}/registration', headers=self.auth) as response:
            self.assertEqual((response.status, await response.json()), (200, {'removed': True}))

        self.assertTrue(peer.revoked)
        self.assertFalse(peer.pending)
        try:
            dropped = await asyncio.wait_for(pending, 2)
        except aiohttp.ClientError:
            pass  # the relay may abort the coordinator transport instead of returning its 502 drop response
        else:
            async with dropped:
                self.assertIn(dropped.status, (410, 502))
        async with self.http.get(f'{self.url}/relay/{WORKER_UUID}/next?wait=0', headers=self.auth) as response:
            self.assertEqual(response.status, 410)
