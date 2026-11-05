"""The outbound-only relay: coordinator calls reach a PC's local service through a long-polling connector."""
import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
from flowkit import relay as relay_module
from flowkit.connector import Connector
from flowkit.relay import make_app

TOKEN = 'pc-token-0123456789'


async def until(predicate, timeout=10, message='condition'):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline: raise AssertionError(f'timed out waiting for {message}')
        await asyncio.sleep(.02)


def local_service(seen):
    """A stand-in for the execution service: echoes bodies, reports health, can be slow."""
    async def health(request):
        seen.append(('GET', '/health'))
        return web.json_response({'id': 'pc', 'active': 0, 'capacity': {'state': 'known'}})
    async def echo(request):
        body = await request.read()
        seen.append((request.method, request.path_qs, request.headers.get('Authorization'), len(body)))
        return web.Response(body=body, content_type=request.content_type, status=int(request.query.get('status', 200)))
    async def stream(request):
        response = web.StreamResponse()
        await response.prepare(request)
        for i in range(int(request.query.get('chunks', 8))): await response.write(bytes([i]) * 100000)
        await response.write_eof()
        return response
    async def slow(request):
        await asyncio.sleep(float(request.query.get('seconds', 1)))
        return web.json_response({'ok': True})
    app = web.Application(client_max_size=300 * 1024 * 1024)
    app.router.add_get('/health', health)
    app.router.add_get('/stream', stream)
    app.router.add_get('/slow', slow)
    app.router.add_route('*', '/echo/{tail:.*}', echo)
    return app


class RelayFixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.seen = []
        self.local = TestServer(local_service(self.seen)); await self.local.start_server()
        self.addAsyncCleanup(self.local.close)
        self.tokens = {'pc': TOKEN}
        self.relay = TestServer(make_app(token_for=self.tokens.get)); await self.relay.start_server()
        self.addAsyncCleanup(self.relay.close)
        self.http = aiohttp.ClientSession(); self.addAsyncCleanup(self.http.close)
        self.url = f'http://127.0.0.1:{self.relay.port}'
        self.auth = {'Authorization': f'Bearer {TOKEN}'}
        self.connector = None

    async def connect(self, worker_id='pc', token=TOKEN, local_port=None):
        self.connector = Connector(self.url, worker_id, token, local_port or self.local.port)
        task = asyncio.create_task(self.connector.run())
        self.addAsyncCleanup(self.stop, task)
        await until(lambda: self.relay.app['relay'].peer(worker_id).online() if worker_id in self.relay.app['relay'].peers else False, message='connector online')
        return task

    async def stop(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class RelayTests(RelayFixture):
    async def test_a_call_reaches_the_local_service_and_the_answer_comes_back(self):
        await self.connect()
        async with self.http.post(f'{self.url}/w/pc/echo/jobs?x=1', json={'a': 1}, headers=self.auth) as response:
            self.assertEqual((response.status, await response.json()), (200, {'a': 1}))
        method, path, authorization, size = [s for s in self.seen if s[0] == 'POST'][0]
        self.assertEqual((path, authorization), ('/echo/jobs?x=1', f'Bearer {TOKEN}'))  # the token is forwarded for the service's own check

    async def test_status_codes_and_empty_bodies_pass_through(self):
        await self.connect()
        async with self.http.get(f'{self.url}/w/pc/echo/missing?status=404', headers=self.auth) as response:
            self.assertEqual(response.status, 404)
            self.assertEqual(await response.read(), b'')

    async def test_large_bodies_stream_both_ways_without_being_held(self):
        await self.connect()
        payload = bytes(range(256)) * (8 * 1024 * 1024 // 256)  # 8 MiB
        async with self.http.post(f'{self.url}/w/pc/echo/blob', data=payload, headers={**self.auth, 'Content-Type': 'application/octet-stream'}) as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.read(), payload)
        async with self.http.get(f'{self.url}/w/pc/stream?chunks=20', headers=self.auth) as response:
            self.assertEqual(len(await response.read()), 2_000_000)

    async def test_an_unreachable_pc_drops_the_connection_so_the_coordinator_sees_offline(self):
        for attempt in range(2):  # never connected, and connected then gone
            with self.assertRaises(aiohttp.ClientError):
                async with self.http.get(f'{self.url}/w/pc/echo/x', headers=self.auth) as response: await response.read()
            if attempt == 0:
                task = await self.connect()
                await self.stop(task)
                self.relay.app['relay'].peer('pc').last_poll = 1.0  # long ago
                await until(lambda: not self.relay.app['relay'].peer('pc').online(), message='offline')

    async def test_a_wrong_or_missing_token_is_refused_on_both_faces(self):
        for headers in ({}, {'Authorization': 'Bearer nope'}):
            async with self.http.get(f'{self.url}/w/pc/health', headers=headers) as response: self.assertEqual(response.status, 401)
            async with self.http.get(f'{self.url}/relay/pc/next?wait=0', headers=headers) as response: self.assertEqual(response.status, 401)
            async with self.http.post(f'{self.url}/relay/pc/status', json={}, headers=headers) as response: self.assertEqual(response.status, 401)
        async with self.http.get(f'{self.url}/w/other/health', headers=self.auth) as response: self.assertEqual(response.status, 401)  # unknown id

    async def test_one_pc_cannot_use_another_pcs_token(self):
        self.tokens['pc2'] = 'second-token-abcdefghij'
        async with self.http.get(f'{self.url}/relay/pc2/next?wait=0', headers=self.auth) as response: self.assertEqual(response.status, 401)

    async def test_pushed_health_is_served_without_a_round_trip_until_it_goes_stale(self):
        await self.connect()
        await until(lambda: self.relay.app['relay'].peer('pc').status is not None, message='status push')
        self.seen.clear()
        for _ in range(5):
            async with self.http.get(f'{self.url}/w/pc/health', headers=self.auth) as response:
                self.assertEqual((await response.json())['id'], 'pc')
        self.assertEqual(self.seen, [])  # no relayed call reached the PC
        with patch.object(relay_module, 'STATUS_FRESH', 0.0):
            async with self.http.get(f'{self.url}/w/pc/health', headers=self.auth) as response: self.assertEqual(response.status, 200)
        self.assertEqual(self.seen, [('GET', '/health')])

    async def test_a_local_service_that_is_down_reads_as_offline(self):
        await self.connect(local_port=1)  # nothing listens there
        with self.assertRaises(aiohttp.ClientError):
            async with self.http.get(f'{self.url}/w/pc/echo/x', headers=self.auth) as response: await response.read()

    async def test_a_call_nobody_collects_in_time_fails_as_offline(self):
        peer = self.relay.app['relay'].peer('pc'); peer.last_poll = asyncio.get_running_loop().time() + 1e9  # looks online, never polls
        with patch.object(relay_module, 'PICKUP_TIMEOUT', .3), self.assertRaises(aiohttp.ClientError):
            async with self.http.get(f'{self.url}/w/pc/echo/x', headers=self.auth) as response: await response.read()
        self.assertTrue(peer.pending and all(call.closed for call in peer.pending))  # still queued, but flagged so a later poll skips them (the client may have retried)

    async def test_a_coordinator_that_gave_up_is_never_delivered_to_the_pc(self):
        peer = self.relay.app['relay'].peer('pc'); peer.last_poll = asyncio.get_running_loop().time() + 1e9
        task = asyncio.create_task(self.http.get(f'{self.url}/w/pc/echo/late', headers=self.auth))
        await until(lambda: peer.pending, message='queued')
        task.cancel(); await asyncio.gather(task, return_exceptions=True)
        await asyncio.sleep(.1)
        await self.connect()
        await asyncio.sleep(.5)
        self.assertFalse([s for s in self.seen if s[0] != 'GET'])

    async def test_many_concurrent_calls_all_get_their_own_answer(self):
        await self.connect()
        async def call(i):
            async with self.http.post(f'{self.url}/w/pc/echo/n{i}', json={'i': i}, headers=self.auth) as response:
                return (await response.json())['i']
        self.assertEqual(sorted(await asyncio.gather(*(call(i) for i in range(120)))), list(range(120)))

    async def test_a_slow_call_does_not_block_other_calls(self):
        await self.connect()
        slow = asyncio.create_task(self.http.get(f'{self.url}/w/pc/slow?seconds=1.5', headers=self.auth))
        await asyncio.sleep(.2)
        async with self.http.get(f'{self.url}/w/pc/echo/fast', headers=self.auth) as response: self.assertEqual(response.status, 200)
        self.assertFalse(slow.done())
        response = await slow
        self.assertEqual((await response.json()), {'ok': True}); response.release()

    async def test_the_connector_reconnects_after_the_relay_restarts(self):
        await self.connect()
        port = self.relay.port
        await self.relay.close()
        self.relay = TestServer(make_app(token_for=self.tokens.get), port=port); await self.relay.start_server()
        self.addAsyncCleanup(self.relay.close)
        await until(lambda: 'pc' in self.relay.app['relay'].peers and self.relay.app['relay'].peers['pc'].online(), timeout=15, message='reconnected')
        async with self.http.get(f'{self.url}/w/pc/echo/again', headers=self.auth) as response: self.assertEqual(response.status, 200)

    async def test_online_state_is_exposed(self):
        await self.connect()
        async with self.http.get(f'{self.url}/relay-state') as response: state = await response.json()
        self.assertTrue(state['pc']['online'])


if __name__ == '__main__':
    unittest.main()
