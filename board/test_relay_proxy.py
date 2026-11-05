"""The board's pass-through for PCs: /install/* and /relay/* skip the login and stream to the relay."""
import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

import relay_proxy
import server


def fake_relay(seen):
    async def anything(request):
        body = await request.read()
        seen.append({'method': request.method, 'path': request.path_qs, 'client': request.headers.get('X-Relay-Client'),
                     'authorization': request.headers.get('Authorization'), 'cookie': request.headers.get('Cookie'),
                     'size': len(body), 'relay_status': request.headers.get('X-Relay-Status')})
        if request.path.endswith('/slow'):
            await asyncio.sleep(.3)
            return web.Response(status=204)
        if request.path.endswith('/big'):
            response = web.StreamResponse(headers={'Content-Type': 'application/gzip'})
            await response.prepare(request)
            for i in range(30): await response.write(bytes([i]) * 100000)
            await response.write_eof()
            return response
        return web.json_response({'echo': len(body)}, status=int(request.query.get('status', 200)))
    app = web.Application(client_max_size=300 * 1024 * 1024)
    app.router.add_route('*', '/{tail:.*}', anything)
    return app


class ProxyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.seen = []
        self.relay = TestServer(fake_relay(self.seen)); await self.relay.start_server()
        self.addAsyncCleanup(self.relay.close)
        env = {'BOARD_PASSWORD': 'correct horse', 'BOARD_SECRET': 's', 'FLOW_RELAY_URL': f'http://127.0.0.1:{self.relay.port}'}
        for key in ('HATCHET_CLIENT_TOKEN', 'TRIGGER_SECRET_KEY', 'TRIGGER_DEV_SECRET_KEY', 'PREFECT_API_URL'): env[key] = ''
        patcher = patch.dict(os.environ, env); patcher.start(); self.addCleanup(patcher.stop)
        self.client = TestClient(TestServer(server.make_app())); await self.client.start_server()
        self.addAsyncCleanup(self.client.close)

    async def test_pcs_reach_the_relay_without_a_login_but_the_board_stays_locked(self):
        for path in ('/install/CODE/install.sh', '/relay/pc/next?wait=1'):
            response = await self.client.get(path, allow_redirects=False)
            self.assertEqual(response.status, 200, path)
        self.assertEqual((await self.client.get('/api/systems')).status, 401)
        self.assertEqual((await self.client.get('/api/fleet/installs')).status, 401)
        self.assertEqual((await self.client.post('/api/fleet/installs', json={})).status, 401)
        self.assertEqual((await self.client.get('/installer', allow_redirects=False)).status, 302)  # only the exact prefixes are open
        self.assertEqual((await self.client.get('/relay', allow_redirects=False)).status, 302)

    async def test_the_peer_address_is_named_by_the_board_and_cannot_be_spoofed(self):
        await self.client.get('/install/CODE/install.sh', headers={'X-Relay-Client': '10.9.9.9', 'Cookie': 'board_session=x', 'X-Evil': '1'})
        self.assertEqual(self.seen[-1]['client'], '127.0.0.1')
        self.assertIsNone(self.seen[-1]['cookie'])  # the board's session cookie is never passed on

    async def test_authorization_and_relay_headers_pass_through(self):
        await self.client.put('/relay/pc/calls/abc/response', data=b'x' * 10, headers={'Authorization': 'Bearer t', 'X-Relay-Status': '404'})
        self.assertEqual((self.seen[-1]['authorization'], self.seen[-1]['relay_status'], self.seen[-1]['size']), ('Bearer t', '404', 10))

    async def test_status_codes_and_the_query_string_pass_through(self):
        response = await self.client.post('/install/CODE/complete?status=409', json={'a': 1})
        self.assertEqual((response.status, await response.json()), (409, {'echo': 8}))
        self.assertEqual(self.seen[-1]['path'], '/install/CODE/complete?status=409')

    async def test_big_bodies_stream_both_ways(self):
        payload = os.urandom(6 * 1024 * 1024)
        response = await self.client.put('/relay/pc/calls/abc/response', data=payload)
        self.assertEqual((await response.json())['echo'], len(payload))
        response = await self.client.get('/relay/pc/calls/abc/big')
        self.assertEqual((response.status, len(await response.read())), (200, 3_000_000))

    async def test_a_request_body_over_the_cap_is_refused(self):
        with patch.object(relay_proxy, 'MAX_BODY', 1000):
            response = await self.client.put('/relay/pc/calls/abc/response', data=b'x' * 5000)
        self.assertIn(response.status, (413, 502))

    async def test_a_held_long_poll_does_not_block_other_requests(self):
        slow = asyncio.create_task(self.client.get('/relay/pc/slow'))
        await asyncio.sleep(.05)
        fast = await self.client.get('/install/CODE/install.sh')
        self.assertEqual(fast.status, 200)
        self.assertFalse(slow.done())
        self.assertEqual((await slow).status, 204)

    async def test_a_relay_that_is_down_answers_with_a_clear_502(self):
        await self.relay.close()
        response = await self.client.get('/install/CODE/install.sh')
        self.assertEqual(response.status, 502)
        self.assertIn('relay', (await response.json())['error'])


if __name__ == '__main__':
    unittest.main()
