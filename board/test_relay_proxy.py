"""The board's pass-through for PCs: install, update and relay credentials never unlock the board."""
import asyncio
import hashlib
import os
import sys
import tempfile
import time
import uuid
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'agents'))
sys.path.insert(0, str(Path(__file__).parent))
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

import relay_proxy
import server
from flowkit import install as worker_install, update as worker_update
from flowkit.relay import make_app as make_relay_app


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


class BinaryDownloadTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory(); self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        binaries = root / 'binaries'; binaries.mkdir()
        self.payloads = {'windows': b'MZ\x00\xff' + bytes(range(256)) * 513,
                         'linux': b'\x7fELF\x00' + bytes(reversed(range(256))) * 513}
        for os_name, filename in worker_install.TARGETS.items():
            (binaries / filename).write_bytes(self.payloads[os_name])
        env = {'BOARD_PASSWORD': 'correct horse', 'BOARD_SECRET': 's', 'FLOW_RELAY_URL': '',
               'FLOW_STATE_DIR': str(root / 'state'), 'FLOW_WORKER_BINARIES': str(binaries),
               'FLOW_WORKERS_FILE': str(root / 'workers.yaml'), 'FLOW_WORKERS_ENV_FILE': str(root / 'workers.env')}
        for key in ('HATCHET_CLIENT_TOKEN', 'TRIGGER_SECRET_KEY', 'TRIGGER_DEV_SECRET_KEY', 'PREFECT_API_URL'): env[key] = ''
        patcher = patch.dict(os.environ, env); patcher.start(); self.addCleanup(patcher.stop)
        for name in ('FAILED_ATTEMPTS', '_binary_versions'):
            patcher = patch.object(worker_install, name, {}); patcher.start(); self.addCleanup(patcher.stop)
        self.worker_id = str(uuid.uuid4())
        self.worker_token = 'download-worker-token-0123456789abcdef'
        upstream = make_relay_app(token_for=lambda key: self.worker_token if key == self.worker_id else None,
                                  install=worker_install.add_routes)
        worker_update.add_routes(upstream)
        self.upstream_cookies = []

        @web.middleware
        async def observe_cookies(request, handler):
            self.upstream_cookies.append(request.headers.get('Cookie'))
            return await handler(request)

        upstream.middlewares.append(observe_cookies)
        self.upstream = TestClient(TestServer(upstream)); self.addAsyncCleanup(self.upstream.close)
        await self.upstream.start_server()
        os.environ['FLOW_RELAY_URL'] = str(self.upstream.make_url('/')).rstrip('/')
        self.client = TestClient(TestServer(server.make_app())); self.addAsyncCleanup(self.client.close)
        await self.client.start_server()

    def create_install(self, os_name, name):
        return worker_install.create({'id': name, 'os': os_name, 'engines': ['hatchet'], 'pool': 'build',
                                      'server': 'http://192.0.2.10:8090'})

    async def assert_binary(self, client, path, os_name, headers=None):
        response = await client.get(path, headers=headers, allow_redirects=False)
        self.assertEqual(response.status, 200, path)
        self.assertEqual(await response.read(), self.payloads[os_name])
        self.assertEqual(response.content_type, 'application/octet-stream')
        self.assertIsNotNone(response.content_disposition)
        self.assertEqual(response.content_disposition.type, 'attachment')
        self.assertEqual(response.content_disposition.filename, worker_install.TARGETS[os_name])
        self.assertEqual(response.headers.get('Cache-Control'), 'no-store')
        self.assertEqual(response.headers.get('X-Worker-Version'), hashlib.sha256(self.payloads[os_name]).hexdigest()[:16])
        self.assertNotIn('Set-Cookie', response.headers)

    async def assert_rejected(self, path, status=404, headers=None):
        response = await self.client.get(path, headers=headers, allow_redirects=False)
        self.assertEqual(response.status, status, path)
        self.assertEqual(response.content_type, 'application/json')
        self.assertIn('error', await response.json())
        self.assertNotIn('Content-Disposition', response.headers)

    async def test_install_download_preserves_native_filename_bytes_and_metadata(self):
        for os_name in self.payloads:
            record = self.create_install(os_name, f'install-{os_name}')
            path = f'/install/{record["code"]}/agent-worker'
            for client in (self.upstream, self.client):
                with self.subTest(os=os_name, proxied=client is self.client):
                    await self.assert_binary(client, path, os_name)

    async def test_generated_update_links_download_and_become_unusable_after_done(self):
        for os_name in self.payloads:
            record = worker_update.create_link(f'update-{os_name}', os_name)
            path = f'/update/{record["code"]}/agent-worker'
            for client in (self.upstream, self.client):
                with self.subTest(os=os_name, proxied=client is self.client):
                    await self.assert_binary(client, path, os_name)
            response = await self.client.post(f'/update/{record["code"]}/done', json={'ok': True}, allow_redirects=False)
            self.assertEqual(response.status, 204)
            self.assertEqual(worker_update.read_link(record['code'])['state'], 'done')
            await self.assert_rejected(path)
            response = await self.client.post(f'/update/{record["code"]}/done', json={'ok': True}, allow_redirects=False)
            self.assertEqual(response.status, 404)

    async def test_failed_update_links_cannot_download_again(self):
        record = worker_update.create_link('failed-update', 'linux')
        response = await self.client.post(f'/update/{record["code"]}/done', json={'ok': False, 'error': 'interrupted'}, allow_redirects=False)
        self.assertEqual(response.status, 204)
        self.assertEqual(worker_update.read_link(record['code'])['state'], 'failed')
        await self.assert_rejected(f'/update/{record["code"]}/agent-worker')

    async def test_unknown_cancelled_and_expired_install_links_are_denied(self):
        cancelled = self.create_install('windows', 'cancelled-install')
        worker_install.cancel(cancelled['code'])
        expired = self.create_install('linux', 'expired-install')
        expired['expires_at'] = worker_install.now() - 1; worker_install.write_record(expired)
        for code in ('invalid', str(uuid.uuid4()), cancelled['code'], expired['code']):
            with self.subTest(code=code):
                await self.assert_rejected(f'/install/{code}/agent-worker')

    async def test_update_links_reject_unknown_expired_and_install_uuid_credentials(self):
        expired = worker_update.create_link('expired-update', 'linux')
        expired['expires_at'] = time.time() - 1
        worker_update.atomic_json(worker_update.links_dir() / f'{expired["code"]}.json', expired)
        # Even a stored record cannot make an install UUID into an update-link credential.
        install_record = self.create_install('windows', 'different-credential')
        wrong_domain = {**expired, 'code': install_record['code'], 'expires_at': time.time() + 600}
        worker_update.atomic_json(worker_update.links_dir() / f'{wrong_domain["code"]}.json', wrong_domain)
        for code in ('invalid', 'A' * 43, expired['code'], wrong_domain['code']):
            with self.subTest(code=code):
                await self.assert_rejected(f'/update/{code}/agent-worker')
        response = await self.client.post(f'/update/{wrong_domain["code"]}/done', json={'ok': True}, allow_redirects=False)
        self.assertEqual(response.status, 404)

    async def test_authorized_relay_updates_preserve_native_filename_bytes_and_metadata(self):
        for os_name in self.payloads:
            path = f'/relay/{self.worker_id}/update/binary?os={os_name}&arch=x86_64'
            for client in (self.upstream, self.client):
                with self.subTest(os=os_name, proxied=client is self.client):
                    await self.assert_binary(client, path, os_name, {'Authorization': f'Bearer {self.worker_token}'})

    async def test_public_update_download_never_unlocks_board_routes(self):
        record = worker_update.create_link('public-update', 'windows')
        await self.assert_binary(self.client, f'/update/{record["code"]}/agent-worker', 'windows')
        for path in ('/api/systems', '/api/fleet/updates', '/api/fleet/installs'):
            response = await self.client.get(path, allow_redirects=False)
            self.assertEqual(response.status, 401, path)
        self.assertEqual((await self.client.post('/api/fleet/installs', json={}, allow_redirects=False)).status, 401)
        for path in ('/', '/update', '/updates', '/updater', '/relay', '/installer', '/static/chat.js'):
            response = await self.client.get(path, allow_redirects=False)
            self.assertEqual(response.status, 302, path)
            self.assertEqual(response.headers.get('Location'), '/login')

    async def test_board_session_cookies_never_replace_upstream_worker_authentication_or_leak(self):
        login = await self.client.post('/login', data={'password': 'correct horse'}, allow_redirects=False)
        self.assertEqual(login.status, 302)
        self.assertTrue(login.cookies['board_session']['httponly'])
        self.assertEqual(login.cookies['board_session']['samesite'], 'Strict')
        self.assertEqual((await self.client.get('/api/systems')).status, 200)
        install_record = self.create_install('windows', 'cookie-install')
        await self.assert_binary(self.client, f'/install/{install_record["code"]}/agent-worker', 'windows')
        update_record = worker_update.create_link('cookie-update', 'linux')
        await self.assert_binary(self.client, f'/update/{update_record["code"]}/agent-worker', 'linux')
        path = f'/relay/{self.worker_id}/update/binary?os=windows'
        for headers in (None, {'Authorization': 'Bearer wrong'}, {'Authorization': f'Bearer {update_record["code"]}'}):
            await self.assert_rejected(path, 401, headers)
        await self.assert_rejected('/relay/unknown/update/binary?os=windows', 401, {'Authorization': f'Bearer {self.worker_token}'})
        await self.assert_binary(self.client, path, 'windows', {'Authorization': f'Bearer {self.worker_token}'})
        self.assertTrue(self.upstream_cookies)
        self.assertEqual(self.upstream_cookies, [None] * len(self.upstream_cookies))
        self.assertEqual((await self.client.get('/api/systems')).status, 200)


if __name__ == '__main__':
    unittest.main()
