"""Fleet tab API: the board passes fleet requests to the flow host and never lets endpoints,
token names or tokens reach the browser."""
import base64
import json
import sys
import tempfile
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, patch

from aiohttp import web

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp.test_utils import TestClient, TestServer
import server

LEAKY = {'id': 'pc-a', 'pools': ['build'], 'pool': 'build', 'tags': ['gpu'], 'engines': ['hatchet'], 'slots': 2,
         'cpu_limit': 90, 'ram_limit': 90, 'connection': 'configured', 'online': True, 'status': 'online', 'hostname': 'PC-A',
         'active': 0, 'url': 'http://10.0.0.5:8096', 'token_env': 'PC_A_TOKEN', '_headers': {'Authorization': 'Bearer s3cret'},
         'capacity': {'state': 'known', 'cpu_percent': 12.5, 'ram_percent': 40, 'sample_age_seconds': 0.4, 'ram_basis': 'physical total minus available',
                      'settings': {'slots': 2, 'cpu_limit': 90, 'ram_limit': 90}, 'leases': 0, 'endpoint': 'http://10.0.0.5:8096'},
         'inventory': {'agents': {'codex': {'status': 'installed', 'checked_at': '2026-11-05T10:00:00+00:00', 'auth_status': 'unknown', 'path': 'C:/secret'}},
                       'commands': {}, 'refreshed_at': '2026-11-05T10:00:00+00:00', 'cached': False, 'token': 's3cret'},
         'engine_status': {'hatchet': {'route': 'shared_service', 'coordinator': 'running', 'state': 'connected', 'connected': True, 'url': 'http://10.0.0.5'},
                           'trigger': {'route': 'shared_service', 'coordinator': 'unconfirmed', 'state': 'unconfirmed', 'connected': False}},
         'warnings': ['slots 500 in the registry is above the maximum of 100; 100 is enforced'],
         'settings_sync': {'state': 'applied', 'token': 's3cret'}}


def hatchet_client_token(tenant):
    claims = {'sub': tenant, 'server_url': 'http://hatchet.example', 'grpc_broadcast_address': 'hatchet.example:7070'}
    header = base64.urlsafe_b64encode(b'{"alg":"HS256","typ":"JWT"}').decode().rstrip('=')
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip('=')
    return f'{header}.{payload}.signature'

class FakeHatchet:
    def __init__(self):
        self.calls = []
        self.tokens = {}
        self.revoked = []

    def app(self):
        app = web.Application()
        app.router.add_post('/api/v1/users/login', self.login)
        app.router.add_post('/api/v1/tenants/{tenant}/api-tokens', self.create)
        app.router.add_get('/api/v1/tenants/{tenant}/api-tokens', self.list_tokens)
        app.router.add_post('/api/v1/api-tokens/{token_id}', self.revoke)
        return app

    async def login(self, request):
        body = await request.json()
        self.calls.append(('login', request.path, body, request.headers.get('Authorization')))
        response = web.json_response({'ok': True})
        response.set_cookie('hatchet_admin', 'admin-session', path='/')
        return response

    async def create(self, request):
        body = await request.json()
        self.calls.append(('create', request.path, body, request.cookies.get('hatchet_admin')))
        if request.cookies.get('hatchet_admin') != 'admin-session':
            return web.json_response({'error': 'session required'}, status=401)
        token_id = str(uuid.uuid4())
        secret = f'private-hatchet-worker-token-{len(self.tokens) + 1}'
        self.tokens[body['name']] = {'id': token_id, 'token': secret, 'readOnly': body['readOnly']}
        return web.json_response({'token': secret})

    async def list_tokens(self, request):
        self.calls.append(('list', request.path, None, request.cookies.get('hatchet_admin')))
        if request.cookies.get('hatchet_admin') != 'admin-session':
            return web.json_response({'error': 'session required'}, status=401)
        return web.json_response({'rows': [
            {'metadata': {'id': item['id']}, 'name': name, 'readOnly': item['readOnly'],
             'expiresAt': '2099-01-01T00:00:00Z'}
            for name, item in self.tokens.items()
        ]})

    async def revoke(self, request):
        self.calls.append(('revoke', request.path, await request.read(), request.cookies.get('hatchet_admin')))
        if request.cookies.get('hatchet_admin') != 'admin-session':
            return web.json_response({'error': 'session required'}, status=401)
        token_id = request.match_info['token_id']
        for name, item in list(self.tokens.items()):
            if item['id'] == token_id:
                del self.tokens[name]
                self.revoked.append(token_id)
                break
        return web.Response(status=204)


class FleetAPI(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        with patch.object(server, 'load_adapters', return_value={}):
            self.app = server.make_app()
        self.host = self.app['flow_host']
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    def assert_clean(self, data):
        text = json.dumps(data)
        for secret in ('10.0.0.5', 'PC_A_TOKEN', 's3cret', 'token_env', '"url"', '_headers', 'C:/secret', 'endpoint'):
            self.assertNotIn(secret, text)

    async def test_listing_and_metrics_are_filtered_even_if_the_flow_host_leaks(self):
        self.host.fleet = AsyncMock(return_value=(200, {'workers': [LEAKY], 'tags': ['gpu'], 'pools': ['build'], 'engines': ['hatchet']}))
        self.host.fleet_metrics = AsyncMock(return_value=(200, {'workers': [LEAKY], 'engines': ['hatchet']}))
        response = await self.client.get('/api/fleet')
        self.assertEqual(response.status, 200)
        data = await response.json()
        self.assert_clean(data)
        worker = data['workers'][0]
        self.assertEqual((worker['id'], worker['capacity']['cpu_percent'], worker['inventory']['agents']['codex']['status']), ('pc-a', 12.5, 'installed'))
        self.assertEqual(worker['engine_status']['hatchet']['connected'], True)
        self.assertEqual(worker['engine_status']['trigger']['state'], 'unconfirmed')
        self.assertIn('above the maximum of 100', worker['warnings'][0])
        self.assertEqual(data['tags'], ['gpu'])
        metrics = await (await self.client.get('/api/fleet/metrics')).json()
        self.assert_clean(metrics)

    async def test_register_update_refresh_and_tags_pass_through(self):
        self.host.register_worker = AsyncMock(return_value=(200, {'worker': LEAKY, 'settings_sync': {'state': 'pending'}}))
        body = {'id': 'pc-a', 'pools': ['build', 'gpu'], 'tags': ['gpu'], 'engines': ['hatchet', 'prefect'], 'slots': 2,
                'cpu_limit': 90, 'ram_limit': 90, 'url': 'http://10.0.0.5:8096', 'token_env': 'PC_A_TOKEN'}
        response = await self.client.post('/api/fleet/workers', json=body)
        self.assertEqual(response.status, 200)
        self.host.register_worker.assert_awaited_once_with(body)  # write-only fields reach the flow host
        self.assert_clean(await response.json())
        self.host.update_worker = AsyncMock(return_value=(409, {'error': 'Worker pc-a is already registered'}))
        response = await self.client.put('/api/fleet/workers/pc%20a', json={'engines': ['prefect']})
        self.assertEqual(response.status, 409)
        self.host.update_worker.assert_awaited_once_with('pc a', {'engines': ['prefect']})
        response = await self.client.put('/api/fleet/workers/pc-a', json={'password': 'x'})
        self.assertEqual(response.status, 400)
        response = await self.client.post('/api/fleet/workers', json=['not', 'an', 'object'])
        self.assertEqual(response.status, 400)
        self.host.refresh_capabilities = AsyncMock(return_value=(200, {'inventory': LEAKY['inventory']}))
        response = await self.client.post('/api/fleet/workers/pc-a/refresh', json={})
        self.assertEqual(response.status, 200)
        self.assert_clean(await response.json())
        self.host.add_fleet_tag = AsyncMock(return_value=(200, {'tags': ['gpu', 'new']}))
        response = await self.client.post('/api/fleet/tags', json={'name': 'new'})
        self.assertEqual((response.status, (await response.json())['tags']), (200, ['gpu', 'new']))
        self.host.add_fleet_tag.assert_awaited_once_with('new')
        self.host.unregister_worker = AsyncMock(return_value=(200, {'worker': LEAKY}))
        response = await self.client.delete('/api/fleet/workers/pc%20a')
        self.assertEqual(response.status, 200)
        data = await response.json()
        self.assert_clean(data)
        self.assertEqual(data['worker']['id'], 'pc-a')
        self.host.unregister_worker.assert_awaited_once_with('pc a')
        self.host.unregister_worker = AsyncMock(return_value=(404, {'error': 'Unknown worker'}))
        response = await self.client.delete('/api/fleet/workers/missing')
        self.assertEqual((response.status, await response.json()), (404, {'error': 'Unknown worker'}))
        self.host.fleet_tags = AsyncMock(return_value=(200, {'tags': ['gpu']}))
        self.assertEqual((await (await self.client.get('/api/fleet/tags')).json())['tags'], ['gpu'])


    async def test_install_uses_admin_endpoints_and_never_returns_private_claim_fields(self):
        import engine_admin
        tenant = '11111111-1111-4111-8111-111111111111'
        fake = FakeHatchet()
        hatchet = TestServer(fake.app(), host='localhost')
        await hatchet.start_server()
        self.addAsyncCleanup(hatchet.close)
        hatchet_url = str(hatchet.make_url('/')).rstrip('/')
        code = str(uuid.uuid4())
        public = {'id': 'pc-a', 'os': 'windows', 'engines': ['hatchet', 'prefect'],
                  'hatchet_api_url': 'https://stale.example/api',
                  'hatchet_worker_grpc_host_port': 'stale.example:7070',
                  'hatchet_worker_tls_strategy': 'none',
                  'prefect_worker_api_url': 'https://stale-prefect.example/api'}
        self.host.create_install = AsyncMock(return_value=(200, {
            'install': {'id': 'pc-a', 'code': code, 'state': 'pending',
                        'hatchet_token': 'private-hatchet-worker-token-1',
                        'hatchet_api_url': 'https://hatchet.example/api',
                        'hatchet_worker_grpc_host_port': 'hatchet.example:7070',
                        'hatchet_worker_tls_strategy': 'tls',
                        'prefect_worker_api_url': 'https://prefect.example/api'},
            'commands': {'powershell': 'agent-worker install <code>'},
        }))
        settings = {
            'hatchet': {'api_url': hatchet_url,
                        'hatchet_worker_grpc_host_port': 'hatchet.example:7070',
                        'hatchet_worker_tls_strategy': 'tls'},
            'prefect': {'prefect_worker_api_url': 'https://prefect.example/api'},
        }
        client_token = hatchet_client_token(tenant)
        with tempfile.TemporaryDirectory() as directory, patch.dict('os.environ', {
                'HATCHET_ADMIN_EMAIL': 'owner@example.test', 'HATCHET_ADMIN_PASSWORD': 'admin-password',
                'HATCHET_CLIENT_TOKEN': client_token,
                'BOARD_HATCHET_TOKEN_STORE_PATH': str(Path(directory) / 'hatchet-tokens.json')}):
            with patch.object(engine_admin, 'config_for', side_effect=lambda name: settings[name]):
                response = await self.client.post('/api/fleet/installs', json=public)
                self.assertEqual(response.status, 200)
                worker_token = fake.tokens[next(iter(fake.tokens))]['token']
                worker_token_id = fake.tokens[next(iter(fake.tokens))]['id']
                expected = {
                    **public,
                    'hatchet_token': worker_token,
                    'hatchet_api_url': hatchet_url,
                    'hatchet_worker_grpc_host_port': 'hatchet.example:7070',
                    'hatchet_worker_tls_strategy': 'tls',
                    'prefect_worker_api_url': 'https://prefect.example/api',
                }
                self.host.create_install.assert_awaited_once_with(expected)
                data = await response.json()
                text = json.dumps(data)
                for private in (worker_token, client_token, 'hatchet_worker_grpc_host_port',
                                'hatchet_worker_tls_strategy', 'prefect_worker_api_url', 'hatchet_api_url'):
                    self.assertNotIn(private, text)
                self.assertEqual(data['commands']['powershell'], 'agent-worker install <code>')
                self.assertEqual([call[0] for call in fake.calls], ['login', 'create', 'list'])
                self.assertEqual(fake.calls[1][3], 'admin-session')
                self.assertEqual(fake.calls[1][2], {'name': next(iter(fake.tokens)), 'readOnly': False})
                self.assertTrue(all(call[3] is None for call in fake.calls if call[0] == 'login'),
                                'the shared API token is not used for admin session login')

                self.host.cancel_install = AsyncMock(return_value=(200, {
                    'install': {'code': code, 'state': 'cancelled', 'hatchet_token': worker_token},
                }))
                cancelled = await self.client.delete(f'/api/fleet/installs/{code}')
                self.assertEqual(cancelled.status, 200)
                self.assertNotIn(worker_token, json.dumps(await cancelled.json()))
                self.assertEqual(fake.revoked, [worker_token_id])

                code2 = str(uuid.uuid4())
                public2 = {**public, 'id': 'pc-b'}
                self.host.create_install.return_value = (200, {
                    'install': {'id': 'pc-b', 'code': code2, 'state': 'pending'},
                    'commands': {'powershell': 'agent-worker install <code>'},
                })
                second = await self.client.post('/api/fleet/installs', json=public2)
                self.assertEqual(second.status, 200)
                second_token_id = fake.tokens[next(reversed(fake.tokens))]['id']
                self.host.unregister_worker = AsyncMock(return_value=(200, {'ok': True}))
                removed = await self.client.delete('/api/fleet/workers/pc-b')
                self.assertEqual(removed.status, 200)
                self.assertEqual(fake.revoked[-1], second_token_id)

                code3 = str(uuid.uuid4())
                public3 = {**public, 'id': 'pc-c'}
                self.host.create_install.return_value = (200, {
                    'install': {'id': 'pc-c', 'code': code3, 'state': 'pending'},
                    'commands': {'powershell': 'agent-worker install <code>'},
                })
                third = await self.client.post('/api/fleet/installs', json=public3)
                self.assertEqual(third.status, 200)
                third_token_id = fake.tokens[next(reversed(fake.tokens))]['id']
                self.host.fleet_installs = AsyncMock(return_value=(200, {
                    'installs': [{'id': 'pc-c', 'code': code3, 'state': 'expired'}],
                }))
                listing = await self.client.get('/api/fleet/installs')
                self.assertEqual(listing.status, 200)
                self.assertEqual(fake.revoked[-1], third_token_id)
                self.assertEqual(json.loads((Path(directory) / 'hatchet-tokens.json').read_text()), {})

    async def test_install_rejects_browser_hatchet_tokens_and_explains_missing_issuer_credentials(self):
        import engine_admin
        settings = {'api_url': 'http://127.0.0.1:8080', 'hatchet_worker_grpc_host_port': 'hatchet:7070',
                    'hatchet_worker_tls_strategy': 'none'}
        self.host.create_install = AsyncMock()
        with patch.object(engine_admin, 'config_for', return_value=settings), patch.dict('os.environ', {
                'HATCHET_ADMIN_EMAIL': '', 'HATCHET_ADMIN_PASSWORD': ''}):
            manual = await self.client.post('/api/fleet/installs', json={
                'id': 'pc-a', 'os': 'windows', 'engines': ['hatchet'], 'hatchet_token': 'manual-token',
            })
            self.assertEqual((manual.status, (await manual.json())['error']), (400, 'unknown install fields: hatchet_token'))
            missing = await self.client.post('/api/fleet/installs', json={
                'id': 'pc-a', 'os': 'windows', 'engines': ['hatchet'],
            })
        self.assertEqual(missing.status, 503)
        self.assertIn('HATCHET_ADMIN_EMAIL', (await missing.json())['error'])
        self.host.create_install.assert_not_awaited()
    async def test_flow_host_requests_use_the_authenticated_fleet_paths(self):
        calls = []
        async def call(method, path, body=None, timeout=10):
            calls.append((method, path, body))
            return 200, {}
        self.host._call = call
        await self.host.fleet(); await self.host.fleet_metrics(); await self.host.fleet_tags(); await self.host.add_fleet_tag('x')
        await self.host.register_worker({'id': 'a'}); await self.host.update_worker('a/b', {'tags': []}); await self.host.unregister_worker('a/b'); await self.host.refresh_capabilities('a')
        self.assertEqual([c[:2] for c in calls], [('GET', '/fleet'), ('GET', '/fleet/metrics'), ('GET', '/fleet/tags'), ('POST', '/fleet/tags'),
                                                  ('POST', '/fleet/workers'), ('PUT', '/fleet/workers/a%2Fb'), ('DELETE', '/fleet/workers/a%2Fb'), ('POST', '/fleet/workers/a/capabilities/refresh')])

    async def test_board_login_protects_the_fleet_routes(self):
        await self.client.close()
        with patch.dict('os.environ', {'BOARD_PASSWORD': 'pw'}), patch.object(server, 'load_adapters', return_value={}):
            app = server.make_app()
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        for method, path in (('GET', '/api/fleet'), ('GET', '/api/fleet/metrics'), ('POST', '/api/fleet/workers'), ('DELETE', '/api/fleet/workers/pc-a'), ('POST', '/api/fleet/tags')):
            response = await self.client.request(method, path, json={} if method != 'GET' else None)
            self.assertEqual(response.status, 401, path)


if __name__ == '__main__':
    unittest.main()
