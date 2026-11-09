"""Fleet tab API: the board passes fleet requests to the flow host and never lets endpoints,
token names or tokens reach the browser."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

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
        public = {'id': 'pc-a', 'os': 'windows', 'engines': ['hatchet', 'prefect'],
                  'hatchet_token': 'per-pc-secret'}
        self.host.create_install = AsyncMock(return_value=(200, {
            'id': 'pc-a', 'hatchet_token': 'per-pc-secret',
            'hatchet_api_url': 'https://hatchet.example/api',
            'hatchet_worker_grpc_host_port': 'hatchet.example:7070',
            'hatchet_worker_tls_strategy': 'tls',
            'prefect_worker_api_url': 'https://prefect.example/api',
            'commands': {'powershell': 'agent-worker install <code>'},
        }))
        settings = {
            'hatchet': {'api_url': 'https://hatchet.example/api',
                        'hatchet_worker_grpc_host_port': 'hatchet.example:7070',
                        'hatchet_worker_tls_strategy': 'tls'},
            'prefect': {'prefect_worker_api_url': 'https://prefect.example/api'},
        }
        with patch.object(engine_admin, 'config_for', side_effect=lambda name: settings[name]):
            response = await self.client.post('/api/fleet/installs', json=public)
        self.assertEqual(response.status, 200)
        self.host.create_install.assert_awaited_once_with({
            **public,
            'hatchet_api_url': 'https://hatchet.example/api',
            'hatchet_worker_grpc_host_port': 'hatchet.example:7070',
            'hatchet_worker_tls_strategy': 'tls',
            'prefect_worker_api_url': 'https://prefect.example/api',
        })
        data = await response.json()
        text = json.dumps(data)
        for private in ('per-pc-secret', 'hatchet_worker_grpc_host_port',
                        'hatchet_worker_tls_strategy', 'prefect_worker_api_url', 'hatchet_api_url'):
            self.assertNotIn(private, text)
        self.assertEqual(data['commands']['powershell'], 'agent-worker install <code>')
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
