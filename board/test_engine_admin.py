"""Admin engine URL persistence and status routes."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp.test_utils import TestClient, TestServer
import server


class AdminEngineAPI(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {
            "BOARD_ENGINE_SETTINGS_PATH": str(Path(self.directory.name) / "settings.json"),
            "HATCHET_CLIENT_TOKEN": "hatchet-secret",
            "TRIGGER_SECRET_KEY": "trigger-secret",
            "TRIGGER_DEV_SECRET_KEY": "",
            "PREFECT_API_URL": "",
            "BOARD_PASSWORD": "",
        }, clear=False)
        self.env.start()
        with patch.object(server, "load_adapters", return_value={}):
            self.app = server.make_app()
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        self.env.stop()
        self.directory.cleanup()

    async def test_lists_redacted_engine_urls_and_disabled_state(self):
        response = await self.client.get('/api/admin/engines')
        self.assertEqual(response.status, 200)
        data = await response.json()
        self.assertEqual([x['id'] for x in data['engines']], ['hatchet', 'trigger', 'trigger-dev', 'prefect'])
        self.assertIn(data['engines'][0]['status'], {'reachable', 'unreachable', 'unauthorized'})
        self.assertEqual(data['engines'][2]['status'], 'disabled')
        text = json.dumps(data)
        self.assertNotIn('hatchet-secret', text)
        self.assertNotIn('trigger-secret', text)

    async def test_saves_urls_persistently_and_rejects_invalid_urls(self):
        body = {'api_url': 'http://127.0.0.1:65530/api', 'dashboard_url': 'http://localhost:4200'}
        response = await self.client.put('/api/admin/engines/prefect', json=body)
        self.assertEqual(response.status, 200)
        saved = await response.json()
        self.assertEqual(saved['api_url'], body['api_url'])
        self.assertTrue(saved['enabled'])
        self.assertIn('prefect', (await (await self.client.get('/api/systems')).json())['systems'])
        self.assertEqual(json.loads((Path(self.directory.name) / 'settings.json').read_text())['prefect'], body)
        bad = await self.client.put('/api/admin/engines/prefect', json={**body, 'api_url': 'file:///etc/passwd'})
        self.assertEqual(bad.status, 400)
        unknown = await self.client.put('/api/admin/engines/nope', json=body)
        self.assertEqual(unknown.status, 404)

    async def test_probe_maps_unreachable_and_disabled(self):
        await self.client.put('/api/admin/engines/trigger', json={'api_url': 'http://127.0.0.1:65530', 'dashboard_url': 'http://localhost:3000'})
        failed = await self.client.post('/api/admin/engines/trigger/check')
        result = await failed.json()
        self.assertEqual((failed.status, result['status']), (200, 'unreachable'))
        self.assertTrue(result['checked_at'])
        self.assertTrue(result['error'])
        disabled = await self.client.post('/api/admin/engines/trigger-dev/check')
        self.assertEqual(disabled.status, 409)

    async def test_routes_require_board_login(self):
        await self.client.close()
        with patch.dict(os.environ, {'BOARD_PASSWORD': 'password'}), patch.object(server, 'load_adapters', return_value={}):
            app = server.make_app()
        self.client = TestClient(TestServer(app))
        await self.client.start_server()
        for method, path in [('GET', '/api/admin/engines'), ('PUT', '/api/admin/engines/hatchet'), ('POST', '/api/admin/engines/hatchet/check')]:
            response = await self.client.request(method, path, json={})
            self.assertEqual(response.status, 401, path)


if __name__ == '__main__':
    unittest.main()
