"""Hatchet worker-token issuance uses an admin session cookie, never a shared API token."""
import uuid
import sys
import unittest
from pathlib import Path

from aiohttp import web
from aiohttp.test_utils import TestServer

sys.path.insert(0, str(Path(__file__).parent))
import hatchet_tokens

TENANT = '11111111-1111-4111-8111-111111111111'
TOKEN_ID = '22222222-2222-4222-8222-222222222222'
TOKEN_SECRET = 'pc-worker-token-secret'


class HatchetTokenIssuerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls = []
        self.created = {}
        app = web.Application()

        async def login(request):
            self.calls.append(('login', request.path, await request.json(), request.cookies.get('hatchet_admin')))
            response = web.json_response({'ok': True})
            response.set_cookie('hatchet_admin', 'admin-session', path='/')
            return response

        async def create(request):
            body = await request.json()
            self.calls.append(('create', request.path, body, request.cookies.get('hatchet_admin')))
            if request.cookies.get('hatchet_admin') != 'admin-session':
                return web.json_response({'error': 'session required'}, status=401)
            self.created.update(body)
            return web.json_response({'token': TOKEN_SECRET})

        async def list_tokens(request):
            self.calls.append(('list', request.path, None, request.cookies.get('hatchet_admin')))
            if request.cookies.get('hatchet_admin') != 'admin-session':
                return web.json_response({'error': 'session required'}, status=401)
            return web.json_response({'rows': [{
                'metadata': {'id': TOKEN_ID}, 'name': self.created['name'], 'readOnly': False,
                'expiresAt': '2099-01-01T00:00:00Z',
            }]})

        async def revoke(request):
            self.calls.append(('revoke', request.path, await request.read(), request.cookies.get('hatchet_admin')))
            if request.cookies.get('hatchet_admin') != 'admin-session':
                return web.json_response({'error': 'session required'}, status=401)
            return web.Response(status=204)

        app.router.add_post('/api/v1/users/login', login)
        app.router.add_post(f'/api/v1/tenants/{TENANT}/api-tokens', create)
        app.router.add_get(f'/api/v1/tenants/{TENANT}/api-tokens', list_tokens)
        app.router.add_post(f'/api/v1/api-tokens/{TOKEN_ID}', revoke)
        self.server = TestServer(app, host='127.0.0.1')
        await self.server.start_server()
        self.addAsyncCleanup(self.server.close)

    async def test_issues_and_revokes_with_the_hatchet_login_session(self):
        issuer = hatchet_tokens.HatchetTokenIssuer(
            api_url=str(self.server.make_url('/')).rstrip('/'),
            email='board-admin@example.test', password='admin-password', tenant_id=TENANT,
        )

        credential = await issuer.issue_worker_token('build-pc')
        self.assertEqual((credential.token, credential.token_id), (TOKEN_SECRET, TOKEN_ID))
        self.assertTrue(credential.name.startswith('agent-board:build-pc:'))
        self.assertEqual(self.calls[0], ('login', '/api/v1/users/login', {
            'email': 'board-admin@example.test', 'password': 'admin-password',
        }, None))
        self.assertEqual(self.calls[1][0:2], ('create', f'/api/v1/tenants/{TENANT}/api-tokens'))
        self.assertEqual(self.calls[1][2], {'name': credential.name, 'readOnly': False})
        self.assertEqual(self.calls[1][3], 'admin-session', 'the create request uses the login cookie')
        self.assertEqual(self.calls[2][0:2], ('list', f'/api/v1/tenants/{TENANT}/api-tokens'))
        self.assertEqual(self.calls[2][3], 'admin-session', 'token lookup uses the same session')

        await issuer.revoke_worker_token(TOKEN_ID)
        self.assertEqual(self.calls[3][0:2], ('login', '/api/v1/users/login'))
        self.assertEqual(self.calls[4], ('revoke', f'/api/v1/api-tokens/{TOKEN_ID}', b'', 'admin-session'))
        self.assertNotEqual(credential.token, 'admin-password')
        self.assertEqual(uuid.UUID(credential.token_id).version, 4)
