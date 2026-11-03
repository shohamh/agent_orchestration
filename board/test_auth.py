import os
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp.test_utils import TestClient, TestServer

import auth
import server


class AuthTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        env = {"BOARD_PASSWORD": "correct horse", "BOARD_SECRET": "s"}
        for key in ("HATCHET_CLIENT_TOKEN", "TRIGGER_SECRET_KEY", "TRIGGER_DEV_SECRET_KEY", "PREFECT_API_URL"):
            env[key] = ""
        self.env = patch.dict(os.environ, env)
        self.env.start()
        self.client = TestClient(TestServer(server.make_app()))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()
        self.env.stop()

    async def test_everything_requires_login(self):
        page = await self.client.get("/", allow_redirects=False)
        self.assertEqual(page.status, 302)
        self.assertEqual(page.headers["Location"], "/login")
        self.assertEqual((await self.client.get("/api/systems")).status, 401)
        self.assertEqual((await self.client.post("/api/cards/x/y/cancel")).status, 401)
        self.assertEqual((await self.client.get("/static/chat.js", allow_redirects=False)).status, 302)

    async def test_wrong_password_is_rejected(self):
        with patch.object(auth.asyncio, "sleep", return_value=None):
            response = await self.client.post("/login", data={"password": "nope"}, allow_redirects=False)
        self.assertEqual(response.status, 200)
        self.assertIn("Wrong password", await response.text())
        self.assertEqual((await self.client.get("/api/systems")).status, 401)

    async def test_login_sets_strict_httponly_cookie_and_unlocks_api(self):
        response = await self.client.post("/login", data={"password": "correct horse"}, allow_redirects=False)
        self.assertEqual(response.status, 302)
        cookie = response.cookies[auth.COOKIE]
        self.assertTrue(cookie["httponly"])
        self.assertEqual(cookie["samesite"], "Strict")
        self.assertEqual((await self.client.get("/api/systems")).status, 200)
        await self.client.get("/logout", allow_redirects=False)
        self.client.session.cookie_jar.clear()
        self.assertEqual((await self.client.get("/api/systems")).status, 401)

    def test_tokens_expire_and_cannot_be_forged(self):
        gate = auth.Auth("pw")
        self.assertTrue(gate.valid(gate.token()))
        expired = int(time.time()) - 1
        self.assertFalse(gate.valid(f"{expired}.{gate._sign(expired)}"))
        future = int(time.time()) + 999
        self.assertFalse(gate.valid(f"{future}.{'0' * 64}"))
        self.assertFalse(auth.Auth("other").valid(gate.token()))


if __name__ == "__main__":
    unittest.main()
