"""Permanent cutoff handling in the legacy Python relay connector."""
import asyncio
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
from flowkit.connector import Connector

TOKEN = 'pc-token-0123456789'


async def until(predicate, timeout=2):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError('timed out waiting for the persisted cutoff')
        await asyncio.sleep(.02)


class RevokedConnectorTests(unittest.IsolatedAsyncioTestCase):
    async def test_protocol_410_persists_cutoff_logs_recovery_and_prevents_restart_polling(self):
        polls = 0

        async def gone(request):
            nonlocal polls
            polls += 1
            return web.json_response({
                'error': 'worker_revoked',
                'message': 'This worker has been unregistered from the fleet. Run agent-worker uninstall, then install again with a new Fleet install command.',
            }, status=410)

        app = web.Application()
        app.router.add_get('/relay/worker/next', gone)
        server = TestServer(app)
        await server.start_server()
        self.addAsyncCleanup(server.close)
        with tempfile.TemporaryDirectory() as home:
            url = f'http://127.0.0.1:{server.port}'
            connector = Connector(url, 'worker', TOKEN, local_port=0, home=home)
            task = asyncio.create_task(connector.run())
            state_path = Path(home) / 'runtime' / 'connector.json'
            try:
                with self.assertLogs('relay-connector', level='WARNING') as captured:
                    await until(lambda: state_path.exists() and json.loads(state_path.read_text(encoding='utf-8')).get('revoked') is True)
                state = json.loads(state_path.read_text(encoding='utf-8'))
                self.assertFalse(state['connected'])
                self.assertEqual(state['revocation']['reason'], 'worker_revoked')
                self.assertEqual(state['revocation']['message'], 'This worker has been unregistered from the fleet. Run agent-worker uninstall, then install again with a new Fleet install command.')
                self.assertIsInstance(state['revocation']['at'], (int, float))
                self.assertIn('agent-worker uninstall', captured.output[0])
                self.assertIn('new Fleet install command', captured.output[0])
                self.assertEqual(polls, 1)
                self.assertFalse(task.done())  # the execution-service runner remains alive after relay shutdown

                restarted = asyncio.create_task(Connector(url, 'worker', TOKEN, local_port=0, home=home).run())
                try:
                    await asyncio.sleep(.05)
                    self.assertFalse(restarted.done())
                    self.assertEqual(polls, 1)
                finally:
                    restarted.cancel()
                    await asyncio.gather(restarted, return_exceptions=True)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_unknown_410_and_401_do_not_mark_the_connector_revoked(self):
        async def refused(request):
            if request.match_info['worker_id'] == 'wrong':
                return web.json_response({'error': 'worker_revoked'}, status=401)
            return web.json_response({'error': 'unknown'}, status=410)

        app = web.Application()
        app.router.add_get('/relay/{worker_id}/next', refused)
        server = TestServer(app)
        await server.start_server()
        self.addAsyncCleanup(server.close)
        async with aiohttp.ClientSession() as client:
            connector = Connector(f'http://127.0.0.1:{server.port}', 'worker', TOKEN)
            with self.assertRaisesRegex(RuntimeError, 'Server answered 410'):
                await connector.poll(client)
            wrong = Connector(f'http://127.0.0.1:{server.port}', 'wrong', TOKEN)
            with self.assertRaisesRegex(RuntimeError, "refused this PC's token"):
                await wrong.poll(client)
            self.assertIsNone(connector.revocation)
            self.assertIsNone(wrong.revocation)


if __name__ == '__main__':
    unittest.main()
