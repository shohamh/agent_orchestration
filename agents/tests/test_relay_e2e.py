"""A relayed PC end to end: the real coordinator placement code, the relay, the connector and a real
execution service, with the PC registered through the install completion path."""
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
from aiohttp.test_utils import TestServer
from flowkit import install
from flowkit.connector import Connector
from flowkit.definition import parse
from flowkit.relay import make_app as make_relay
from agents.tests.test_fleet_matching import FleetFixture, info, until
from agents.tests.worker_fixture import make_app

TOKEN = 'e2e-token-0123456789abcdefghijklmnop'
FORM = {'id': 'pc-r', 'os': 'linux', 'engines': ['hatchet', 'prefect', 'trigger'], 'pool': 'build', 'tags': [], 'slots': 1,
        'cpu_limit': 100, 'ram_limit': 100, 'server': 'http://192.168.1.10:8090'}
FLOW = {'name': 'relayed', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'run': 'python -c "print(\'ran through the relay\')"'}}}


class RelayedPcTests(FleetFixture):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.relay = TestServer(make_relay(install=install.add_routes)); await self.relay.start_server()
        self.addAsyncCleanup(self.relay.close)
        env = patch.dict(os.environ, {'FLOW_RELAY_LOCAL_URL': f'http://127.0.0.1:{self.relay.port}', 'FLOW_WORKERS_ENV_FILE': str(self.root/'workers.env'),
                                      'FLOW_STATE_DIR': str(self.root/'state')})
        env.start(); self.addCleanup(env.stop)
        for target, value in (('flowkit.relay.ONLINE_WINDOW', 3.0),):
            patcher = patch(target, value); patcher.start(); self.addCleanup(patcher.stop)

    async def start_pc(self, register=True):
        """A PC: a real execution service on localhost and a connector, registered like a finished install."""
        self.service = make_app(worker_id='pc-r', root=self.root/'pc-r', slots=1, token=TOKEN)
        self.local = TestServer(self.service); await self.local.start_server()
        self.addAsyncCleanup(self.local.close)
        if register:
            from flowkit.fleet_registry import delete_worker
            install.register(install.create(FORM), TOKEN)
            delete_worker('local')  # the default entry would point at this machine's real service
        return await self.connect()

    async def connect(self):
        connector = Connector(f'http://127.0.0.1:{self.relay.port}', 'pc-r', TOKEN, self.local.port)
        task = asyncio.create_task(connector.run())
        self.addAsyncCleanup(self.stop, task)
        await until(lambda: 'pc-r' in self.relay.app['relay'].peers and self.relay.app['relay'].peers['pc-r'].online(), message='connector online')
        return task

    async def stop(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_a_step_runs_on_a_relayed_pc_and_the_fleet_shows_it_online(self):
        from flowkit.distributed import execute_step
        from flowkit.workers import catalog, registry
        await self.start_pc()
        output = await asyncio.wait_for(execute_step(parse(FLOW), 'go', {'input': {}, 'steps': {}}, 'relayed-run', self.receive, self.sink), 60)
        self.assertIn('ran through the relay', output['stdout'])
        self.assertEqual(info(self.root, 'relayed-run', 'go')['state'], 'done')
        self.assertEqual([e['worker']['id'] for e in self.events if e.get('type') == 'worker'], ['pc-r'])
        entry = (await catalog(registry()))[0]
        self.assertEqual((entry['id'], entry['online'], entry['connection'], entry['settings_sync']['state']), ('pc-r', True, 'relay', 'applied'))
        self.assertEqual(entry['capacity']['settings'], {'slots': 1, 'cpu_limit': 100, 'ram_limit': 100})  # the registry limits reached the PC through the relay

    async def test_a_pc_that_goes_offline_makes_the_step_wait_and_it_resumes_when_the_connector_returns(self):
        from flowkit.distributed import execute_step
        task = await self.start_pc()
        await self.stop(task)
        await until(lambda: not self.relay.app['relay'].peers['pc-r'].online(), timeout=10, message='offline')
        run = asyncio.create_task(execute_step(parse(FLOW), 'go', {'input': {}, 'steps': {}}, 'offline-run', self.receive, self.sink))
        try:
            await until(lambda: info(self.root, 'offline-run', 'go').get('waiting_reason') == 'offline', message='waiting as offline')
            self.assertFalse(run.done())
            await self.connect()
            output = await asyncio.wait_for(run, 60)
            self.assertIn('ran through the relay', output['stdout'])
        finally:
            if not run.done(): run.cancel()
            await asyncio.gather(run, return_exceptions=True)

    async def test_cancelling_a_relayed_step_stops_it_on_the_pc(self):
        from flowkit.distributed import execute_step, mark_cancelled
        marker = self.root/'finished.txt'
        flow = parse({'name': 'slow', 'worker': {'pool': 'build'}, 'steps': {'go': {'kind': 'program', 'timeout_minutes': 2,
            'run': f'python -c "import time, pathlib; time.sleep(20); pathlib.Path(r\'{marker.as_posix()}\').write_text(\'x\')"'}}})
        await self.start_pc()
        run = asyncio.create_task(execute_step(flow, 'go', {'input': {}, 'steps': {}}, 'cancel-run', self.receive, self.sink))
        try:
            await until(lambda: any(j.started for j in self.service['jobs'].values()), timeout=90,
                        message=f"job started on the PC (a loaded machine can be slow to spawn the child); record: {info(self.root, 'cancel-run', 'go')}")
            self.assertTrue(mark_cancelled('cancel-run'))
            run.cancel()
            with self.assertRaises(asyncio.CancelledError): await run
            await until(lambda: all(j.state in ('cancelled', 'failed', 'done') for j in self.service['jobs'].values()) or not self.service['jobs'], timeout=20, message='job stopped on the PC')
            await asyncio.sleep(1)
            self.assertFalse(marker.exists())
        finally:
            if not run.done(): run.cancel()
            await asyncio.gather(run, return_exceptions=True)

    async def test_a_pc_behind_the_boards_proxy_runs_a_step_with_the_boards_login_in_force(self):
        """The real path: connector -> board (login on, PC paths exempt) -> relay -> coordinator code -> PC service."""
        from flowkit.distributed import execute_step
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'board'))
        import server as board_server
        env = {'BOARD_PASSWORD': 'pw', 'BOARD_SECRET': 's', 'FLOW_RELAY_URL': f'http://127.0.0.1:{self.relay.port}', 'HATCHET_CLIENT_TOKEN': '',
               'TRIGGER_SECRET_KEY': '', 'TRIGGER_DEV_SECRET_KEY': '', 'PREFECT_API_URL': ''}
        patcher = patch.dict(os.environ, env); patcher.start(); self.addCleanup(patcher.stop)
        board = TestServer(board_server.make_app()); await board.start_server(); self.addAsyncCleanup(board.close)
        from flowkit.fleet_registry import delete_worker
        self.service = make_app(worker_id='pc-r', root=self.root/'pc-r', slots=1, token=TOKEN)
        self.local = TestServer(self.service); await self.local.start_server(); self.addAsyncCleanup(self.local.close)
        install.register(install.create(FORM), TOKEN); delete_worker('local')
        connector = Connector(f'http://127.0.0.1:{board.port}', 'pc-r', TOKEN, self.local.port)
        task = asyncio.create_task(connector.run()); self.addAsyncCleanup(self.stop, task)
        await until(lambda: 'pc-r' in self.relay.app['relay'].peers and self.relay.app['relay'].peers['pc-r'].online(), message='connected through the board')
        output = await asyncio.wait_for(execute_step(parse(FLOW), 'go', {'input': {}, 'steps': {}}, 'board-run', self.receive, self.sink), 60)
        self.assertIn('ran through the relay', output['stdout'])

    async def test_the_registry_never_holds_the_token_and_the_relay_authenticates_with_it(self):
        await self.start_pc()
        self.assertNotIn(TOKEN, (self.root/'workers.yaml').read_text())
        async with aiohttp.ClientSession() as http:
            async with http.get(f'http://127.0.0.1:{self.relay.port}/w/pc-r/health') as response: self.assertEqual(response.status, 401)
            async with http.get(f'http://127.0.0.1:{self.relay.port}/w/pc-r/health', headers={'Authorization': f'Bearer {TOKEN}'}) as response:
                self.assertEqual((await response.json())['id'], 'pc-r')
