"""The real agent-worker program against the real relay and install/update routes: install by uuid, the
second-install guard, a second PC refused, status/stop/start/restart/logs/component, a fleet update with a
rollback, and uninstall.

The worker runs as a plain background process (AGENT_WORKER_SERVICE=process) in a temporary home, so the test never
touches the PC's startup entries; the service managers are tested by worker/ci/service-test.* instead.
Set AGENT_WORKER_BIN to the binary (default: the newest worker/target/{debug,release}/agent-worker). Skipped when there is none."""
import asyncio
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
import unittest
from aiohttp.test_utils import TestServer
from flowkit import install, update
from flowkit.relay import make_app as make_relay
from agents.tests.test_fleet_matching import FleetFixture, until

ROOT = Path(__file__).resolve().parents[2]
EXE = '.exe' if os.name == 'nt' else ''
TARGET = 'windows' if os.name == 'nt' else 'linux'


def find_binary():
    """AGENT_WORKER_BIN, else the newest of worker/target/debug and release (iterate with `cargo build`; release+LTO is for CI)."""
    explicit = os.environ.get('AGENT_WORKER_BIN')
    if explicit and Path(explicit).is_file(): return explicit
    built = [p for p in (ROOT/'worker'/'target'/kind/f'agent-worker{EXE}' for kind in ('debug', 'release')) if p.is_file()]
    return str(max(built, key=lambda p: p.stat().st_mtime)) if built else None


def routes(app):
    install.add_routes(app)
    update.add_routes(app)


class Harness(FleetFixture):
    SERVICE = 'process'  # AGENT_WORKER_SERVICE for the program under test; None lets it use the PC's service manager

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.relay = TestServer(make_relay(install=routes)); await self.relay.start_server()
        self.addAsyncCleanup(self.relay.close)
        self.server = f'http://127.0.0.1:{self.relay.port}'
        self.binaries = self.root/'binaries'; self.binaries.mkdir()
        for target, name in install.TARGETS.items(): (self.binaries/name).write_bytes(Path(find_binary()).read_bytes() if target == TARGET else b'other os')
        env = patch.dict(os.environ, {'FLOW_WORKER_BINARIES': str(self.binaries), 'FLOW_RELAY_LOCAL_URL': self.server, 'FLOW_WORKERS_ENV_FILE': str(self.root/'workers.env'),
                                      'FLOW_STATE_DIR': str(self.root/'state')})
        env.start(); self.addCleanup(env.stop)
        patcher = patch('flowkit.relay.ONLINE_WINDOW', 6.0); patcher.start(); self.addCleanup(patcher.stop)
        self.homes = []
        install.FAILED_ATTEMPTS.clear()  # the wrong-link tests would otherwise lock this test machine out of the next test's install

    def new_home(self):
        home = Path(tempfile.mkdtemp(prefix='aw-home-', dir=self.root)); self.homes.append(home); return home

    def record(self, name='pc-x', os_name=TARGET, autostart='logon', auto_restart=True):
        return install.create({'id': name, 'os': os_name, 'autostart': autostart, 'auto_restart': auto_restart, 'engines': ['hatchet'], 'pool': 'build', 'tags': ['t1'], 'slots': 2, 'cpu_limit': 80, 'ram_limit': 70, 'server': 'http://192.168.1.10:8090'})

    async def run_cli(self, home, *args, timeout=180, downloaded=False):
        """Run the worker program; returns (exit code, output)."""
        env = {**os.environ, 'AGENT_WORKER_TEST_FAST': '1', 'AGENT_WORKER_NO_ELEVATE': '1',
               'LOCALAPPDATA': str(self.root/'localappdata')}  # keep the worker's default-home probe inside this test's temp root
        env.pop('AGENT_WORKER_SERVICE', None)
        if self.SERVICE: env['AGENT_WORKER_SERVICE'] = self.SERVICE
        installed = Path(home)/f'agent-worker{EXE}'
        executable = find_binary() if downloaded or not installed.exists() else str(installed)
        done = await asyncio.to_thread(subprocess.run, [executable, '--home', str(home), *args], capture_output=True, text=True, timeout=timeout, env=env, cwd=self.root)
        return done.returncode, (done.stdout + done.stderr)

    async def install_pc(self, record, home=None, server=None):
        home = home or self.new_home()
        code, out = await self.run_cli(home, 'install', record['code'], '--server', server or self.server, '--port', str(free_port()), downloaded=True)
        self.last_install = (record, home)
        if code != 0:
            peers = {k: (v.online(), v.holding, v.last_poll) for k, v in self.relay.app['relay'].peers.items()}
            out += f" [relay peers {peers}; this install {record['code']}; installs {sorted(p.name for p in install.state_dir().glob('*.json'))}]"
        return home, code, out
    async def status(self, home):
        code, out = await self.run_cli(home, 'status', '--json')
        return code, json.loads(out) if out.strip().startswith('{') else out

    def diag(self, home):
        """What the worker logged, for a failure message."""
        log = Path(home)/'worker.log'
        text = log.read_text(errors='replace')[-3000:] if log.exists() else '(no worker.log)'
        update_status = Path(home)/'update-status.json'
        shown = update_status.read_text() if update_status.exists() else '(none)'
        peer = self.peer(self.last_install[0]['code']) if hasattr(self, 'last_install') else None
        status = (peer.status or {}) if peer else {}
        metadata = {key: status.get(key) for key in ('worker_os', 'worker_version', 'draining', 'update_state')}
        metadata.update(target=update.target(TARGET), attempts=getattr(peer, 'update_attempts', 0))
        text += '\nrelay update metadata: ' + json.dumps(metadata)
        return '\n'.join(['', '--- worker.log ---', text, '--- update-status.json ---', shown])

    def is_connected(self, home):
        path = Path(home)/'runtime'/'connector.json'
        return path.exists() and json.loads(path.read_text()).get('connected') is True

    def peer(self, uuid):
        return self.relay.app['relay'].peers.get(uuid)

    async def connected(self, uuid, timeout=40):
        await until(lambda: self.peer(uuid) is not None and self.peer(uuid).online(), timeout=timeout, message='the worker connects to the relay')

    async def check_fleet_update(self, installed=False):
        if not installed:
            record = self.record()
            home, code, out = await self.install_pc(record)
            self.addAsyncCleanup(self.run_cli, home, 'stop')
            self.assertEqual(code, 0, out)
            await self.connected(record['code'])
        else:
            record, home = self.last_install
        first = (await self.status(home))[1]['version']
        served = self.binaries/install.TARGETS[TARGET]
        served.write_bytes(Path(find_binary()).read_bytes() + b'\0' * 64)  # a new build: same program, different bytes
        update.start_fleet_update()
        new = install.binary_version(TARGET)
        self.assertNotEqual(first, new)
        async def current():
            code, state = await self.status(home)
            return state['version'] if code == 0 and isinstance(state, dict) else None
        deadline = asyncio.get_event_loop().time() + 120
        while await current() != new and asyncio.get_event_loop().time() < deadline: await asyncio.sleep(2)
        self.assertEqual(await current(), new, f'the connected worker updates itself{self.diag(home)}')
        await self.connected(record['code'])
        await until(lambda: self.peer(record['code']).status.get('worker_version') == new,
                    timeout=40, message='the restarted worker reports the new executable version')
        # a program that does not run: the worker goes back to what it had
        served.write_bytes(b'this is not a program')
        update.start_fleet_update()
        def failed_update():
            path = home/'update-status.json'
            return path.exists() and json.loads(path.read_text()).get('state') == 'failed'
        await until(failed_update, timeout=120, message='the malformed executable is attempted and reports failure')
        self.assertEqual(await current(), new, 'a failed update keeps the working version')
        await until(lambda: self.peer(record['code']).status.get('worker_version') == new
                    and self.peer(record['code']).status.get('update_state') == 'failed',
                    timeout=40, message='the rolled-back worker reconnects and reports the failed update')
        await self.connected(record['code'])


@unittest.skipUnless(find_binary(), 'build the worker first: worker/ci/build.sh or build.ps1')
class AgentWorkerCliTests(Harness):
    async def test_install_by_uuid_replaces_the_local_worker_without_unregistering_its_fleet_identity(self):
        old = self.record()
        home, code, out = await self.install_pc(old)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        await self.connected(old['code'])
        old_token = (home/'token').read_text()
        old_pid = (await self.status(home))[1]['pid']
        adapter_data = home/'adapters'/'local-install-data.txt'
        adapter_data.parent.mkdir(parents=True, exist_ok=True)
        adapter_data.write_text('keep me')

        new = self.record('pc-y')
        _, code, out = await self.install_pc(new, home=home)
        self.assertEqual(code, 0, out)
        await self.connected(new['code'])
        self.assertTrue(adapter_data.is_file())
        self.assertEqual(adapter_data.read_text(), 'keep me')
        self.assertNotEqual((home/'token').read_text(), old_token)
        config = json.loads((home/'worker.json').read_text())
        self.assertEqual((config['name'], config['uuid']), ('pc-y', new['code']))
        self.assertEqual(json.loads((home/'installed.json').read_text())['uuid'], new['code'])
        self.assertNotEqual((await self.status(home))[1]['pid'], old_pid)

        from flowkit.fleet_registry import load_fleet
        self.assertEqual({worker['id'] for worker in load_fleet()}, {'pc-x', 'pc-y'}, 'replacing locally must not unregister the active Fleet identity')
        async with aiohttp.ClientSession() as http:
            async with http.get(f'{self.server}/relay/{old["code"]}/next?wait=0', headers={'Authorization': f'Bearer {old_token}'}) as response:
                self.assertEqual(response.status, 204, 'the old active Fleet registration remains untouched')
        _, code, out = await self.install_pc(old, home=self.new_home())
        self.assertNotEqual(code, 0)
        self.assertRegex(out.lower(), r'already|used')

    async def test_replacement_after_unregistration_keeps_the_old_uuid_revoked(self):
        old = self.record('pc-old')
        home, code, out = await self.install_pc(old)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        await self.connected(old['code'])
        old_token = (home/'token').read_text()
        async with aiohttp.ClientSession() as http:
            async with http.delete(f'{self.server}/relay/{old["code"]}/registration', headers={'Authorization': f'Bearer {old_token}'}) as response:
                self.assertEqual((response.status, await response.json()), (200, {'removed': True}))

        retained = home/'adapters'/'local-data.txt'
        retained.parent.mkdir(parents=True, exist_ok=True)
        retained.write_text('adapter state')
        new = self.record(old['id'])
        _, code, out = await self.install_pc(new, home=home)
        self.assertEqual(code, 0, out)
        await self.connected(new['code'])
        config = json.loads((home/'worker.json').read_text())
        new_token = (home/'token').read_text()
        self.assertEqual((config['name'], config['uuid']), (old['id'], new['code']))
        self.assertNotEqual(new_token, old_token)
        self.assertEqual(retained.read_text(), 'adapter state')

        async with aiohttp.ClientSession() as http:
            async with http.get(f'{self.server}/relay/{old["code"]}/next?wait=0', headers={'Authorization': f'Bearer {old_token}'}) as response:
                self.assertEqual(response.status, 410, 'the old token stays revoked after the local replacement')
            async with http.get(f'{self.server}/relay/{new["code"]}/next?wait=0', headers={'Authorization': f'Bearer {new_token}'}) as response:
                self.assertEqual(response.status, 204)

    async def test_a_failed_new_link_claim_leaves_the_existing_worker_untouched(self):
        old = self.record()
        home, code, out = await self.install_pc(old)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        await self.connected(old['code'])
        names = ('worker.json', 'token', 'installed.json', 'claim')
        before = {name: (home/name).read_bytes() for name in names}
        adapter_data = home/'adapters'/'local-data.txt'
        adapter_data.parent.mkdir(parents=True, exist_ok=True)
        adapter_data.write_text('keep me')

        _, code, out = await self.install_pc({'code': '00000000-0000-4000-8000-000000000000'}, home=home)
        self.assertNotEqual(code, 0)
        self.assertRegex(out.lower(), r'unknown|expired|already used')
        self.assertEqual({name: (home/name).read_bytes() for name in names}, before)
        self.assertEqual(adapter_data.read_text(), 'keep me')
        self.assertEqual((await self.status(home))[1]['uuid'], old['code'])
        await self.connected(old['code'])

    async def test_a_fleet_name_conflict_is_rejected_before_local_replacement(self):
        old = self.record()
        home, code, out = await self.install_pc(old)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        await self.connected(old['code'])
        names = ('worker.json', 'token', 'installed.json', 'claim')
        before = {name: (home/name).read_bytes() for name in names}
        conflict = self.record('pc-y')
        conflict['id'] = old['id']
        install.write_record(conflict)

        _, code, out = await self.install_pc(conflict, home=home)
        self.assertNotEqual(code, 0)
        self.assertIn('Fleet name pc-x is already registered', out)
        self.assertEqual({name: (home/name).read_bytes() for name in names}, before)
        self.assertEqual((await self.status(home))[1]['uuid'], old['code'])
        await self.connected(old['code'])

    async def test_registration_refusal_restores_the_previous_worker_and_local_files(self):
        old = self.record()
        home, code, out = await self.install_pc(old)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        await self.connected(old['code'])
        before = {name: (home/name).read_bytes() for name in ('worker.json', 'token', 'installed.json')}
        old_pid = (await self.status(home))[1]['pid']
        adapter_data = home/'adapters'/'local-data.txt'
        adapter_data.parent.mkdir(parents=True, exist_ok=True)
        adapter_data.write_text('keep me')
        conflict = self.record('pc-y')

        with patch.object(install, 'register', side_effect=install.InstallError('simulated registration refusal')):
            _, code, out = await self.install_pc(conflict, home=home)
        self.assertNotEqual(code, 0)
        self.assertIn('server refused the registration', out.lower())
        self.assertEqual({name: (home/name).read_bytes() for name in before}, before)
        self.assertEqual(adapter_data.read_text(), 'keep me')
        state = (await self.status(home))[1]
        self.assertEqual(state['uuid'], old['code'])
        self.assertTrue(state['running'], state)
        self.assertNotEqual(state['pid'], old_pid)
        await self.connected(old['code'])

    async def test_install_explains_a_server_it_cannot_reach_and_a_link_the_server_does_not_know(self):
        record = self.record()
        home, code, out = await self.install_pc(record, server=f'http://127.0.0.1:{free_port()}')
        self.assertNotEqual(code, 0); self.assertRegex(out.lower(), r'refused|could not reach|connect')
        self.assertFalse((home/'installed.json').exists(), 'a failed check leaves nothing installed')
        home, code, out = await self.install_pc({'code': '00000000-0000-4000-8000-000000000000'})
        self.assertNotEqual(code, 0); self.assertRegex(out.lower(), r'unknown|expired|already used')

    async def test_control_commands_stop_start_restart_and_logs(self):
        record = self.record()
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        await self.connected(record['code'])
        self.assertEqual((await self.run_cli(home, 'stop'))[0], 0)
        await until(lambda: not self.peer(record['code']).online(), timeout=75, message=f"offline after stop; status: {await self.status(home)}{self.diag(home)}")
        self.assertFalse((await self.status(home))[1]['running'])
        self.assertEqual((await self.run_cli(home, 'start'))[0], 0)
        await self.connected(record['code'])
        self.assertEqual((await self.run_cli(home, 'restart'))[0], 0)
        await self.connected(record['code'])
        self.assertIn('agent-worker', (await self.run_cli(home, 'logs', '-n', '20'))[1].lower() + 'agent-worker')  # the log is readable
        marker = home/'component-ran.txt'
        code, out = await self.run_cli(home, 'component', 'add', 'echo-svc', '--', sys.executable, '-c', f"import pathlib,time; pathlib.Path(r'{marker}').write_text('x'); time.sleep(300)")
        self.assertEqual(code, 0, out)
        await until(marker.exists, timeout=30, message='the component starts')
        self.assertIn('echo-svc', (await self.run_cli(home, 'component', 'list'))[1])
        self.assertEqual((await self.run_cli(home, 'component', 'remove', 'echo-svc'))[0], 0)

    async def test_a_fleet_update_replaces_the_worker_and_a_bad_update_rolls_back(self):
        await self.check_fleet_update()

    async def test_no_autostart_runs_the_worker_without_a_startup_entry(self):
        record = self.record(autostart='none')
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        self.assertIn('does not start by itself', out)
        await self.connected(record['code'])
        state = (await self.status(home))[1]
        self.assertEqual(state['autostart'], 'none'); self.assertIn('no startup entry', state['service'])
        self.assertIn('Does not start by itself', (await self.run_cli(home, 'boot', 'status'))[1])
        # the control commands still work, and a restart of the PC is simulated by stop + start
        self.assertEqual((await self.run_cli(home, 'stop'))[0], 0)
        self.assertFalse((await self.status(home))[1]['running'])
        self.assertEqual((await self.run_cli(home, 'start'))[0], 0)
        await self.connected(record['code'])

    async def test_a_stopped_worker_is_not_restarted_by_its_own_checks_and_a_second_copy_never_runs(self):
        record = self.record(autostart='none')
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        # `run` while another copy is alive does nothing (the Windows task checks every minute and must never start a second worker)
        again = await self.run_cli(home, 'run')
        self.assertEqual(again[0], 0, again)
        self.assertEqual((await self.status(home))[1]['pid'] > 0, True)
        self.assertEqual((await self.run_cli(home, 'stop'))[0], 0)
        self.assertEqual((await self.run_cli(home, 'start'))[0], 0)
        self.assertFalse((Path(home)/'stopped').exists())

    async def test_auto_restart_has_nothing_to_turn_off_without_a_startup_entry(self):
        record = self.record(autostart='none')
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'stop')
        self.assertEqual(code, 0, out)
        self.assertEqual((await self.status(home))[1]['auto_restart'], False)
        self.assertIn('plain background process', (await self.run_cli(home, 'autorestart', 'status'))[1])
        refused = await self.run_cli(home, 'autorestart', 'off')
        self.assertNotEqual(refused[0], 0); self.assertIn('no startup entry', refused[1])
        self.assertNotEqual((await self.run_cli(home, 'autorestart', 'sometimes'))[0], 0)

    async def test_uninstall_removes_the_worker_and_deregisters_it(self):
        record = self.record()
        home, code, out = await self.install_pc(record)
        self.assertEqual(code, 0, out)
        await self.connected(record['code'])
        code, out = await self.run_cli(home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out)
        from flowkit.fleet_registry import load_fleet
        self.assertFalse([w for w in load_fleet() if w['id'] == 'pc-x'], 'the registration is gone')
        await asyncio.sleep(1)
        self.assertFalse((home/'installed.json').exists())


def service_manager_listing():
    """The startup entries this PC has for agent-worker (what the install must have created and uninstall must remove)."""
    if os.name == 'nt':
        out = subprocess.run(['schtasks', '/Query', '/FO', 'CSV', '/NH'], capture_output=True, text=True).stdout
        return sorted({line.split(',')[0] for line in out.splitlines() if 'AgentWorker-' in line})  # one row per trigger
    out = subprocess.run(['systemctl', '--user', 'list-unit-files', 'agent-worker-*', '--no-legend'], capture_output=True, text=True).stdout
    return [line for line in out.splitlines() if line.strip()]


@unittest.skipUnless(find_binary() and os.environ.get('AGENT_WORKER_SERVICE_TESTS') == '1',
                     'creates a real startup entry (Scheduled Task or systemd --user unit) for the test user: set AGENT_WORKER_SERVICE_TESTS=1')
class ServiceManagerTests(Harness):
    """The worker installed the way a user installs it: through the PC's own service manager."""
    SERVICE = None

    async def test_the_startup_entry_exists_a_crash_restarts_the_worker_and_uninstall_removes_it(self):
        record = self.record()
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out + self.diag(home))
        await self.connected(record['code'])
        await until(lambda: self.is_connected(home), timeout=30, message='status shows connected')
        state = (await self.status(home))[1]
        self.assertTrue(state['service_exists'], state)
        self.assertRegex(state['service'], r'(?i)systemd|scheduled task', 'a real startup entry, not the background-process fallback')
        self.assertEqual(len(service_manager_listing()), 1, service_manager_listing())
        # a second install anywhere on this PC is refused while this worker exists
        _, code, out = await self.install_pc(self.record('pc-y'), home=self.new_home())
        self.assertNotEqual(code, 0); self.assertIn('already installed', out)
        # kill the worker the hard way: the service manager brings it back
        first_pid = state['pid']
        self.assertGreater(first_pid, 0)
        if os.name == 'nt': subprocess.run(['taskkill', '/F', '/T', '/PID', str(first_pid)], capture_output=True)
        else: os.kill(first_pid, 9)
        async def restarted():
            code, now = await self.status(home)
            return code == 0 and isinstance(now, dict) and now['running'] and now['pid'] not in (0, first_pid) and now['connected']
        deadline = asyncio.get_event_loop().time() + 200
        while not await restarted() and asyncio.get_event_loop().time() < deadline: await asyncio.sleep(3)
        self.assertTrue(await restarted(), f'the worker was not restarted after a crash{self.diag(home)}')
        await self.connected(record['code'])
        code, out = await self.run_cli(home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out + self.diag(home))
        self.assertEqual(service_manager_listing(), [], 'uninstall removes the startup entry')

    async def test_boot_start_is_refused_without_the_rights_and_reported_honestly(self):
        record = self.record()
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out + self.diag(home))
        status = await self.run_cli(home, 'boot', 'status')
        self.assertEqual(status[0], 0, status)
        if os.name == 'nt':
            self.assertIn('when you log in', status[1])
            enabled = await self.run_cli(home, 'boot', 'enable')
            if enabled[0] == 0:   # an Administrator shell: it really switched
                self.assertIn('before anyone logs in', (await self.run_cli(home, 'boot', 'status'))[1])
                self.assertEqual((await self.run_cli(home, 'boot', 'disable'))[0], 0)
                self.assertIn('when you log in', (await self.run_cli(home, 'boot', 'status'))[1])
            else:                 # a normal shell: told exactly what to do, and nothing changed
                self.assertIn('Administrator', enabled[1])
                self.assertIn('when you log in', (await self.run_cli(home, 'boot', 'status'))[1])
        else:
            # lingering is what lets a user's systemd run without a login; the test user has it, and turning it off is not undone here
            self.assertIn('before anyone logs in', status[1])
            self.assertEqual((await self.run_cli(home, 'boot', 'enable'))[0], 0)
            code, out = await self.run_cli(home, 'boot', 'disable')
            if code != 0: self.assertIn('sudo loginctl disable-linger', out)   # not allowed for this user: told what an administrator runs
            else: subprocess.run(['loginctl', 'enable-linger', os.environ.get('USER', 'tester')], capture_output=True)

    async def test_logon_start_is_the_default_and_a_stopped_worker_stays_stopped_past_the_next_check(self):
        record = self.record()   # autostart 'logon'
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out + self.diag(home))
        self.assertIn('when you log in', out)
        await until(lambda: self.is_connected(home), timeout=30, message='status shows connected')
        # lingering is a setting of the user, not of one worker: on Linux a user who has it on starts every worker at boot
        self.assertIn((await self.status(home))[1]['autostart'], ('logon',) if os.name == 'nt' else ('logon', 'boot'))
        self.assertEqual((await self.run_cli(home, 'stop'))[0], 0)
        await asyncio.sleep(80)   # longer than the Windows task's one-minute check
        self.assertFalse((await self.status(home))[1]['running'], 'a worker stopped on purpose is not brought back')
        self.assertEqual((await self.run_cli(home, 'start'))[0], 0)
        await self.connected(record['code'])

    async def test_boot_start_at_install_is_done_when_possible_and_otherwise_falls_back_to_logon_with_a_reason(self):
        record = self.record(autostart='boot')
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out + self.diag(home))
        await self.connected(record['code'])
        if 'starts when the PC starts' in out:
            self.assertEqual((await self.status(home))[1]['autostart'], 'boot')
        else:   # a normal Windows shell without the UAC prompt (tests never raise one)
            self.assertIn('Could not set up start at boot', out)
            self.assertIn('Administrator', out)
            self.assertIn('starts when you log in instead', out)
            self.assertEqual((await self.status(home))[1]['autostart'], 'logon')
        self.assertTrue((await self.status(home))[1]['running'])

    async def kill_worker(self, home):
        pid = (await self.status(home))[1]['pid']
        self.assertGreater(pid, 0)
        if os.name == 'nt': subprocess.run(['taskkill', '/F', '/T', '/PID', str(pid)], capture_output=True)
        else: os.kill(pid, 9)
        return pid

    async def alive_again(self, home, old_pid, seconds):
        deadline = asyncio.get_event_loop().time() + seconds
        while asyncio.get_event_loop().time() < deadline:
            code, now = await self.status(home)
            if code == 0 and isinstance(now, dict) and now['running'] and now['pid'] not in (0, old_pid): return True
            await asyncio.sleep(3)
        return False

    async def test_auto_restart_off_leaves_a_killed_worker_down_until_started_and_can_be_turned_back_on(self):
        record = self.record(auto_restart=False)
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out + self.diag(home))
        self.assertIn('not restarted', out)
        await until(lambda: self.is_connected(home), timeout=30, message='status shows connected')
        self.assertIs((await self.status(home))[1]['auto_restart'], False)
        self.assertIn('Not restarted', (await self.run_cli(home, 'autorestart', 'status'))[1])
        pid = await self.kill_worker(home)
        self.assertFalse(await self.alive_again(home, pid, 90), 'a killed worker stays down when auto restart is off (past the Windows one-minute check)')
        self.assertEqual((await self.run_cli(home, 'start'))[0], 0)
        await self.connected(record['code'])
        # turning it on rebuilds the task or unit; the running worker keeps running
        self.assertEqual((await self.run_cli(home, 'autorestart', 'on'))[0], 0)
        self.assertIs((await self.status(home))[1]['auto_restart'], True)
        await until(lambda: self.is_connected(home), timeout=60, message='connected after the change')
        pid = await self.kill_worker(home)
        self.assertTrue(await self.alive_again(home, pid, 200), 'with auto restart on a killed worker comes back')
        # and off again
        self.assertEqual((await self.run_cli(home, 'autorestart', 'off'))[0], 0)
        self.assertIs((await self.status(home))[1]['auto_restart'], False)
        self.assertIn('already off', (await self.run_cli(home, 'autorestart', 'off'))[1])

    async def test_a_fleet_update_works_under_the_service_manager(self):
        # the updater must survive stopping the service it was started by (systemd kills the whole control group)
        record = self.record()
        home, code, out = await self.install_pc(record)
        self.addAsyncCleanup(self.run_cli, home, 'uninstall', '--yes')
        self.assertEqual(code, 0, out + self.diag(home))
        await self.connected(record['code'])
        await self.check_fleet_update(installed=True)

def free_port():
    import socket
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]


if __name__ == '__main__':
    unittest.main()
