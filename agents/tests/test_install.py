"""One-line PC install: form validation, one-time records, the command with the form's values as flags, the claim by a PC's
uuid and the /install routes. No script is served: the command downloads the worker program and runs its `install`."""
import asyncio
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import threading
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
from aiohttp.test_utils import TestClient, TestServer
from flowkit import install
from flowkit.relay import make_app

ROOT = Path(__file__).resolve().parents[2]
FORM = {'id': 'build-pc', 'os': 'windows', 'engines': ['hatchet', 'prefect'], 'pool': 'build', 'tags': ['gpu'], 'slots': 2,
        'cpu_limit': 80, 'ram_limit': 85, 'server': 'http://192.168.1.10:8090', 'autostart': 'boot', 'auto_restart': False}
GOOD_TOKEN = 'abcdefghijklmnopqrstuvwxyzABCDEF0123456789_-'


SECRET = 'pc-claim-secret-0123456789abcdefghij'


def claim_body(**changes):
    """What a PC running `agent-worker install <uuid> --server ...` sends to /claim."""
    return {'claim': SECRET, 'host_os': 'windows', 'hostname': 'PC-1', **changes}


class Isolated(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        binaries = self.root / 'binaries'; binaries.mkdir()
        for name in install.TARGETS.values(): (binaries / name).write_bytes(b'MZ-fake-worker-' + name.encode())
        env = patch.dict(os.environ, {'FLOW_STATE_DIR': str(self.root/'state'), 'FLOW_WORKERS_FILE': str(self.root/'workers.yaml'),
                                      'FLOW_WORKERS_ENV_FILE': str(self.root/'workers.env'), 'FLOW_WORKER_BINARIES': str(binaries)})
        env.start(); self.addCleanup(env.stop)


class AutomaticUpdateTests(Isolated, unittest.IsolatedAsyncioTestCase):
    async def test_first_status_after_boot_updates_immediately_and_retries_are_bounded(self):
        from types import SimpleNamespace
        from flowkit import update
        from flowkit.relay import Relay
        relay = Relay(lambda _: 'token')
        app = {'relay': relay}
        update.start_fleet_update()
        status = {'worker_os': 'windows', 'worker_version': 'old', 'draining': False}
        clock = SimpleNamespace(monotonic=lambda: 5.0, time=time.time)
        with patch.object(update, 'time', clock):
            update.on_status(app, 'pc', status)
            peer = relay.peer('pc')
            self.assertEqual(peer.update_attempts, 1)
            self.assertEqual(len(peer.pending), 1)
            update.on_status(app, 'pc', status)
            self.assertEqual(peer.update_attempts, 1)
            for attempt in range(2, update.MAX_ATTEMPTS + 2):
                clock.monotonic = lambda attempt=attempt: 5.0 + attempt * update.RETRY_AFTER_SECONDS
                update.on_status(app, 'pc', status)
            self.assertEqual(peer.update_attempts, update.MAX_ATTEMPTS)
            self.assertEqual(len(peer.pending), update.MAX_ATTEMPTS)
            binary = install.binary_path('windows')
            binary.write_bytes(binary.read_bytes() + b'-new-target')
            update.start_fleet_update()
            update.on_status(app, 'pc', status)
            self.assertEqual(peer.update_attempts, 1)
            self.assertEqual(len(peer.pending), update.MAX_ATTEMPTS + 1)


class FormTests(Isolated):
    def test_a_valid_form_is_normalized(self):
        form = install.validate_form({**FORM, 'tags': ['gpu', 'gpu']})
        self.assertEqual((form['id'], form['engines'], form['pools'], form['tags'], form['server']), ('build-pc', ['hatchet', 'prefect'], ['build'], ['gpu'], 'http://192.168.1.10:8090'))

    def test_autostart_defaults_to_logon_and_takes_boot_or_none(self):
        base = {k: v for k, v in FORM.items() if k != 'autostart'}
        self.assertEqual(install.validate_form(base)['autostart'], 'logon')
        for choice in ('logon', 'boot', 'none'):
            self.assertEqual(install.validate_form({**base, 'autostart': choice})['autostart'], choice)
        self.assertEqual(install.view(install.create({**base, 'id': 'viewed', 'autostart': 'none'}))['autostart'], 'none')

    def test_auto_restart_defaults_to_on_and_takes_a_boolean(self):
        base = {k: v for k, v in FORM.items() if k != 'auto_restart'}
        self.assertIs(install.validate_form(base)['auto_restart'], True)
        self.assertIs(install.validate_form({**base, 'auto_restart': False})['auto_restart'], False)
        self.assertIs(install.view(install.create({**base, 'id': 'norestart', 'auto_restart': False}))['auto_restart'], False)

    def test_every_field_is_checked(self):
        bad = {'id': ['', 'a b', '-x', 'x' * 65, 7, 'a/b', '../x', 'a\nb', "a'b"], 'os': ['', 'beos', None], 'engines': [[], ['docker'], 'hatchet', None],
               'pool': ['', ' x', 'a,b', None, 'x' * 101], 'slots': [0, 101, 1.5, True, '2'], 'cpu_limit': [0, 101, -1, True, 'x'], 'ram_limit': [0, 101],
               'tags': ['gpu', ['a,b'], [''], ['x'] * 51], 'autostart': ['', 'startup', None, True, 'Boot'], 'auto_restart': ['yes', 0, None, 'false']}
        for field, values in bad.items():
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaises(install.InstallError):
                    install.validate_form({**FORM, field: value})
        with self.assertRaises(install.InstallError): install.validate_form({**FORM, 'extra': 1})
        with self.assertRaises(install.InstallError): install.validate_form('nope')

    def test_the_install_id_is_a_server_generated_uuid(self):
        a, b = install.create(FORM), install.create({**FORM, 'id': 'other'})
        for record in (a, b):
            self.assertTrue(install.CODE.match(record['code']))
            self.assertEqual(str(uuid.UUID(record['code'])), record['code'])
            self.assertEqual(uuid.UUID(record['code']).version, 4)
        self.assertNotEqual(a['code'], b['code'])

    def test_the_server_address_must_be_reachable_from_another_pc(self):
        for address in ('', None, 'localhost:8090', 'http://localhost:8090', 'http://127.0.0.1:8090', 'http://[::1]:8090', 'http://0.0.0.0:8090', 'ftp://x:8090',
                        'http://u:p@host:8090', 'http://host:8090/path', 'http://host:8090?x=1', 'http://host:99999', 'http://', 'http://foo.localhost:8090'):
            with self.subTest(address=address), self.assertRaises(install.InstallError):
                install.validate_form({**FORM, 'server': address})
        self.assertEqual(install.server_address('https://board.example.ts.net/'), 'https://board.example.ts.net')

    def test_names_already_registered_or_waiting_are_refused(self):
        with self.assertRaisesRegex(install.InstallError, 'already registered'): install.validate_form(FORM, registered={'build-pc'})
        with self.assertRaisesRegex(install.InstallError, 'waiting'): install.validate_form(FORM, pending={'build-pc'})

    def test_creating_twice_for_one_name_is_refused_until_cancelled(self):
        record = install.create(FORM)
        with self.assertRaisesRegex(install.InstallError, 'waiting'): install.create(FORM)
        install.cancel(record['code'])
        self.assertEqual(install.create(FORM)['state'], 'pending')

    def test_unregistration_releases_name_and_marks_the_old_install_inactive(self):
        import flow_host
        from flowkit.fleet_registry import save_worker
        worker_uuid = '90b96c8e-0210-420c-a6b1-586d54d79a02'
        install.write_record({'code': worker_uuid, 'id': FORM['id'], 'state': 'registered',
                              'created_at': install.now(), 'expires_at': install.now() + install.VALID_SECONDS})
        save_worker({'id': FORM['id'], 'uuid': worker_uuid}, create=True)

        self.assertEqual(flow_host.unregister_fleet_worker(FORM['id'])['id'], FORM['id'])
        self.assertEqual(install.read_record(worker_uuid)['state'], 'unregistered')
        replacement = install.create(FORM)
        self.assertEqual((replacement['id'], replacement['state']), (FORM['id'], 'pending'))

    def test_a_link_needs_a_worker_program_for_the_chosen_os(self):
        for name in install.TARGETS.values(): (Path(os.environ['FLOW_WORKER_BINARIES']) / name).unlink()
        with self.assertRaisesRegex(install.InstallError, 'no worker program for windows'): install.create(FORM)


class RecordTests(Isolated):
    def test_the_code_is_long_random_and_single_purpose(self):
        a, b = install.create(FORM), install.create({**FORM, 'id': 'other'})
        self.assertNotEqual(a['code'], b['code'])
        self.assertTrue(install.CODE.match(a['code']))
        self.assertGreater(a['expires_at'] - a['created_at'], 29 * 60)

    def test_the_view_never_carries_secrets_and_expiry_is_computed(self):
        record = install.create(FORM)
        shown = install.view(record)
        self.assertEqual(shown['state'], 'pending')
        self.assertNotIn('token', json.dumps(shown))
        with patch.object(install, 'now', lambda: record['expires_at'] + 1): self.assertEqual(install.view(record)['state'], 'expired')

    def test_cancelled_records_are_unusable_and_old_finished_ones_are_removed(self):
        record = install.create(FORM)
        self.assertEqual(install.cancel(record['code'])['state'], 'cancelled')
        with patch.object(install, 'now', lambda: record['expires_at'] + install.KEEP_SECONDS + 1): self.assertEqual(install.all_records(), [])
        self.assertFalse(install.record_path(record['code']).exists())

    def test_a_malformed_code_never_reads_a_file(self):
        for code in ('../x', 'a' * 43 + '/..', '', None, 'short'): self.assertIsNone(install.read_record(code))

    def test_the_token_variable_name_is_unique_and_safe(self):
        self.assertEqual(install.token_env_for('build-pc'), 'FLOW_WORKER_TOKEN_BUILD_PC')
        self.assertEqual(install.token_env_for('build-pc', {'FLOW_WORKER_TOKEN_BUILD_PC'}), 'FLOW_WORKER_TOKEN_BUILD_PC_2')


class CommandTests(Isolated):
    def record(self, **changes): return install.create({**FORM, **changes})

    def test_each_shell_gets_one_line_that_downloads_the_program_and_runs_install_with_the_uuid_and_the_server(self):
        record = self.record()
        commands = install.commands(record)
        uid, origin = record['code'], 'http://192.168.1.10:8090'
        for shell, quote in (('powershell', "'%s'"), ('cmd', '"%s"'), ('bash', "'%s'")):
            text = commands[shell]
            self.assertNotIn('\n', text, shell)
            self.assertIn(f'{origin}/install/{uid}/agent-worker', text, shell)
            self.assertIn(f' install {uid} --server ' + quote % origin, text, shell)
            for flag in ('--name', '--slots', '--engine', '--pool', '--cpu-limit', '--os', '--code'):
                self.assertNotIn(flag, text, f'{shell}: the program gets these from the server, not from the command')
        self.assertIn('iwr -UseBasicParsing', commands['powershell'])
        self.assertTrue(commands['cmd'].startswith('curl.exe -fsSL'))
        self.assertTrue(commands['bash'].startswith('f=$(mktemp) && curl -fsSL'))
        self.assertIn('normal user', commands['elevation'])

    def test_no_script_is_involved_and_nothing_but_the_uuid_and_address_identify_the_install(self):
        record = self.record(pool='anything goes', tags=['it is a tag'])
        text = json.dumps(install.commands(record))
        for forbidden in ('| iex', '| bash', '| sh', 'install.ps1', 'install.sh', 'Invoke-Expression', 'anything goes', 'it is a tag'):
            self.assertNotIn(forbidden, text)
        self.assertFalse((ROOT / 'agents' / 'flowkit' / 'install_templates').exists())

    def test_the_bash_command_parses(self):
        bash = shutil.which('bash')
        if not bash: self.skipTest('bash is not installed')
        self.assertEqual(subprocess.run([bash, '-n', '-c', install.commands(self.record())['bash']], capture_output=True, text=True).returncode, 0)

    def test_the_powershell_command_parses(self):
        powershell = shutil.which('powershell') or shutil.which('pwsh')
        if not powershell: self.skipTest('PowerShell is not installed')
        path = self.root / 'command.ps1'; path.write_text(install.commands(self.record())['powershell'], encoding='utf-8')
        command = ("$e=$null; [void][System.Management.Automation.Language.Parser]::ParseFile('%s',[ref]$null,[ref]$e); "
                   "if($e){$e|%%{$_.Message}; exit 1}") % str(path).replace("'", "''")
        result = subprocess.run([powershell, '-NoProfile', '-Command', command], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_the_commands_never_ask_for_elevation_or_a_firewall_change(self):
        text = json.dumps({k: v for k, v in install.commands(self.record()).items() if k != 'elevation'})
        self.assertNotRegex(text, r'(?i)\bsudo\b|runas|-Verb|New-NetFirewallRule|netsh|firewall-cmd|ufw ')

    def test_update_link_commands_run_the_downloaded_program_in_update_mode(self):
        from flowkit import update
        link = update.create_link('build-pc', 'linux')
        commands = update.link_commands(link, 'http://192.168.1.10:8090')
        for shell in ('powershell', 'cmd', 'bash'):
            self.assertIn(f'/update/{link["code"]}/agent-worker', commands[shell])
            self.assertIn('update --server', commands[shell]); self.assertIn('--force', commands[shell])
            self.assertNotIn('| iex', commands[shell]); self.assertNotIn('| bash', commands[shell])
        self.assertEqual(commands['local'], 'agent-worker update')


class RegistrationTests(Isolated):
    def test_registering_stores_the_token_privately_and_adds_a_relayed_worker_keyed_by_the_installs_uuid(self):
        from flowkit.fleet_registry import _read
        from flowkit.relay import worker_url
        record = install.create(FORM)
        env = install.register(record, GOOD_TOKEN, 'HOST')
        self.assertIn(f'{env}={GOOD_TOKEN}', (self.root/'workers.env').read_text())
        if os.name != 'nt': self.assertEqual((self.root/'workers.env').stat().st_mode & 0o777, 0o600)
        worker = next(w for w in _read() if w['id'] == 'build-pc')
        self.assertEqual((worker['uuid'], worker['url'], worker['pools'], worker['tags'], worker['engines'], worker['slots'], worker['token_env']),
                         (record['code'], worker_url(record['code']), ['build'], ['gpu'], ['hatchet', 'prefect'], 2, env))
        self.assertNotIn(GOOD_TOKEN, (self.root/'workers.yaml').read_text())  # tokens never go into the registry

    def test_the_registry_never_holds_two_workers_with_one_uuid_or_changes_a_uuid(self):
        from flowkit.fleet_registry import save_worker
        pc = str(uuid.uuid4())
        save_worker({'id': 'a', 'uuid': pc, 'pools': ['x'], 'slots': 1, 'url': 'http://127.0.0.1:8097/w/' + pc, 'token_env': 'A_TOKEN'}, create=True)
        with self.assertRaisesRegex(ValueError, 'already registered as a'):
            save_worker({'id': 'b', 'uuid': pc, 'pools': ['x'], 'slots': 1, 'url': 'http://127.0.0.1:8097/w/' + pc, 'token_env': 'B_TOKEN'}, create=True)
        with self.assertRaisesRegex(ValueError, 'never changes'): save_worker({'id': 'a', 'uuid': str(uuid.uuid4())})
        with self.assertRaisesRegex(ValueError, 'Invalid worker uuid'): save_worker({'id': 'c', 'uuid': 'nope', 'pools': ['x']}, create=True)

    def test_invalid_tokens_are_refused(self):
        record = install.create(FORM)
        for token in (None, '', 'short', 'x' * 201, GOOD_TOKEN + ' ', GOOD_TOKEN + '\n', 'a' * 40 + '=', 7):
            with self.subTest(token=token), self.assertRaises(install.InstallError): install.register(record, token)

    def test_env_file_updates_replace_rather_than_duplicate(self):
        path = self.root/'workers.env'
        install.set_env_value('A_B', 'one', path); install.set_env_value('C', 'two', path); install.set_env_value('A_B', 'three', path)
        self.assertEqual(sorted(path.read_text().split()), ['A_B=three', 'C=two'])


class RouteTests(Isolated, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        Isolated.setUp(self)
        self.tokens = {}
        self.server = TestServer(make_app(token_for=self.tokens.get, install=install.add_routes)); await self.server.start_server()
        self.addAsyncCleanup(self.server.close)
        self.http = aiohttp.ClientSession(); self.addAsyncCleanup(self.http.close)
        self.url = f'http://127.0.0.1:{self.server.port}'
        install.FAILED_ATTEMPTS.clear()
        self.delay = patch.object(install, 'WRONG_CODE_DELAY', 0); self.delay.start(); self.addCleanup(self.delay.stop)

    def make(self, **changes): return install.create({**FORM, **changes})

    async def claim(self, record, **changes):
        async with self.http.post(f'{self.url}/install/{record["code"]}/claim', json=claim_body(**changes)) as response:
            return response.status, await response.json()

    async def test_the_worker_program_is_served_for_a_pending_install_and_no_script_exists(self):
        record = self.make()
        async with self.http.get(f'{self.url}/install/{record["code"]}/agent-worker') as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.read(), b'MZ-fake-worker-agent-worker-x86_64-pc-windows-msvc.exe')
            self.assertEqual(response.headers['X-Worker-Version'], install.binary_version('windows'))
        for path in ('install.ps1', 'install.sh', 'settings', 'verify'):
            async with self.http.get(f'{self.url}/install/{record["code"]}/{path}') as response: self.assertEqual(response.status, 404, path)

    async def test_checking_in_claims_the_install_and_returns_what_the_form_saved(self):
        record = self.make(tags=['gpu', 'cuda'])
        status, body = await self.claim(record)
        self.assertEqual(status, 200)
        self.assertEqual({k: body[k] for k in ('ok', 'uuid', 'name', 'os', 'engines', 'pool', 'tags', 'slots', 'cpu_limit', 'ram_limit', 'autostart', 'auto_restart')},
                         {'ok': True, 'uuid': record['code'], 'name': 'build-pc', 'os': 'windows', 'engines': ['hatchet', 'prefect'], 'pool': 'build', 'tags': ['gpu', 'cuda'], 'slots': 2, 'cpu_limit': 80, 'ram_limit': 85, 'autostart': 'boot', 'auto_restart': False})
        shown = install.view(install.read_record(record['code']))
        self.assertEqual((shown['uuid'], shown['claimed_host']), (record['code'], 'PC-1'))
        self.assertNotIn(SECRET, json.dumps(install.read_record(record['code'])), 'only a hash of the secret is stored')

    async def test_another_pc_running_the_same_command_is_refused_with_who_has_it(self):
        record = self.make()
        self.assertEqual((await self.claim(record, hostname='FIRST-PC'))[0], 200)
        status, body = await self.claim(record, claim='another-pcs-own-secret-0123456789abc', hostname='SECOND-PC')
        self.assertEqual(status, 409)
        self.assertIn('already used by another PC (FIRST-PC', body['error'])
        self.assertIn('create a new link', body['error'])

    async def test_the_same_pc_may_check_in_again_after_a_failed_attempt(self):
        record = self.make()
        for _ in range(3): self.assertEqual((await self.claim(record))[0], 200)

    async def test_the_pcs_real_os_must_match_the_form(self):
        record = self.make()
        status, body = await self.claim(record, host_os='linux')
        self.assertEqual(status, 409)
        self.assertIn('The form says this PC runs windows but it runs linux', body['error'])
        self.assertIn('choose linux', body['error'])
        self.assertIsNone(install.read_record(record['code']).get('claim_hash'), 'a refused PC claims nothing')

    async def test_a_missing_or_weak_claim_secret_is_refused(self):
        record = self.make()
        for body in ({}, {'claim': ''}, {'claim': 'short'}, {'claim': 7}):
            async with self.http.post(f'{self.url}/install/{record["code"]}/claim', json=body) as response: self.assertEqual(response.status, 400)
        async with self.http.post(f'{self.url}/install/{record["code"]}/claim', data='nope', headers={'Content-Type': 'application/json'}) as response: self.assertEqual(response.status, 400)

    async def test_a_uuid_that_is_already_registered_cannot_be_claimed_again(self):
        record = self.make()
        install.register(record, GOOD_TOKEN)
        status, body = await self.claim(record)
        self.assertEqual(status, 409)
        self.assertIn('already registered as build-pc', body['error'])
        self.assertIsNone(install.read_record(record['code']).get('claim_hash'))

    async def test_a_registered_fleet_name_is_refused_before_claiming_a_new_link(self):
        active = self.make()
        install.register(active, GOOD_TOKEN)
        replacement = self.make(id='next-pc')
        replacement['id'] = active['id']
        install.write_record(replacement)

        status, body = await self.claim(replacement)
        self.assertEqual(status, 409)
        self.assertIn('Fleet name build-pc is already registered', body['error'])
        self.assertIsNone(install.read_record(replacement['code']).get('claim_hash'))
    async def test_unknown_wrong_expired_cancelled_and_used_codes_all_look_the_same(self):
        record = self.make()
        install.cancel(record['code'])
        expired = self.make(id='old')
        results = []
        with patch.object(install, 'now', lambda: expired['expires_at'] + 5):
            async with self.http.post(f'{self.url}/install/{expired["code"]}/claim', json=claim_body()) as response: results.append((response.status, await response.text()))
        for code in (str(uuid.uuid4()), record['code'], 'short'):
            async with self.http.post(f'{self.url}/install/{code}/claim', json=claim_body()) as response: results.append((response.status, await response.text()))
        async with self.http.get(f'{self.url}/install/../../etc/passwd') as response: self.assertEqual(response.status, 404)
        self.assertEqual({r[0] for r in results}, {404})
        self.assertEqual(len({r[1] for r in results}), 1)

    async def test_wrong_codes_from_one_source_are_limited_by_the_peer_the_board_names(self):
        statuses = []
        for i in range(14):
            async with self.http.get(f'{self.url}/install/{str(uuid.UUID(int=7, version=4))}/agent-worker', headers={'X-Relay-Client': '10.1.1.1'}) as response: statuses.append(response.status)
        self.assertEqual(statuses[:10], [404] * 10)
        self.assertEqual(set(statuses[10:]), {429})
        async with self.http.get(f'{self.url}/install/{str(uuid.UUID(int=7, version=4))}/agent-worker', headers={'X-Relay-Client': '10.2.2.2'}) as response: self.assertEqual(response.status, 404)

    async def test_progress_is_recorded_and_failures_show_their_reason(self):
        record = self.make()
        async with self.http.post(f'{self.url}/install/{record["code"]}/progress', json={'step': 'service', 'state': 'running'}) as response: self.assertEqual(response.status, 204)
        self.assertEqual(install.view(install.read_record(record['code']))['step'], 'service')
        async with self.http.post(f'{self.url}/install/{record["code"]}/progress', json={'step': 'check', 'state': 'failed', 'message': 'A worker is already installed on this PC'}) as response: self.assertEqual(response.status, 204)
        shown = install.view(install.read_record(record['code']))
        self.assertEqual((shown['step'], shown['error']), ('check', 'A worker is already installed on this PC'))
        async with self.http.post(f'{self.url}/install/{record["code"]}/progress', json={'step': 'bogus'}) as response: self.assertEqual(response.status, 400)

    async def test_completion_registers_the_pc_by_its_uuid_once_and_burns_the_link(self):
        from flowkit.fleet_registry import _read
        record = self.make()
        self.assertEqual((await self.claim(record))[0], 200)
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN, 'claim': SECRET, 'hostname': 'PC-1'}) as response:
            self.assertEqual((response.status, await response.json()), (200, {'ok': True, 'id': 'build-pc'}))
        worker = next(w for w in _read() if w['id'] == 'build-pc')
        self.assertEqual(worker['uuid'], record['code'])
        shown = install.view(install.read_record(record['code']))
        self.assertEqual((shown['state'], shown['hostname'], shown['uuid']), ('registered', 'PC-1', record['code']))
        async with self.http.get(f'{self.url}/install/{record["code"]}/agent-worker') as response: self.assertEqual(response.status, 404)
        async with self.http.post(f'{self.url}/install/{record["code"]}/claim', json=claim_body()) as response: self.assertEqual(response.status, 404)
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': 'q' * 40, 'claim': SECRET}) as response: self.assertEqual(response.status, 404)
        self.assertEqual(len([w for w in _read() if w['id'] == 'build-pc']), 1)

    async def test_a_registration_from_a_pc_that_did_not_claim_the_link_is_refused(self):
        record = self.make()
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN, 'claim': SECRET}) as response:
            self.assertEqual(response.status, 409)  # nobody has claimed it
        self.assertEqual((await self.claim(record))[0], 200)
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN, 'claim': 'someone-elses-secret-0123456789abcdef'}) as response:
            self.assertEqual(response.status, 409)
            self.assertIn('claimed by a different PC', (await response.json())['error'])

    async def test_a_bad_token_is_refused_and_the_link_stays_usable(self):
        record = self.make()
        await self.claim(record)
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': 'weak', 'claim': SECRET}) as response: self.assertEqual(response.status, 409)
        self.assertEqual(install.view(install.read_record(record['code']))['state'], 'pending')
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN, 'claim': SECRET}) as response: self.assertEqual(response.status, 200)

    async def test_state_reports_connection_through_the_relay_by_uuid(self):
        record = self.make()
        await self.claim(record)
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN, 'claim': SECRET}) as response: self.assertEqual(response.status, 200)
        async with self.http.get(f'{self.url}/install/{record["code"]}/state') as response: self.assertFalse((await response.json())['connected'])
        self.server.app['relay'].peer(record['code']).last_poll = time.monotonic()
        async with self.http.get(f'{self.url}/install/{record["code"]}/state') as response: body = await response.json()
        self.assertEqual((body['state'], body['connected'], body['uuid']), ('registered', True, record['code']))
        self.assertNotIn(GOOD_TOKEN, json.dumps(body))


class BoardInstallContractTests(Isolated, unittest.IsolatedAsyncioTestCase):
    """The real board, authenticated flow host and install service share one temporary registry."""
    async def asyncSetUp(self):
        import flow_host
        with patch.object(sys, 'path', [str(ROOT / 'board'), *sys.path]):
            import server as board_server

        env = patch.dict(os.environ, {'FLOW_HOST_TOKEN': 'install-test-host-secret',
                                      'BOARD_PASSWORD': 'install-test-password', 'BOARD_SECRET': 'install-test-cookie-key'})
        env.start(); self.addCleanup(env.stop)
        attempts = patch.object(install, 'FAILED_ATTEMPTS', {})
        attempts.start(); self.addCleanup(attempts.stop)
        delay = patch.object(install, 'WRONG_CODE_DELAY', 0)
        delay.start(); self.addCleanup(delay.stop)
        self.relay = TestServer(make_app(install=install.add_routes))
        self.addAsyncCleanup(self.relay.close)
        await self.relay.start_server()
        host = flow_host.serve(None, '127.0.0.1', 0)
        self.addCleanup(host.server_close)
        self.host_url = f'http://127.0.0.1:{host.server_port}'
        # The enclosing environment patch restores these addresses after all three servers close.
        os.environ.update({'FLOW_HOST_URL': self.host_url, 'FLOW_RELAY_URL': str(self.relay.make_url('')),
                           'FLOW_RELAY_LOCAL_URL': str(self.relay.make_url(''))})
        thread = threading.Thread(target=host.serve_forever, daemon=True)
        thread.start()
        self.addAsyncCleanup(asyncio.to_thread, thread.join)
        self.addAsyncCleanup(asyncio.to_thread, host.shutdown)

        with patch.object(board_server, 'load_adapters', return_value={}):
            app = board_server.make_app()
        self.client = TestClient(TestServer(app), cookie_jar=aiohttp.CookieJar(unsafe=True))
        self.addAsyncCleanup(self.client.close)
        await self.client.start_server()
        self.pc = aiohttp.ClientSession()
        self.addAsyncCleanup(self.pc.close)
        async with self.client.post('/login', data={'password': 'install-test-password'}, allow_redirects=False) as response:
            self.assertEqual(response.status, 302)

    async def api(self, method, path='/api/fleet/installs', **kwargs):
        async with self.client.request(method, path, **kwargs) as response:
            return response.status, await response.json()

    async def claim(self, code):
        async with self.pc.post(self.client.make_url(f'/install/{code}/claim'), json=claim_body()) as response:
            return response.status, await response.json()

    async def download(self, code):
        async with self.pc.get(self.client.make_url(f'/install/{code}/agent-worker')) as response:
            return response.status, await response.read()

    async def test_startup_choices_survive_board_creation_listing_and_the_pc_claim(self):
        for autostart in ('logon', 'boot', 'none'):
            for auto_restart in (True, False):
                with self.subTest(autostart=autostart, auto_restart=auto_restart):
                    name = f'{autostart}-restart-{int(auto_restart)}'
                    status, created = await self.api('POST', json={**FORM, 'id': name, 'autostart': autostart, 'auto_restart': auto_restart})
                    self.assertEqual(status, 200, created)
                    code = created['install']['code']
                    self.assertEqual((str(uuid.UUID(code)), uuid.UUID(code).version), (code, 4))
                    status, listing = await self.api('GET')
                    self.assertEqual(status, 200, listing)
                    saved = next(item for item in listing['installs'] if item['code'] == code)
                    self.assertEqual((saved['id'], saved['autostart'], saved['auto_restart'], saved['state']),
                                     (name, autostart, auto_restart, 'pending'))
                    status, settings = await self.claim(code)
                    self.assertEqual(status, 200, settings)
                    self.assertEqual({key: settings[key] for key in ('ok', 'uuid', 'name', 'os', 'engines', 'pool', 'tags', 'slots',
                                                                    'cpu_limit', 'ram_limit', 'autostart', 'auto_restart')},
                                     {'ok': True, 'uuid': code, 'name': name, 'os': 'windows', 'engines': ['hatchet', 'prefect'],
                                      'pool': 'build', 'tags': ['gpu'], 'slots': 2, 'cpu_limit': 80, 'ram_limit': 85,
                                      'autostart': autostart, 'auto_restart': auto_restart})

    async def test_bad_startup_choices_and_unknown_fields_never_create_an_install(self):
        for changes in ({'autostart': 'startup'}, {'autostart': 'Boot'}, {'autostart': None}, {'autostart': True},
                        {'auto_restart': 'false'}, {'auto_restart': 0}, {'auto_restart': None}, {'extra': True}):
            with self.subTest(changes=changes):
                status, rejected = await self.api('POST', json={**FORM, **changes})
                self.assertEqual(status, 400, rejected)
                status, listing = await self.api('GET')
                self.assertEqual((status, listing), (200, {'installs': []}))
        status, created = await self.api('POST', json=FORM)
        self.assertEqual(status, 200, created)
        self.assertEqual((await self.claim(created['install']['code']))[0], 200)

    async def test_cancelling_a_uuid_burns_its_download_and_claim_and_frees_the_name(self):
        status, created = await self.api('POST', json=FORM)
        self.assertEqual(status, 200, created)
        code = created['install']['code']
        expected_binary = b'MZ-fake-worker-agent-worker-x86_64-pc-windows-msvc.exe'
        self.assertEqual(await self.download(code), (200, expected_binary))
        status, duplicate = await self.api('POST', json=FORM)
        self.assertEqual(status, 400, duplicate)

        status, cancelled = await self.api('DELETE', f'/api/fleet/installs/{code}')
        self.assertEqual(status, 200, cancelled)
        self.assertEqual((cancelled['install']['code'], cancelled['install']['state']), (code, 'cancelled'))
        self.assertEqual((await self.download(code))[0], 404)
        self.assertEqual((await self.claim(code))[0], 404)
        self.assertEqual(await self.api('GET'), (200, {'installs': []}))

        status, fresh = await self.api('POST', json=FORM)
        self.assertEqual(status, 200, fresh)
        replacement = fresh['install']['code']
        self.assertNotEqual(replacement, code)
        self.assertEqual(await self.download(replacement), (200, expected_binary))
        status, settings = await self.claim(replacement)
        self.assertEqual(status, 200, settings)
        self.assertEqual((settings['uuid'], settings['name'], settings['autostart'], settings['auto_restart']),
                         (replacement, 'build-pc', 'boot', False))
        self.assertEqual((await self.download(code))[0], 404)

    async def test_bad_codes_and_unauthenticated_management_cannot_cancel_a_pending_install(self):
        status, created = await self.api('POST', json=FORM)
        self.assertEqual(status, 200, created)
        code = created['install']['code']
        for method, path in (('GET', '/api/fleet/installs'), ('POST', '/api/fleet/installs'),
                             ('DELETE', f'/api/fleet/installs/{code}')):
            async with self.pc.request(method, self.client.make_url(path), json=FORM) as response:
                self.assertEqual(response.status, 401, path)
        for method, path in (('GET', '/fleet/installs'), ('POST', '/fleet/installs'),
                             ('DELETE', f'/fleet/installs/{code}')):
            async with self.pc.request(method, self.host_url + path, json=FORM) as response:
                self.assertEqual(response.status, 401, path)
        for bad_code in ('short', 'x' * 43, str(uuid.uuid4())):
            with self.subTest(code=bad_code):
                status, rejected = await self.api('DELETE', f'/api/fleet/installs/{bad_code}')
                self.assertEqual(status, 400, rejected)
                self.assertEqual((await self.download(bad_code))[0], 404)
                self.assertEqual((await self.claim(bad_code))[0], 404)
        status, listing = await self.api('GET')
        self.assertEqual(status, 200, listing)
        self.assertEqual([(item['code'], item['state']) for item in listing['installs']], [(code, 'pending')])
        self.assertEqual(await self.download(code), (200, b'MZ-fake-worker-agent-worker-x86_64-pc-windows-msvc.exe'))


class BundleTests(unittest.TestCase):
    """The Python worker bundle (the legacy install path and the test oracle's reference)."""
    def names(self):
        with tarfile.open(fileobj=io.BytesIO(install.build_bundle()), mode='r:gz') as archive: return archive.getnames()

    def test_the_bundle_has_what_a_python_pc_needs_and_nothing_else(self):
        names = self.names()
        for needed in ('agents/flow_execution_worker.py', 'agents/worker_runner.py', 'agents/relay_connector.py', 'agents/requirements-worker.txt', 'agents/flowkit/worker_service.py'):
            self.assertIn(needed, names)
        for name in names:
            self.assertFalse(re.search(r'(^|/)(\.venv|node_modules|runtime|tests|__pycache__|\.env|workers\.local)', name), name)
        self.assertNotIn('agents/flow_host.py', names)

    def test_worker_requirements_match_the_pinned_requirements(self):
        def pins(path):
            return {l.split('==')[0].lower(): l.split('==')[1] for l in (ROOT/'agents'/path).read_text().splitlines() if '==' in l and not l.startswith('#')}
        worker, full = pins('requirements-worker.txt'), pins('requirements.txt')
        for name, version in worker.items(): self.assertEqual(full.get(name), version, name)
        for path in ('../board/requirements.txt', '../board/requirements-hatchet.txt', 'requirements-flow.txt',
                     'requirements-flow-hatchet.txt', 'requirements-flow-prefect.txt'):
            for name, version in pins(path).items(): self.assertEqual(full.get(name), version, name)


class ConfluenceRuntimeRoute(unittest.IsolatedAsyncioTestCase):
    async def test_a_registered_pc_fetches_the_runtime_only_when_it_changed(self):
        from aiohttp.test_utils import TestClient, TestServer
        from flowkit import update
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {'FLOW_WORKER_BINARIES': folder}):
            app = make_app(token_for=lambda worker_id: GOOD_TOKEN if worker_id == 'pc' else None)
            update.add_routes(app)
            async with TestClient(TestServer(app), headers={'Authorization': f'Bearer {GOOD_TOKEN}'}) as client:
                path = '/relay/pc/update/confluence-runtime?os=windows&arch=x86_64'
                self.assertEqual((await client.get(path, headers={'Authorization': 'Bearer wrong'})).status, 401)
                self.assertEqual((await client.get(path.replace('/pc/', '/unknown/'))).status, 401)
                self.assertEqual((await client.get(path)).status, 404)
                (Path(folder)/'confluence-runtime-windows.tar.gz').write_bytes(b'runtime bytes')
                response = await client.get(path)
                self.assertEqual((response.status, await response.read()), (200, b'runtime bytes'))
                version = response.headers['X-Runtime-Version']
                self.assertEqual(version, hashlib.sha256(b'runtime bytes').hexdigest()[:16])
                self.assertEqual((await client.get(f'{path}&have={version}')).status, 304)
                self.assertEqual((await client.get(path.replace('windows', 'plan9'))).status, 400)


if __name__ == '__main__':
    unittest.main()
