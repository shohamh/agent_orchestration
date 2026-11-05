"""One-line PC install: form validation, one-time records, the bundle, the scripts and the /install routes."""
import asyncio
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
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import aiohttp
from aiohttp.test_utils import TestServer
from flowkit import install
from flowkit.relay import make_app

ROOT = Path(__file__).resolve().parents[2]
FORM = {'id': 'build-pc', 'os': 'windows', 'engines': ['hatchet', 'prefect'], 'pool': 'build', 'tags': ['gpu'], 'slots': 2,
        'cpu_limit': 80, 'ram_limit': 85, 'server': 'http://192.168.1.10:8090'}
GOOD_TOKEN = 'abcdefghijklmnopqrstuvwxyzABCDEF0123456789_-'


class Isolated(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        env = patch.dict(os.environ, {'FLOW_STATE_DIR': str(self.root/'state'), 'FLOW_WORKERS_FILE': str(self.root/'workers.yaml'),
                                      'FLOW_WORKERS_ENV_FILE': str(self.root/'workers.env')})
        env.start(); self.addCleanup(env.stop)


class FormTests(Isolated):
    def test_a_valid_form_is_normalized(self):
        form = install.validate_form({**FORM, 'tags': ['gpu', 'gpu']})
        self.assertEqual((form['id'], form['engines'], form['pools'], form['tags'], form['server']), ('build-pc', ['hatchet', 'prefect'], ['build'], ['gpu'], 'http://192.168.1.10:8090'))

    def test_every_field_is_checked(self):
        bad = {'id': ['', 'a b', '-x', 'x' * 65, 7, 'a/b', '../x', 'a\nb', "a'b"], 'os': ['', 'beos', None], 'engines': [[], ['docker'], 'hatchet', None],
               'pool': ['', ' x', 'a,b', None, 'x' * 101], 'slots': [0, 101, 1.5, True, '2'], 'cpu_limit': [0, 101, -1, True, 'x'], 'ram_limit': [0, 101],
               'tags': ['gpu', ['a,b'], [''], ['x'] * 51]}
        for field, values in bad.items():
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaises(install.InstallError):
                    install.validate_form({**FORM, field: value})
        with self.assertRaises(install.InstallError): install.validate_form({**FORM, 'extra': 1})
        with self.assertRaises(install.InstallError): install.validate_form('nope')

    def test_the_server_address_must_be_reachable_from_another_pc(self):
        for address in ('', None, 'localhost:8090', 'http://localhost:8090', 'http://127.0.0.1:8090', 'http://[::1]:8090', 'http://0.0.0.0:8090', 'ftp://x:8090',
                        'http://u:p@host:8090', 'http://host:8090/path', 'http://host:8090?x=1', 'http://host:99999', 'http://', 'http://foo.localhost:8090'):
            with self.subTest(address=address), self.assertRaises(install.InstallError):
                install.validate_form({**FORM, 'server': address})
        self.assertEqual(install.server_address('https://board.example.ts.net/'), 'https://board.example.ts.net')
        self.assertEqual(install.server_address('http://10.0.0.5:8090'), 'http://10.0.0.5:8090')

    def test_names_already_registered_or_waiting_are_refused(self):
        with self.assertRaisesRegex(install.InstallError, 'already registered'): install.validate_form(FORM, registered={'build-pc'})
        with self.assertRaisesRegex(install.InstallError, 'waiting'): install.validate_form(FORM, pending={'build-pc'})

    def test_creating_twice_for_one_name_is_refused_until_cancelled(self):
        record = install.create(FORM)
        with self.assertRaisesRegex(install.InstallError, 'waiting'): install.create(FORM)
        install.cancel(record['code'])
        self.assertEqual(install.create(FORM)['state'], 'pending')


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

    def test_commands_are_one_line_per_shell_and_say_no_elevation(self):
        record = install.create(FORM)
        commands = install.commands(record)
        for shell in ('powershell', 'cmd', 'bash'):
            self.assertNotIn('\n', commands[shell])
            self.assertIn(f'/install/{record["code"]}/', commands[shell])
        self.assertTrue(commands['powershell'].startswith('irm ') and commands['powershell'].endswith('| iex'))
        self.assertTrue(commands['cmd'].startswith('powershell -NoProfile'))
        self.assertTrue(commands['bash'].startswith('curl -fsSL ') and commands['bash'].endswith('| bash'))
        self.assertIn('normal user', commands['elevation'])
        self.assertNotIn(GOOD_TOKEN, json.dumps(commands))

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


class RegistrationTests(Isolated):
    def test_registering_stores_the_token_privately_and_adds_a_relayed_worker(self):
        from flowkit.fleet_registry import _read
        from flowkit.relay import worker_url
        record = install.create(FORM)
        env = install.register(record, GOOD_TOKEN, 'HOST')
        self.assertIn(f'{env}={GOOD_TOKEN}', (self.root/'workers.env').read_text())
        if os.name != 'nt': self.assertEqual((self.root/'workers.env').stat().st_mode & 0o777, 0o600)
        worker = next(w for w in _read() if w['id'] == 'build-pc')
        self.assertEqual((worker['url'], worker['pools'], worker['tags'], worker['engines'], worker['slots'], worker['cpu_limit'], worker['ram_limit'], worker['token_env']),
                         (worker_url('build-pc'), ['build'], ['gpu'], ['hatchet', 'prefect'], 2, 80, 85, env))
        self.assertNotIn(GOOD_TOKEN, (self.root/'workers.yaml').read_text())  # tokens never go into the registry

    def test_invalid_tokens_are_refused(self):
        record = install.create(FORM)
        for token in (None, '', 'short', 'x' * 201, GOOD_TOKEN + ' ', GOOD_TOKEN + '\n', 'a' * 40 + '=', 7):
            with self.subTest(token=token), self.assertRaises(install.InstallError): install.register(record, token)

    def test_a_second_registration_of_the_same_name_is_refused_and_keeps_the_first(self):
        record = install.create(FORM)
        install.register(record, GOOD_TOKEN)
        with self.assertRaisesRegex(install.InstallError, 'already registered'): install.register(record, 'z' * 40)

    def test_env_file_updates_replace_rather_than_duplicate(self):
        path = self.root/'workers.env'
        install.set_env_value('A_B', 'one', path); install.set_env_value('C', 'two', path); install.set_env_value('A_B', 'three', path)
        self.assertEqual(sorted(path.read_text().split()), ['A_B=three', 'C=two'])


class BundleTests(unittest.TestCase):
    def names(self):
        with tarfile.open(fileobj=io.BytesIO(install.build_bundle()), mode='r:gz') as archive: return archive.getnames()

    def test_the_bundle_has_what_a_pc_needs_and_nothing_else(self):
        names = self.names()
        for needed in ('agents/flow_execution_worker.py', 'agents/worker_runner.py', 'agents/relay_connector.py', 'agents/requirements-worker.txt',
                       'agents/package.json', 'agents/package-lock.json', 'agents/flowkit/worker_service.py', 'agents/flowkit/connector.py'):
            self.assertIn(needed, names)
        for name in names:
            self.assertFalse(re.search(r'(^|/)(\.venv|node_modules|runtime|tests|__pycache__|\.env|workers\.local)', name), name)
            self.assertFalse(name.endswith(('.pyc', '.env', '.pem', '.key')), name)
            self.assertFalse(name.startswith('/') or '..' in name.split('/'), name)
        self.assertNotIn('agents/flow_host.py', names)  # the coordinator-only code stays home
        self.assertNotIn('agents/flowkit/install.py', [n for n in names if n.endswith('never')])

    def test_the_bundle_runs_without_the_repository(self):
        """Extracted alone, the worker's modules import with only the packages in requirements-worker.txt."""
        with tempfile.TemporaryDirectory() as directory, tarfile.open(fileobj=io.BytesIO(install.build_bundle()), mode='r:gz') as archive:
            archive.extractall(directory, filter='data')
            code = 'import worker_runner, relay_connector, flow_execution_worker; print("ok")'
            result = subprocess.run([sys.executable, '-c', code], cwd=Path(directory)/'agents', capture_output=True, text=True, env={**os.environ, 'PYTHONPATH': ''})
            self.assertEqual((result.returncode, result.stdout.strip()), (0, 'ok'), result.stderr)

    def test_the_bundle_is_cached_until_a_file_changes(self):
        first = install.build_bundle()
        self.assertIs(install.build_bundle(), first)

    def test_worker_requirements_match_the_pinned_requirements(self):
        def pins(path):
            return {l.split('==')[0].lower(): l.split('==')[1] for l in (ROOT/'agents'/path).read_text().splitlines() if '==' in l and not l.startswith('#')}
        worker, full = pins('requirements-worker.txt'), pins('requirements.txt')
        self.assertTrue(worker)
        for name, version in worker.items(): self.assertEqual(full.get(name), version, name)


class ScriptTests(Isolated):
    def record(self, **changes):
        return {**install.create({**FORM, **changes}), }

    def test_values_are_filled_in_and_quoted(self):
        record = self.record()
        sh, ps = install.render('sh', record), install.render('ps1', record)
        for text in (sh, ps):
            self.assertNotIn('@@', text)
            self.assertIn(record['code'], text)
            self.assertIn("'build-pc'", text)
            self.assertIn("'http://192.168.1.10:8090'", text)
        self.assertNotIn('\r', sh)
        self.assertEqual(ps.count('\r\n'), ps.count('\n'))

    def test_quoting_cannot_be_broken_out_of(self):
        self.assertEqual(install.sh_quote("a'b"), "'a'\\''b'")
        self.assertEqual(install.ps_quote("a'b"), "'a''b'")
        record = {**self.record(), 'server': "http://h:1'; touch /tmp/pwned; '"}
        self.assertNotIn("'; touch /tmp/pwned; '", install.render('sh', record).replace("'\\''", ''))

    def test_the_bash_script_parses_after_rendering(self):
        bash = shutil.which('bash')
        if not bash: self.skipTest('bash is not installed')
        path = self.root/'install.sh'
        path.write_text(install.render('sh', self.record()), newline='\n')
        result = subprocess.run([bash, '-n', str(path)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_the_powershell_script_parses_after_rendering(self):
        powershell = shutil.which('powershell') or shutil.which('pwsh')
        if not powershell: self.skipTest('PowerShell is not installed')
        path = self.root/'install.ps1'
        path.write_text(install.render('ps1', self.record()), encoding='utf-8', newline='')
        command = ("$e=$null; [void][System.Management.Automation.Language.Parser]::ParseFile('%s',[ref]$null,[ref]$e); "
                   "if($e){$e|%%{$_.Message+' line '+$_.Extent.StartLineNumber}; exit 1}") % str(path).replace("'", "''")
        result = subprocess.run([powershell, '-NoProfile', '-Command', command], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_the_scripts_never_ask_for_elevation_and_never_expose_the_token_in_arguments(self):
        record = self.record()
        for kind in ('sh', 'ps1'):
            text = install.render(kind, record)
            self.assertNotRegex(text, r'(?im)^\s*sudo\s')  # sudo appears only in hints the user may follow, never as a command the script runs
            self.assertNotRegex(text, r'(?i)runas|-Verb\s+RunAs|New-NetFirewallRule|netsh|firewall-cmd|ufw ')
            self.assertNotRegex(text, r'(?i)--token|FLOW_WORKER_TOKEN=')


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

    async def test_scripts_and_the_bundle_are_served_for_a_pending_code(self):
        record = self.make()
        for kind, marker in (('sh', '#!/usr/bin/env bash'), ('ps1', 'Installs this PC')):
            async with self.http.get(f'{self.url}/install/{record["code"]}/install.{kind}') as response:
                self.assertEqual(response.status, 200)
                text = await response.text()
                self.assertIn(marker, text); self.assertIn(record['code'], text)
        async with self.http.get(f'{self.url}/install/{record["code"]}/bundle.tgz') as response:
            self.assertEqual(response.status, 200)
            self.assertTrue((await response.read()).startswith(b'\x1f\x8b'))

    async def test_unknown_wrong_expired_cancelled_and_used_codes_all_look_the_same(self):
        record = self.make()
        install.cancel(record['code'])
        expired = self.make(id='old')
        with patch.object(install, 'now', lambda: expired['expires_at'] + 5):
            async with self.http.get(f'{self.url}/install/{expired["code"]}/install.sh') as response: expired_body = (response.status, await response.text())
        results = []
        for code in ('A' * 43, record['code'], 'short'):
            async with self.http.get(f'{self.url}/install/{code}/install.sh') as response: results.append((response.status, await response.text()))
        async with self.http.get(f'{self.url}/install/../../etc/passwd') as response: self.assertEqual(response.status, 404)  # never reaches a handler
        self.assertEqual({r[0] for r in results} | {expired_body[0]}, {404})
        self.assertEqual({r[1] for r in results}, {expired_body[1]})

    async def test_wrong_codes_from_one_source_are_limited(self):
        statuses = []
        for i in range(14):
            async with self.http.get(f'{self.url}/install/{"B" * 43}/install.sh') as response: statuses.append(response.status)
        self.assertEqual(statuses[:10], [404] * 10)
        self.assertEqual(set(statuses[10:]), {429})

    async def test_progress_is_recorded_and_failures_show_their_reason(self):
        record = self.make()
        async with self.http.post(f'{self.url}/install/{record["code"]}/progress', json={'step': 'python', 'state': 'running'}) as response: self.assertEqual(response.status, 204)
        self.assertEqual(install.view(install.read_record(record['code']))['step'], 'python')
        async with self.http.post(f'{self.url}/install/{record["code"]}/progress', json={'step': 'npm', 'state': 'failed', 'message': 'npm could not reach the registry'}) as response: self.assertEqual(response.status, 204)
        shown = install.view(install.read_record(record['code']))
        self.assertEqual((shown['step'], shown['error']), ('npm', 'npm could not reach the registry'))
        async with self.http.post(f'{self.url}/install/{record["code"]}/progress', json={'step': 'bogus'}) as response: self.assertEqual(response.status, 400)

    async def test_completion_registers_the_pc_once_and_burns_the_link(self):
        from flowkit.fleet_registry import _read
        record = self.make()
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN, 'hostname': 'PC-1'}) as response:
            self.assertEqual((response.status, await response.json()), (200, {'ok': True, 'id': 'build-pc'}))
        self.assertTrue(any(w['id'] == 'build-pc' for w in _read()))
        shown = install.view(install.read_record(record['code']))
        self.assertEqual((shown['state'], shown['hostname']), ('registered', 'PC-1'))
        for path in ('install.sh', 'install.ps1', 'bundle.tgz'):  # the link is dead for everything but the status
            async with self.http.get(f'{self.url}/install/{record["code"]}/{path}') as response: self.assertEqual(response.status, 404)
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': 'q' * 40}) as response: self.assertEqual(response.status, 404)
        self.assertEqual(len([w for w in _read() if w['id'] == 'build-pc']), 1)

    async def test_a_bad_token_is_refused_and_the_link_stays_usable(self):
        record = self.make()
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': 'weak'}) as response: self.assertEqual(response.status, 409)
        self.assertEqual(install.view(install.read_record(record['code']))['state'], 'pending')
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN}) as response: self.assertEqual(response.status, 200)

    async def test_state_reports_connection_through_the_relay(self):
        record = self.make()
        async with self.http.get(f'{self.url}/install/{record["code"]}/state') as response: self.assertFalse((await response.json())['connected'])
        async with self.http.post(f'{self.url}/install/{record["code"]}/complete', json={'token': GOOD_TOKEN}) as response: self.assertEqual(response.status, 200)
        relay = self.server.app['relay']
        relay.peer('build-pc').last_poll = time.monotonic()
        async with self.http.get(f'{self.url}/install/{record["code"]}/state') as response:
            body = await response.json()
        self.assertEqual((body['state'], body['connected']), ('registered', True))
        self.assertNotIn(GOOD_TOKEN, json.dumps(body))


if __name__ == '__main__':
    unittest.main()
