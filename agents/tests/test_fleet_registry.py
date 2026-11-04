import os, sys, tempfile, unittest
from pathlib import Path
from unittest.mock import patch
import yaml
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

class FleetTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'custom.yaml'
        self.env = patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(self.path), 'SECRET_TOKEN': 'top-secret'})
        self.env.start(); self.addCleanup(self.env.stop)

    def test_implicit_local_and_migration_is_idempotent(self):
        from flowkit.fleet_registry import load_fleet, migrate_legacy_registry
        self.assertEqual(load_fleet()[0]['pools'], ['default'])
        self.path.write_text('workers:\n- id: pc\n  pool: build\n  url: http://localhost:8096\n  token_env: SECRET_TOKEN\n', encoding='utf-8')
        migrate_legacy_registry(); first = self.path.read_bytes(); migrate_legacy_registry()
        self.assertEqual(first, self.path.read_bytes())
        saved = yaml.safe_load(first); self.assertEqual(saved['workers'][0]['pools'], ['build'])
        self.assertEqual(saved['workers'][0]['token_env'], 'SECRET_TOKEN')
        self.assertNotIn('top-secret', str(load_fleet()))

    def test_duplicate_ids_rejected(self):
        from flowkit.fleet_registry import load_fleet
        self.path.write_text('workers:\n- {id: x}\n- {id: x}\n', encoding='utf-8')
        with self.assertRaises(ValueError): load_fleet()

    def test_interrupted_write_preserves_original(self):
        from flowkit.fleet_registry import migrate_legacy_registry
        self.path.write_text('workers:\n- {id: x, pool: old, token_env: SECRET_TOKEN}\n', encoding='utf-8')
        original = self.path.read_bytes()
        with patch('flowkit.fleet_registry.os.replace', side_effect=OSError('interrupted')):
            with self.assertRaises(OSError): migrate_legacy_registry()
        self.assertEqual(self.path.read_bytes(), original)

    def test_save_preserves_connection_and_defaults(self):
        from flowkit.fleet_registry import save_worker
        self.path.write_text('workers:\n- {id: x, pool: old, url: "http://localhost:8096", token_env: SECRET_TOKEN}\n', encoding='utf-8')
        result = save_worker({'id':'x', 'pools':['old','new'], 'tags':['gpu']})
        self.assertEqual((result['cpu_limit'], result['ram_limit']), (90,90))
        self.assertNotIn('url', result); self.assertNotIn('token_env', result)
        saved=yaml.safe_load(self.path.read_text())['workers'][0]
        self.assertEqual(saved['token_env'], 'SECRET_TOKEN'); self.assertEqual(saved['pools'], ['old','new'])

class FleetRouteTests(unittest.TestCase):
    def setUp(self):
        import threading, flow_host
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'workers.yaml'
        self.path.write_text('workers:\n- {id: pc.example, pool: build, url: "http://localhost:8096", token_env: SECRET_TOKEN}\n', encoding='utf-8')
        self.env = patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(self.path), 'FLOW_HOST_TOKEN':'host-secret', 'SECRET_TOKEN':'top-secret'})
        self.env.start(); self.addCleanup(self.env.stop)
        self.server = flow_host.serve(None, '127.0.0.1', 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close); self.addCleanup(self.server.shutdown)

    def call(self, method, path, body=None, auth=True):
        import json, urllib.request, urllib.error
        headers={'Content-Type':'application/json'}
        if auth: headers['Authorization']='Bearer host-secret'
        request=urllib.request.Request('http://127.0.0.1:%s%s' % (self.server.server_port, path), method=method, headers=headers, data=None if body is None else json.dumps(body).encode())
        try:
            with urllib.request.urlopen(request) as response: return response.status, json.load(response)
        except urllib.error.HTTPError as error: return error.code, json.load(error)

    def test_authenticated_crud_migration_sanitization_and_errors(self):
        self.assertEqual(self.call('GET','/workers',auth=False)[0],401)
        self.assertEqual(self.call('POST','/workers/migrate',{})[0],200)
        status, body=self.call('GET','/workers'); self.assertEqual(status,200)
        self.assertNotIn('top-secret',str(body)); self.assertNotIn('token_env',str(body)); self.assertNotIn('localhost',str(body))
        self.assertEqual(self.call('PUT','/workers/pc.example',{'tags':['gpu']})[0],200)
        self.assertEqual(self.call('POST','/workers',{'id':'offline','pools':['build']})[0],200)
        self.assertEqual(self.call('PUT','/workers/pc.example',{'id':'mismatch'})[0],400)
        self.assertEqual(self.call('DELETE','/workers/pc.example')[0],200)
        self.assertEqual(self.call('DELETE','/workers/pc.example')[0],400)
        before=self.path.read_bytes()
        self.assertEqual(self.call('POST','/workers',{'id':'bad','url':'https://remote.example'})[0],400)
        self.assertEqual(before,self.path.read_bytes())

class FleetConcurrencyTests(unittest.TestCase):
    def test_multiple_processes_preserve_all_updates(self):
        import subprocess
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'registry.yaml'; env=os.environ.copy(); env['FLOW_WORKERS_FILE']=str(path)
            script="import sys; sys.path.insert(0, 'agents'); from flowkit.fleet_registry import save_worker; save_worker({'id':sys.argv[1]})"
            processes=[subprocess.Popen([sys.executable,'-c',script,'worker'+str(i)],env=env) for i in range(12)]
            self.assertEqual([p.wait(timeout=30) for p in processes],[0]*12)
            self.assertEqual(len(yaml.safe_load(path.read_text())['workers']),13)

    def test_migrate_then_restore_protected_backup(self):
        from flowkit.fleet_registry import migrate_legacy_registry
        import shutil
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'registry.yaml'; backup=Path(tmp)/'backup.yaml'
            path.write_text('workers:\n- {id: pc, pool: build, token_env: SECRET_TOKEN}\n')
            original=path.read_bytes(); shutil.copy2(path,backup)
            with patch.dict(os.environ,{'FLOW_WORKERS_FILE':str(path)}): migrate_legacy_registry()
            self.assertNotEqual(original,path.read_bytes())
            os.replace(backup,path)
            self.assertEqual(original,path.read_bytes())
            self.assertEqual(yaml.safe_load(path.read_text())['workers'][0]['token_env'],'SECRET_TOKEN')

class SnapshotTests(unittest.IsolatedAsyncioTestCase):
    async def test_choose_uses_same_snapshot_as_health(self):
        from flowkit import workers
        snapshot=[{'id':'pc','pools':['build'],'tags':[], 'url':'http://localhost', '_headers':{}}]
        async def health(items):
            self.assertIs(items, snapshot)
            return [{'id':'pc','online':True,'active':0,'slots':1}]
        with patch.object(workers,'registry',side_effect=[snapshot, AssertionError('read twice')]), patch.object(workers,'catalog',side_effect=health):
            self.assertEqual((await workers.choose({'pool':'build'}))['id'],'pc')

class ReadOnlyAndConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def test_missing_remote_credentials_do_not_hide_healthy_worker(self):
        from flowkit import workers
        from aiohttp.test_utils import TestServer
        from agents.tests.worker_fixture import make_app
        with tempfile.TemporaryDirectory() as tmp:
            app=make_app(worker_id='healthy',root=Path(tmp)/'jobs')
            async with TestServer(app) as service:
                path=Path(tmp)/'workers.yaml'
                path.write_text(yaml.safe_dump({'workers':[{'id':'healthy','url':str(service.make_url(''))}, {'id':'remote','url':'https://remote.example','token_env':'UNSET_FLEET_TOKEN'}]}))
                with patch.dict(os.environ,{'FLOW_WORKERS_FILE':str(path)},clear=False):
                    os.environ.pop('UNSET_FLEET_TOKEN', None)
                    result=await workers.catalog(); chosen=await workers.choose({'id':'healthy'})
                self.assertEqual(chosen['id'],'healthy')
                remote=next(w for w in result if w['id']=='remote')
                self.assertFalse(remote['online']); self.assertIn('credential',remote['reason'].lower())
                self.assertNotIn('remote.example',str(remote)); self.assertNotIn('UNSET_FLEET_TOKEN',str(remote))

    async def test_catalog_does_not_open_lock_for_read(self):
        from flowkit import workers, fleet_registry
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'workers.yaml'; path.write_text('workers:\n- {id: offline}\n')
            with patch.dict(os.environ,{'FLOW_WORKERS_FILE':str(path)}), patch.object(fleet_registry,'_locked',side_effect=PermissionError('read only')):
                self.assertEqual((await workers.catalog())[0]['id'],'offline')
                self.assertEqual(fleet_registry.load_fleet()[0]['id'],'offline')
