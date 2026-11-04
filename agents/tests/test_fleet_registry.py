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
