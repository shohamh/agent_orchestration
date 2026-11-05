import tempfile
import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit.activation import Store


class ImmutableAssetTests(unittest.TestCase):
    def test_asset_reference_cannot_be_attached_to_different_definition(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); drafts = root/'flows'; drafts.mkdir()
            store = Store(drafts=drafts, state=root/'state')
            rev = store.write_snapshot('example', 'name: example\nsteps:\n  run: {kind: program, run: "echo safe"}\n')
            ref = store.asset_reference('example', rev)
            wrong = {'name': 'example', 'steps': {'run': {'kind': 'program', 'run': 'echo different'}}}
            with self.assertRaisesRegex(ValueError, 'definition|revision'):
                store.load_bound_flow(ref, wrong)

    def test_asset_reference_rejects_rewritten_manifest_with_new_digest(self):
        import json
        from flowkit.activation import revision
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); drafts = root/'flows'; drafts.mkdir()
            (drafts/'task.py').write_text('old', encoding='utf-8')
            store = Store(drafts=drafts, state=root/'state')
            rev = store.write_snapshot('example', 'name: example\nsteps:\n  run: {kind: program, run: "python task.py"}\n')
            ref = store.asset_reference('example', rev)
            path = store.asset_manifest_path('example', rev)
            data = json.loads(path.read_text(encoding='utf-8'))
            data['files']['task.py']['data'] = 'bmV3'
            data['digest'] = revision(json.dumps(data['files'], sort_keys=True, separators=(',', ':'), ensure_ascii=False))
            path.write_text(json.dumps(data), encoding='utf-8')
            with self.assertRaisesRegex(ValueError, 'digest'):
                store.snapshot_assets(ref)
            flows, broken = store.load({'example': rev})
            self.assertNotIn('example', flows)
            self.assertIn('example', broken)

    def test_activation_refuses_credentials_inside_asset_tree(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); drafts = root/'flows'; drafts.mkdir()
            (drafts/'.netrc').write_text('machine service password secret', encoding='utf-8')
            store = Store(drafts=drafts, state=root/'state')
            with self.assertRaisesRegex(ValueError, 'credential|secret'):
                store.write_snapshot('example', 'name: example\nsteps:\n  run: {kind: program, run: "echo hi"}\n')
            self.assertFalse((root/'state'/'active'/'example').exists())

    def test_other_drafts_and_layouts_do_not_change_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp); drafts = root/'flows'; drafts.mkdir()
            (drafts/'task.py').write_text('print(1)', encoding='utf-8')
            (drafts/'other.yaml').write_text('name: other', encoding='utf-8')
            (drafts/'other.layout.json').write_text('{}', encoding='utf-8')
            cache = drafts/'__pycache__'; cache.mkdir(); (cache/'task.pyc').write_bytes(b'old')
            store = Store(drafts=drafts, state=root/'state')
            text = 'name: example\nsteps:\n  run: {kind: program, run: "python task.py"}\n'
            rev = store.write_snapshot('example', text)
            (drafts/'other.yaml').write_text('name: changed', encoding='utf-8')
            (drafts/'other.layout.json').write_text('{"x":1}', encoding='utf-8')
            (cache/'task.pyc').write_bytes(b'new')
            self.assertEqual(store.write_snapshot('example', text), rev)

    def test_each_flow_revision_loads_its_original_assets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            drafts = root / 'flows'; drafts.mkdir()
            script = drafts / 'task.py'; script.write_text('old', encoding='utf-8')
            store = Store(drafts=drafts, state=root/'state')
            old_rev = store.write_snapshot('example', 'name: example\nsteps:\n  run: {kind: program, run: "python task.py"}\n')
            script.write_text('new', encoding='utf-8')
            new_rev = store.write_snapshot('example', 'name: example\ndescription: updated\nsteps:\n  run: {kind: program, run: "python task.py"}\n')
            with self.assertRaisesRegex(ValueError, 'assets.*changed'):
                store.write_snapshot('example', 'name: example\nsteps:\n  run: {kind: program, run: "python task.py"}\n')
            old_flow = store.load({'example': old_rev})[0]['example']
            new_flow = store.load({'example': new_rev})[0]['example']
            import base64
            old = store.snapshot_assets(old_flow._asset_snapshot)
            new = store.snapshot_assets(new_flow._asset_snapshot)
            self.assertEqual(base64.b64decode(old['task.py']['data']), b'old')
            self.assertEqual(base64.b64decode(new['task.py']['data']), b'new')
            from flowkit.restart import definition_hash
            self.assertNotEqual(definition_hash(old_flow), definition_hash(new_flow))
