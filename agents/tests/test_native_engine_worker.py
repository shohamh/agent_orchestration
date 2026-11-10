"""Offline contracts for the per-PC native engine dispatcher."""
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

AGENTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AGENTS))
import native_engine_worker as native


class FleetMetadataProjectionTests(unittest.TestCase):
    def test_hatchet_labels_and_prefect_tags_track_fleet_sets_and_clear_removed_values(self):
        metadata = {'revision': 7, 'pools': ['gpu', 'lab'], 'tags': ['linux', 'cuda']}
        self.assertEqual(native.hatchet_fleet_labels(metadata),
                         {'fleet_pools': 'gpu,lab', 'fleet_tags': 'linux,cuda'})
        self.assertEqual(native.prefect_fleet_tags(metadata),
                         ['fleet-pool:gpu', 'fleet-pool:lab', 'fleet-tag:cuda', 'fleet-tag:linux'])
        cleared = {'revision': 8, 'pools': ['default'], 'tags': []}
        self.assertTrue(native.metadata_changed(metadata, cleared))
        self.assertEqual(native.hatchet_fleet_labels(cleared),
                         {'fleet_pools': 'default', 'fleet_tags': ''})
        self.assertEqual(native.prefect_fleet_tags(cleared), ['fleet-pool:default'])

    def test_hatchet_projection_calls_live_worker_label_upsert(self):
        from hatchet_sdk.types.labels import WorkerLabel
        dispatcher = Mock()
        client = SimpleNamespace(dispatcher=dispatcher)
        metadata = {'revision': 1, 'pools': ['build'], 'tags': ['linux']}
        native.apply_hatchet_fleet_labels(client, 'hatchet-worker-id', metadata)
        worker_id, labels = dispatcher.upsert_worker_labels.call_args.args
        self.assertEqual(worker_id, 'hatchet-worker-id')
        self.assertTrue(all(isinstance(label, WorkerLabel) for label in labels))
        self.assertEqual({label.key: label.value for label in labels},
                         {'fleet_pools': 'build', 'fleet_tags': 'linux'})

    def test_prefect_projection_updates_only_the_stable_deployment_tags(self):
        flow = SimpleNamespace(deploy=Mock())
        metadata = {'revision': 1, 'pools': ['build'], 'tags': ['linux']}
        native.prefect_deploy(flow, 'agent-pc', 'pc-queue', 'pc-deployment', metadata)
        kwargs = flow.deploy.call_args.kwargs
        self.assertEqual((kwargs['name'], kwargs['work_pool_name'], kwargs['work_queue_name']),
                         ('pc-deployment', 'agent-pc', 'pc-queue'))
        self.assertEqual(kwargs['tags'], ['fleet-pool:build', 'fleet-tag:linux'])


class NativeDispatchPollerTests(unittest.TestCase):

    def test_unacknowledged_engine_submission_is_reused_on_retry(self):
        submit = Mock(return_value='engine-run-1')
        poller = native._NativeDispatchPoller('hatchet', 'private-service-token', submit, {})
        with patch.object(poller, '_acknowledge', side_effect=[
            native._NativeDispatchFailure('temporary service failure', status=503), True,
        ]) as acknowledge:
            with self.assertRaises(native._NativeDispatchFailure):
                poller._dispatch_job('job-1', 'nonce-1')
            poller._dispatch_job('job-1', 'nonce-1')
        self.assertEqual(submit.call_count, 1, 'retry must not create a second engine run')
        self.assertEqual(acknowledge.call_count, 2)
        self.assertEqual(acknowledge.call_args.args, ('job-1', 'nonce-1', 'engine-run-1'))

    def test_local_dispatch_poll_authenticates_without_exposing_token_in_url_or_status(self):
        token = 'private-service-token'
        response = MagicMock()
        response.__enter__.return_value = response
        response.read.return_value = b'{"jobs":[{"id":"job-1","nonce":"nonce-1"}]}'
        with patch.object(native.urllib.request, 'urlopen', return_value=response) as urlopen:
            jobs = native._native_dispatch_jobs('http://127.0.0.1:8096/native-dispatch?engine=hatchet', token)
        request = urlopen.call_args.args[0]
        self.assertEqual(jobs, [('job-1', 'nonce-1')])
        self.assertTrue(request.get_header('Authorization') == f'Bearer {token}',
                        'native dispatch polls must authenticate to the local service')
        self.assertNotIn(token, request.full_url)
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {'AGENT_WORKER_HOME': directory}):
            native.write_status('hatchet', 'ready', worker_id='hatchet-worker-1', token=token)
            status = json.loads((Path(directory) / 'runtime/native-engines/hatchet.json').read_text())
        self.assertNotIn('token', status)
        self.assertNotIn(token, json.dumps(status))

    def test_native_runtime_packages_are_importable_in_child_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory)
            packages = runtime / 'packages'
            prefect = packages / 'prefect'
            (runtime / 'python').mkdir()
            (runtime / 'agents').mkdir()
            prefect.mkdir(parents=True)
            (prefect / '__init__.py').write_text("RUNTIME_SENTINEL = 'bundled'\n", encoding='utf-8')
            package_path = str(packages)
            agent_path = str(runtime / 'agents')
            with patch.dict(os.environ, {'AGENT_WORKER_NATIVE_RUNTIME': directory, 'PYTHONPATH': ''}):
                try:
                    native.runtime_paths()
                    child = subprocess.run(
                        [sys.executable, '-c', 'import prefect; print(prefect.RUNTIME_SENTINEL)'],
                        env=os.environ.copy(), capture_output=True, text=True, check=False,
                    )
                finally:
                    for path in (package_path, agent_path):
                        while path in sys.path:
                            sys.path.remove(path)
            self.assertEqual(child.returncode, 0, child.stderr)
            self.assertEqual(child.stdout.strip(), 'bundled')


    def test_installed_entrypoint_selects_the_configured_engine(self):
        with patch.dict(os.environ, {'AGENT_WORKER_NATIVE_ENGINE': 'hatchet'}), \
                patch.object(native, '_hatchet_main') as hatchet, patch.object(native, '_prefect_main') as prefect:
            native.main()
        hatchet.assert_called_once_with()
        prefect.assert_not_called()
        with patch.dict(os.environ, {'AGENT_WORKER_NATIVE_ENGINE': 'prefect'}), \
                patch.object(native, '_hatchet_main') as hatchet, patch.object(native, '_prefect_main') as prefect:
            native.main()
        prefect.assert_called_once_with()
        hatchet.assert_not_called()

    def test_installed_entrypoint_rejects_an_unknown_engine(self):
        with patch.dict(os.environ, {'AGENT_WORKER_NATIVE_ENGINE': 'other'}):
            with self.assertRaisesRegex(RuntimeError, 'AGENT_WORKER_NATIVE_ENGINE'):
                native.main()

    def test_engine_error_redaction_is_value_safe(self):
        token = 'private-hatchet-credential'
        message = native._sanitize(f'authentication failed: {token}', token)
        self.assertTrue(message == 'authentication failed: [redacted]',
                        'engine errors must not retain the dedicated credential')


if __name__ == '__main__':
    unittest.main()
