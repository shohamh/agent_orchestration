"""Offline contracts for the per-PC native engine dispatcher."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

AGENTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(AGENTS))
import native_engine_worker as native


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
