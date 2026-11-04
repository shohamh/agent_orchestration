from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import unittest
from unittest.mock import patch
from flowkit.capabilities import infer_command, probe_agent, probe_command

class CapabilitiesTest(unittest.TestCase):
    def test_inference(self):
        self.assertEqual(infer_command('python script.py'), 'python')
        self.assertIsNone(infer_command('python a.py | cat'))
    def test_python(self):
        self.assertEqual(probe_command('python')['status'], 'installed')
    def test_demo(self):
        self.assertEqual(probe_agent('demo')['status'], 'installed')
        self.assertEqual(probe_agent('demo')['auth_status'], 'unknown')
    def test_missing(self):
        with patch('shutil.which', return_value=None):
            self.assertEqual(probe_command('missing-executable')['status'], 'absent')

    def test_agent_resolution(self):
        with patch('agentdriver.acp_engine.agent_command', return_value=['missing-node', 'adapter.js']), patch('shutil.which', return_value=None):
            for name in ('codex', 'claude', 'opencode2', 'opencode', 'omp', 'pi'):
                self.assertEqual(probe_agent(name)['status'], 'absent')
        with patch('agentdriver.acp_engine.agent_command', side_effect=RuntimeError('missing adapter')):
            self.assertEqual(probe_agent('codex')['status'], 'absent')

    def test_round_trip_requirement_and_staleness(self):
        from flowkit.definition import parse
        from flowkit.capabilities import overdue
        flow = parse({'name':'sample', 'steps':{'run':{'kind':'program', 'run':'python a | cat', 'requires_command':'python'}}})
        self.assertEqual(flow.to_json()['steps']['run']['requires_command'], 'python')
        self.assertTrue(overdue({'refreshed_at':'2000-01-01T00:00:00+00:00'}))
        self.assertTrue(overdue({}))
