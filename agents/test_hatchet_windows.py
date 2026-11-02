"""Exercise the actual SDK startup code paths which fail on Windows."""
import asyncio
import contextlib
import pickle
import signal
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
try:
    import hatchet_windows as compatibility
except ImportError:
    compatibility = None


@unittest.skipUnless(sys.platform == 'win32', 'Windows SDK compatibility')
class WindowsStartupTests(unittest.TestCase):
    def test_worker_registers_supported_signals_and_shutdown_preference(self):
        from hatchet_sdk.worker.worker import Worker
        if compatibility:
            compatibility.install()
        worker = Worker.__new__(Worker)
        worker._handle_exit_signal = lambda *_: None
        worker._handle_force_quit_signal = lambda *_: None
        for force in (False, True):
            worker._config = SimpleNamespace(force_shutdown_on_shutdown_signal=force)
            with patch('signal.signal') as register:
                worker._setup_signal_handlers()
            self.assertEqual([call.args[0] for call in register.call_args_list], [signal.SIGTERM, signal.SIGINT])
            expected = worker._handle_force_quit_signal if force else worker._handle_exit_signal
            self.assertTrue(all(call.args[1] is expected for call in register.call_args_list))

    def test_actual_listener_initialization_ignores_only_parent_owned_signals(self):
        from hatchet_sdk.worker.action_listener_process import WorkerActionListenerProcess
        loop = asyncio.ProactorEventLoop()
        asyncio.set_event_loop(loop)
        scope = compatibility.listener_signal_compatibility() if compatibility else contextlib.nullcontext()
        try:
            with scope, patch('signal.signal') as register, patch('hatchet_sdk.worker.action_listener_process.Client'):
                WorkerActionListenerProcess('test', [], {'default': 1},
                    SimpleNamespace(healthcheck=SimpleNamespace(enabled=False)), None, None,
                    False, False, [], None, None)
                self.assertEqual([call.args[0] for call in register.call_args_list], [signal.SIGINT, signal.SIGTERM])
                self.assertTrue(all(call.args[1] == signal.SIG_IGN for call in register.call_args_list))
        finally:
            loop.close()
            asyncio.set_event_loop(None)

    def test_spawn_target_is_picklable_module_function(self):
        if not compatibility:
            self.fail('Windows spawn wrapper missing')
        self.assertIs(pickle.loads(pickle.dumps(compatibility.windows_listener_process)), compatibility.windows_listener_process)


if __name__ == '__main__':
    unittest.main()
