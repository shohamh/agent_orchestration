import os
from pathlib import Path
import subprocess
import sys
import tempfile
import signal
import time
import unittest

sys.path.insert(0, str(Path(__file__).parent))
import auto_reload


class AutoReloadTests(unittest.TestCase):
    def test_backend_reload_is_disabled_by_default(self):
        self.assertFalse(auto_reload.enabled({}))
        self.assertFalse(auto_reload.enabled({"BOARD_AUTO_RELOAD": "0"}))
        self.assertTrue(auto_reload.enabled({"BOARD_AUTO_RELOAD": "1"}))

    def test_source_change_restarts_child_and_shutdown_stops_it(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            events = root / "events.txt"
            worker = root / "worker.py"
            worker.write_text(
                "import signal, sys, time\n"
                "events = sys.argv[1]\n"
                "def stop(*_):\n"
                "    with open(events, 'a') as f: f.write('stop\\n')\n"
                "    raise SystemExit(0)\n"
                "signal.signal(signal.SIGTERM, stop)\n"
                "if hasattr(signal, 'SIGBREAK'): signal.signal(signal.SIGBREAK, stop)\n"
                "with open(events, 'a') as f: f.write('start\\n')\n"
                "while True: time.sleep(0.02)\n"
            )
            env = os.environ.copy()
            env["BOARD_AUTO_RELOAD"] = "1"
            supervisor = Path(__file__).with_name("auto_reload.py")
            options = {}
            if os.name == "nt":
                options["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
            process = subprocess.Popen(
                [sys.executable, str(supervisor), "--watch-root", str(root),
                 "--poll-interval", "0.05", "--", sys.executable, str(worker), str(events)],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                **options,
            )
            try:
                self._wait_for_starts(events, 1)
                worker.write_text(worker.read_text() + "# changed source\n")
                self._wait_for_starts(events, 2)
                if os.name == "nt":
                    process.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    process.terminate()
                process.wait(timeout=5)
                lines = events.read_text().splitlines()
                self.assertEqual(lines.count("start"), 2)
                if os.name != "nt":
                    self.assertEqual(lines.count("stop"), 2)
            finally:
                if process.poll() is None:
                    if os.name == "nt":
                        process.send_signal(signal.CTRL_BREAK_EVENT)
                    else:
                        process.terminate()
                    process.wait(timeout=5)

    def _wait_for_starts(self, events: Path, count: int) -> None:
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            if events.exists() and events.read_text().splitlines().count("start") >= count:
                return
            time.sleep(0.02)
        self.fail(f"expected {count} worker starts; saw {events.read_text() if events.exists() else 'none'}")


if __name__ == "__main__":
    unittest.main()
