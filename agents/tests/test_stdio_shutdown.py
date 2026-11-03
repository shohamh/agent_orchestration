import subprocess
import sys
import unittest
from pathlib import Path


class StdioShutdownTests(unittest.TestCase):
    def test_closed_stdout_preserves_exit_status(self):
        agents = Path(__file__).resolve().parents[1]
        for module in ('flowkit.worker_stdio', 'flowkit.stdio'):
            for fail in (False, True):
                with self.subTest(module=module, fail=fail):
                    script = '''
import runpy, sys
from unittest.mock import patch
def finish(coro):
    coro.close()
    sys.stdout.close()
    if FAIL:
        raise RuntimeError("expected failure")
with patch("asyncio.run", side_effect=finish):
    runpy.run_module(MODULE, run_name="__main__")
'''.replace('FAIL', repr(fail)).replace('MODULE', repr(module))
                    result = subprocess.run([sys.executable, '-c', script], cwd=agents, capture_output=True, timeout=20)
                    self.assertEqual(result.returncode, int(fail), result.stderr.decode(errors='replace'))
                    self.assertNotIn(b'I/O operation on closed file', result.stderr)


if __name__ == '__main__':
    unittest.main()
