import json
import os
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

    @unittest.skipIf(os.name == 'nt', 'POSIX stdout is made nonblocking by ACP')
    def test_jsonl_backpressure_preserves_every_frame_before_exit(self):
        import select
        agents = Path(__file__).resolve().parents[1]
        script = '''
import asyncio, os, sys
from agentdriver.stdio_io import event_streams
async def main():
    reader, sink = await event_streams()
    async def emit():
        for number in range(4000):
            await sink({'number': number, 'text': 'Unicode-שלום-' * 8})
        await sink({'done': True})
    pending = asyncio.create_task(emit())
    await asyncio.sleep(0)
    print('ready', file=sys.stderr, flush=True)
    await pending
asyncio.run(main())
os._exit(0)
'''
        process = subprocess.Popen([sys.executable, '-c', script], cwd=agents,
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            # Do not consume stdout until the emitter has run into the pipe's backpressure.
            self.assertTrue(select.select([process.stderr], [], [], 10)[0])
            self.assertEqual(process.stderr.readline(), b'ready\n')
            output, errors = process.communicate(timeout=20)
            self.assertEqual(process.returncode, 0, errors.decode(errors='replace'))
            events = [json.loads(line) for line in output.splitlines()]
            self.assertEqual(events, [{'number': i, 'text': 'Unicode-שלום-' * 8} for i in range(4000)] + [{'done': True}])
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()


if __name__ == '__main__':
    unittest.main()
