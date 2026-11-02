import ctypes
import os
import subprocess
import sys
import unittest
from ctypes import wintypes
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentdriver.process_tree import ProcessTree
from agentdriver import process_tree


@unittest.skipUnless(os.name == "nt", "Windows job object ownership")
class ProcessTreeTests(unittest.TestCase):
    def test_close_terminates_child_after_adapter_has_exited(self):
        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = [wintypes.HANDLE, wintypes.DWORD]
        kernel.WaitForSingleObject.restype = wintypes.DWORD
        kernel.TerminateProcess.argtypes = [wintypes.HANDLE, wintypes.UINT]
        kernel.TerminateProcess.restype = wintypes.BOOL
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.CloseHandle.restype = wintypes.BOOL
        # Assignment must precede child creation; stdin provides that handshake.
        adapter = subprocess.Popen([
            sys.executable, "-c",
            "import subprocess,sys; sys.stdin.readline(); "
            "child=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'], "
            "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
            "print(child.pid,flush=True)",
        ], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        owner = None
        child = None
        try:
            owner = ProcessTree(adapter.pid)
            adapter.stdin.write("spawn\n")
            adapter.stdin.flush()
            child_pid = int(adapter.stdout.readline())
            child = kernel.OpenProcess(0x00100000 | 0x0001, False, child_pid)
            self.assertTrue(child, "Could not observe the owned child process")
            adapter.wait(timeout=5)
            self.assertEqual(adapter.returncode, 0)
            self.assertEqual(kernel.WaitForSingleObject(child, 0), 258, "Child should still be running")

            owner.close()
            owner.close()  # Cleanup is safe on repeated calls.

            self.assertEqual(kernel.WaitForSingleObject(child, 5000), 0,
                             "Closing ownership must kill descendants after adapter exit")
        finally:
            if owner:
                owner.close()
            if adapter.poll() is None:
                adapter.kill()
            adapter.communicate(timeout=5)
            if child:
                if kernel.WaitForSingleObject(child, 0) == 258:
                    kernel.TerminateProcess(child, 1)
                    kernel.WaitForSingleObject(child, 5000)
                kernel.CloseHandle(child)

    def test_invalid_pid_fails_explicitly(self):
        with self.assertRaisesRegex(OSError, "own ACP process 0"):
            ProcessTree(0)

    def test_assignment_failure_does_not_allow_unowned_session(self):
        adapter = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                   creationflags=subprocess.CREATE_NO_WINDOW)
        try:
            # An incompatible containing job or OS access restriction can reject
            # assignment. Inject only that OS failure; create real job/process handles.
            def denied(job, process):
                ctypes.set_last_error(5)
                return 0

            with patch.object(process_tree._kernel, "AssignProcessToJobObject", denied):
                with self.assertRaisesRegex(OSError, f"own ACP process {adapter.pid}.*assign job") as caught:
                    ProcessTree(adapter.pid)
            self.assertEqual(caught.exception.winerror, 5)
        finally:
            adapter.kill()
            adapter.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
