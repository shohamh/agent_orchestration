"""Windows ownership of an ACP adapter and children created after assignment.

Construct immediately after spawning, before initializing the adapter. An unnamed,
non-inheritable job handle remains owned by this driver after the adapter exits;
closing it kills every remaining process in the job. Assignment failures raise
instead of allowing an uncontained session to continue.

This cannot retroactively own children created before assignment. Atomic ownership
would require creating the adapter suspended or with a job in its startup attributes.
"""

import ctypes
import os
from ctypes import wintypes


class _BasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_longlong),
        ("PerJobUserTimeLimit", ctypes.c_longlong),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [(name, ctypes.c_ulonglong) for name in (
        "ReadOperationCount", "WriteOperationCount", "OtherOperationCount",
        "ReadTransferCount", "WriteTransferCount", "OtherTransferCount")]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


if os.name == "nt":
    _kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    _kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    _kernel.CreateJobObjectW.restype = wintypes.HANDLE
    _kernel.SetInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD]
    _kernel.SetInformationJobObject.restype = wintypes.BOOL
    _kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
    _kernel.OpenProcess.restype = wintypes.HANDLE
    _kernel.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    _kernel.AssignProcessToJobObject.restype = wintypes.BOOL
    _kernel.CloseHandle.argtypes = [wintypes.HANDLE]
    _kernel.CloseHandle.restype = wintypes.BOOL


def _error(message):
    code = ctypes.get_last_error()
    return OSError(None, f"{message}: {ctypes.FormatError(code).strip()}", None, code)


class ProcessTree:
    """Own one Windows process tree until an explicit, idempotent close()."""

    def __init__(self, pid: int):
        self._handle = None
        if os.name != "nt":
            raise NotImplementedError("ProcessTree requires Windows job objects")
        job = _kernel.CreateJobObjectW(None, None)
        if not job:
            raise _error(f"Cannot own ACP process {pid} (create job)")
        process = None
        try:
            limits = _ExtendedLimitInformation()
            limits.BasicLimitInformation.LimitFlags = 0x00002000  # KILL_ON_JOB_CLOSE
            if not _kernel.SetInformationJobObject(job, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                raise _error(f"Cannot own ACP process {pid} (configure job)")
            # AssignProcessToJobObject requires PROCESS_SET_QUOTA | PROCESS_TERMINATE.
            process = _kernel.OpenProcess(0x0100 | 0x0001, False, pid)
            if not process:
                raise _error(f"Cannot own ACP process {pid} (open process)")
            if not _kernel.AssignProcessToJobObject(job, process):
                raise _error(f"Cannot own ACP process {pid} (assign job)")
        except BaseException:
            _kernel.CloseHandle(job)
            raise
        finally:
            if process:
                _kernel.CloseHandle(process)
        self._handle = job

    def close(self):
        if self._handle is not None:
            if not _kernel.CloseHandle(self._handle):
                raise _error("Cannot close ACP process ownership")
            self._handle = None
