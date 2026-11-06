"""Small cross-platform process helpers shared by the worker command, the runner and the connector."""
import os
import signal
import subprocess
import sys
import time

WINDOWS = os.name == 'nt'


def detached_flags():
    """Flags that let a child outlive its parent and never show a console window (Windows) or share its session (POSIX)."""
    if WINDOWS: return subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.CREATE_NO_WINDOW
    return 0


def spawn_detached(argv, cwd=None, log=None, env=None):
    """Start a process that keeps running after this one exits. Returns its pid."""
    out = open(log, 'ab') if log else subprocess.DEVNULL
    try:
        process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL, stdout=out, stderr=subprocess.STDOUT,
                                   creationflags=detached_flags(), start_new_session=not WINDOWS, close_fds=True)
    finally:
        if log: out.close()
    return process.pid


def pid_alive(pid):
    if not pid: return False
    if WINDOWS:
        result = subprocess.run(['tasklist', '/FI', f'PID eq {int(pid)}', '/FO', 'CSV', '/NH'], capture_output=True, text=True, creationflags=subprocess.CREATE_NO_WINDOW)
        return f'"{int(pid)}"' in result.stdout
    try: os.kill(int(pid), 0)
    except ProcessLookupError: return False
    except PermissionError: return True
    return True


def kill_tree(pid, timeout=10.0):
    """Stop a process and everything it started: gently first, then by force. True when it is gone."""
    if not pid_alive(pid): return True
    if WINDOWS:
        subprocess.run(['taskkill', '/PID', str(int(pid)), '/T', '/F'], capture_output=True, creationflags=subprocess.CREATE_NO_WINDOW)
    else:
        try: os.killpg(os.getpgid(int(pid)), signal.SIGTERM)
        except (ProcessLookupError, PermissionError): pass
        deadline = time.monotonic() + timeout
        while pid_alive(pid) and time.monotonic() < deadline: time.sleep(.2)
        if pid_alive(pid):
            try: os.killpg(os.getpgid(int(pid)), signal.SIGKILL)
            except (ProcessLookupError, PermissionError): pass
    deadline = time.monotonic() + timeout
    while pid_alive(pid) and time.monotonic() < deadline: time.sleep(.2)
    return not pid_alive(pid)


def python_for(home):
    """The worker's own interpreter: the venv inside the install folder."""
    from pathlib import Path
    home = Path(home)
    candidate = home / ('venv/Scripts/python.exe' if WINDOWS else 'venv/bin/python')
    return str(candidate) if candidate.exists() else sys.executable
