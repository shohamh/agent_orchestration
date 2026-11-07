"""Optional Python-source restart supervisor for the development board container."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence


def enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Return whether backend auto-reload was explicitly enabled."""
    values = os.environ if environ is None else environ
    return values.get("BOARD_AUTO_RELOAD") == "1"


def snapshot(root: Path) -> tuple[tuple[str, int, int], ...]:
    """Capture Python source names and stat signatures under the watched source root."""
    entries = []
    for path in root.rglob("*.py"):
        try:
            stat = path.stat()
        except FileNotFoundError:
            continue
        if path.is_file():
            entries.append((path.relative_to(root).as_posix(), stat.st_mtime_ns, stat.st_size))
    return tuple(sorted(entries))


def _stop(child: subprocess.Popen[bytes]) -> None:
    if child.poll() is not None:
        return
    child.terminate()
    try:
        child.wait(timeout=10)
    except subprocess.TimeoutExpired:
        child.kill()
        child.wait()


def run(command: Sequence[str], *, root: Path, interval: float = 0.5,
        reload_enabled: bool | None = None) -> int:
    """Run a server command, restarting it when watched Python sources change."""
    if not command:
        raise ValueError("server command must not be empty")
    if reload_enabled is None:
        reload_enabled = enabled()
    if not reload_enabled:
        os.execvp(command[0], command)
        raise AssertionError("execvp returned")
    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signals = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGBREAK"):
        signals.append(signal.SIGBREAK)
    previous = {sig: signal.signal(sig, request_stop) for sig in signals}
    try:
        current = snapshot(root)
        while True:
            if stopping:
                return 0
            child = subprocess.Popen(command)
            restart = False
            while not stopping:
                try:
                    return_code = child.wait(timeout=interval)
                except subprocess.TimeoutExpired:
                    if snapshot(root) != current:
                        current = snapshot(root)
                        restart = True
                        break
                else:
                    return return_code
            _stop(child)
            if stopping:
                return 0
            if restart:
                continue
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--watch-root", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("command", nargs=argparse.REMAINDER, help="server command, after --")
    args = parser.parse_args(argv)
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command:
        parser.error("provide a server command after --")
    return run(command, root=args.watch_root, interval=args.poll_interval)


if __name__ == "__main__":
    raise SystemExit(main())
