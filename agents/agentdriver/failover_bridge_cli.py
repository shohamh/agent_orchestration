"""Entry point for the independent Windows Claude Desktop / Orca bridge."""
from __future__ import annotations

import argparse
import ctypes
import json
import os
import sys
import time
from pathlib import Path

from .failover_bridge import ConfigError, StateError, load_config
from .failover_runtime import build_runtime


def _acquire_mutex(state_path: str) -> int:
    if os.name != "nt":
        raise RuntimeError("the failover bridge only runs on Windows")
    import hashlib

    identity = os.path.normcase(str(Path(state_path).resolve(strict=False))).encode("utf-8")
    name = "Local\\agentdriver-failover-" + hashlib.sha256(identity).hexdigest()
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
    kernel.CreateMutexW.restype = ctypes.c_void_p
    ctypes.set_last_error(0)
    handle = kernel.CreateMutexW(None, True, name)
    if not handle:
        raise OSError(ctypes.get_last_error(), "could not create bridge mutex")
    if ctypes.get_last_error() == 183:  # ERROR_ALREADY_EXISTS
        kernel.CloseHandle(handle)
        raise RuntimeError("another bridge process already owns this state file")
    return int(handle)


def run(config_path: str, state_path: str) -> None:
    if not Path(config_path).is_absolute():
        raise ValueError("--config must be an absolute path outside the repository")
    if not Path(state_path).is_absolute():
        raise ValueError("--state must be an absolute path")
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.ReleaseMutex.argtypes = [ctypes.c_void_p]
    handle = _acquire_mutex(state_path)
    try:
        config = load_config(config_path)
        bridge = build_runtime(config, state_path)
        while True:
            for item in bridge.poll_once():
                print(json.dumps(item, ensure_ascii=False), flush=True)
            time.sleep(config.poll_interval_seconds)
    finally:
        kernel.ReleaseMutex(handle)
        kernel.CloseHandle(handle)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Windows Claude Desktop ↔ Orca failover bridge")
    parser.add_argument("--config", required=True, help="absolute external JSON configuration path")
    parser.add_argument("--state", required=True, help="absolute local transcript-free state path")
    args = parser.parse_args(argv)
    try:
        run(args.config, args.state)
    except KeyboardInterrupt:
        return 0
    except (ConfigError, StateError, OSError, RuntimeError, ValueError) as exc:
        print(json.dumps({"status": "stopped", "reason": str(exc)}, ensure_ascii=False),
              file=sys.stderr, flush=True)
        return 2
    return 0


if __name__ == "__main__":
    from .failover_bridge import ConfigError

    raise SystemExit(main())
