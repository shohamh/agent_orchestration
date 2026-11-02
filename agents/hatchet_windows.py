"""Windows startup compatibility for the pinned hatchet-sdk==1.44.0 only.

The SDK worker unconditionally registers Unix SIGQUIT. Its spawned listener
also calls ProactorEventLoop.add_signal_handler for two no-op handlers, although
Windows asyncio does not implement that API. The parent controls listener
shutdown through a multiprocessing Event, so those two child handlers may ignore
SIGINT/SIGTERM using signal.signal. This is not a Unix signal implementation.

Only this native worker opts in. No SDK files are edited, and the child event-loop
patch exists only while its original listener target is executing.
"""
import asyncio
import signal
import sys
from contextlib import contextmanager
from importlib.metadata import version
from typing import Any

PINNED_VERSION = "1.44.0"


def _setup_worker_signals(worker: Any) -> None:
    handler = (worker._handle_force_quit_signal if worker._config.force_shutdown_on_shutdown_signal
               else worker._handle_exit_signal)
    signal.signal(signal.SIGTERM, handler)
    signal.signal(signal.SIGINT, handler)


def _ignore_parent_owned_signal(loop: Any, signum: int, callback: Any, *args: Any) -> None:
    if signum not in (signal.SIGINT, signal.SIGTERM):
        raise NotImplementedError("Only Hatchet listener SIGINT/SIGTERM no-op handlers are supported")
    signal.signal(signum, signal.SIG_IGN)


@contextmanager
def listener_signal_compatibility():
    """Local child scope; the SDK registers only no-op handlers at initialization."""
    loop_type = asyncio.ProactorEventLoop
    original = loop_type.add_signal_handler
    loop_type.add_signal_handler = _ignore_parent_owned_signal
    try:
        yield
    finally:
        loop_type.add_signal_handler = original


def windows_listener_process(*args: Any, **kwargs: Any) -> None:
    """Module-level entrypoint, importable by Windows multiprocessing spawn."""
    from hatchet_sdk.worker.action_listener_process import worker_action_listener_process
    with listener_signal_compatibility():
        worker_action_listener_process(*args, **kwargs)


def install() -> bool:
    if sys.platform != "win32":
        return False
    actual = version("hatchet-sdk")
    if actual != PINNED_VERSION:
        raise RuntimeError(f"Windows compatibility requires hatchet-sdk=={PINNED_VERSION}; found {actual}. Review the shim before upgrading.")
    import hatchet_sdk.worker.worker as worker_module
    worker_module.Worker._setup_signal_handlers = _setup_worker_signals
    worker_module.worker_action_listener_process = windows_listener_process
    return True
