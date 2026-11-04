"""Safe, timestamped discovery of software in the execution environment."""
import os
import re
import shlex
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path


def _result(status):
    return {'status': status, 'checked_at': datetime.now(timezone.utc).isoformat(), 'auth_status': 'unknown'}


def infer_command(run):
    # Only a single, literal command can be inferred. Never evaluate shell syntax.
    if not isinstance(run, str) or any(c in run for c in '\n\r`') or '$(' in run:
        return None
    quote = None
    escaped = False
    for char in run:
        if escaped:
            escaped = False
            continue
        if char == '\\' and os.name != 'nt' and quote != "'":
            escaped = True
            continue
        if quote:
            if char == quote: quote = None
        elif char in ('"', "'") and (os.name != 'nt' or char == '"'):
            quote = char
        elif char in '|&;<>()':
            return None
    if quote: return None
    try:
        parts = shlex.split(run, posix=os.name != 'nt')
    except ValueError:
        return None
    if not parts:
        return None
    name = parts[0].strip('"')
    if not name or any(c in name for c in '{}*?=$%'):
        return None
    from .template import _CMD_BUILTINS
    if name.lower() in _CMD_BUILTINS:
        return None
    return name


def probe_command(name):
    try:
        if not isinstance(name, str) or not name or any(c in name for c in '\x00\r\n'):
            return _result('unknown')
        found = (Path(sys.executable).is_file() if name == 'python' else bool(shutil.which(name)))
        return _result('installed' if found else 'absent')
    except (OSError, ValueError):
        return _result('unknown')


def probe_agent(name):
    from .definition import AGENTS
    if name not in AGENTS:
        return _result('unknown')
    if name == 'demo':
        return _result('installed')
    try:
        from agentdriver.acp_engine import agent_command
        command = agent_command(name)
        # agent_command resolves adapters and shims but deliberately falls back to literal node.
        if not shutil.which(command[0]) and not Path(command[0]).is_file():
            return _result('absent')
        if name == 'pi' and not shutil.which('pi'):
            return _result('absent')
        return _result('installed')
    except RuntimeError:
        return _result('absent')
    except (OSError, ValueError, ImportError):
        return _result('unknown')


def refresh(inventory, commands=()):
    from .definition import AGENTS
    return {'agents': {name: probe_agent(name) for name in AGENTS},
            'commands': {name: probe_command(name) for name in sorted(set(commands))},
            'refreshed_at': datetime.now(timezone.utc).isoformat()}


def overdue(inventory):
    try:
        timestamp = datetime.fromisoformat(inventory['refreshed_at'])
        if timestamp.tzinfo is None: return True
        age = (datetime.now(timezone.utc) - timestamp).total_seconds()
        return age < 0 or age >= 86400
    except (KeyError, TypeError, ValueError, OverflowError):
        return True
