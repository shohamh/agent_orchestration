"""Extra processes the worker runner keeps alive next to the execution service: any engine worker, generically.

A component is a name and a command in `<home>/components.json`:

    {"components": [{"name": "hatchet", "command": ["python", "worker.py"], "cwd": "C:/work", "env": {"HATCHET_CLIENT_TOKEN": "..."}}]}

The runner starts every component, restarts it when it exits (1, 2, 5, 10, then 30 seconds apart; the delay resets after
a minute of uptime) and reloads the file while running. `agent-worker stop NAME` and `start NAME` work through files in
`<home>/control/` (`NAME.stop` keeps it stopped, `NAME.restart` restarts it once), so they need no network and work the
same on every OS. What each component is doing is written to `<home>/runtime/components.json`.
"""
import asyncio
import json
import os
import re
import time
from pathlib import Path

from .procutil import WINDOWS, kill_tree, pid_alive

NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$')
BACKOFF = (1, 2, 5, 10, 30)
STABLE_AFTER = 60.0
RESERVED = {'execution', 'connector'}


class ComponentError(ValueError):
    pass


def components_path(home): return Path(home) / 'components.json'
def control_dir(home): return Path(home) / 'control'
def status_path(home): return Path(home) / 'runtime' / 'components.json'


def validate(entry):
    if not isinstance(entry, dict): raise ComponentError('A component is an object')
    name = entry.get('name')
    if not isinstance(name, str) or not NAME.match(name): raise ComponentError('A component name is 1 to 64 letters, digits, dashes or underscores')
    if name in RESERVED: raise ComponentError(f'{name} is reserved for the worker itself')
    command = entry.get('command')
    if not isinstance(command, list) or not command or not all(isinstance(c, str) and c for c in command): raise ComponentError('A component needs a command: a non-empty list of strings')
    env = entry.get('env', {})
    if not isinstance(env, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in env.items()): raise ComponentError('env must map names to strings')
    cwd = entry.get('cwd')
    if cwd is not None and not isinstance(cwd, str): raise ComponentError('cwd must be a path')
    return {'name': name, 'command': list(command), **({'cwd': cwd} if cwd else {}), **({'env': dict(env)} if env else {})}


def load(home):
    """The configured components; a broken file means none (and the runner logs it) rather than a dead worker."""
    try: data = json.loads(components_path(home).read_text(encoding='utf-8'))
    except FileNotFoundError: return []
    except (OSError, ValueError) as exc: raise ComponentError(f'components.json is unreadable: {exc}')
    entries = data.get('components', []) if isinstance(data, dict) else None
    if not isinstance(entries, list): raise ComponentError('components.json must hold {"components": [...]}')
    seen, out = set(), []
    for entry in entries:
        entry = validate(entry)
        if entry['name'] in seen: raise ComponentError(f'Duplicate component {entry["name"]}')
        seen.add(entry['name'])
        out.append(entry)
    return out


def save(home, entries):
    from .restart import atomic_json
    names = [validate(e)['name'] for e in entries]
    if len(set(names)) != len(names): raise ComponentError('Duplicate component names')
    Path(home).mkdir(parents=True, exist_ok=True)
    atomic_json(components_path(home), {'components': entries})


def add(home, entry):
    entry = validate(entry)
    entries = load(home)
    if any(e['name'] == entry['name'] for e in entries): raise ComponentError(f'{entry["name"]} already exists; remove it first')
    save(home, entries + [entry])
    return entry


def remove(home, name):
    entries = load(home)
    if not any(e['name'] == name for e in entries): raise ComponentError(f'No component named {name}')
    save(home, [e for e in entries if e['name'] != name])
    for suffix in ('stop', 'restart'): (control_dir(home) / f'{name}.{suffix}').unlink(missing_ok=True)


def control(home, name, action):
    """stop: keep it stopped. start: lift a stop. restart: restart once."""
    if not any(e['name'] == name for e in load(home)): raise ComponentError(f'No component named {name}')
    control_dir(home).mkdir(parents=True, exist_ok=True)
    stop, restart = control_dir(home) / f'{name}.stop', control_dir(home) / f'{name}.restart'
    if action == 'stop': stop.write_text('1')
    elif action == 'start': stop.unlink(missing_ok=True)
    elif action == 'restart':
        stop.unlink(missing_ok=True)
        restart.write_text('1')
    else: raise ComponentError(f'Unknown action {action}')


def read_status(home):
    try: return json.loads(status_path(home).read_text(encoding='utf-8'))
    except (OSError, ValueError): return {}


class Supervisor:
    """Keeps the configured components running; stop() ends them all."""

    def __init__(self, home, log, interval=1.0):
        self.home, self.log, self.interval = Path(home), log, interval
        self.running = {}  # name -> {'process', 'since', 'entry'}
        self.state = {}    # name -> status dict written to disk
        self.backoff = {}  # name -> [failures, not_before]
        self.stopping = False

    def write_status(self):
        from .restart import atomic_json
        try: atomic_json(status_path(self.home), self.state)
        except OSError: pass

    def reap_stale(self):
        """A previous runner that was killed leaves its components behind: stop them before starting new ones."""
        for name, info in read_status(self.home).items():
            pid = info.get('pid') if isinstance(info, dict) else None
            if pid and info.get('state') == 'running' and pid_alive(pid):
                self.log.warning('Stopping %s (pid %s) left behind by an earlier runner', name, pid)
                kill_tree(pid)

    async def start(self, entry):
        env = {**os.environ, **entry.get('env', {})}
        out = open(self.home / f'component-{entry["name"]}.log', 'ab')
        try:
            kwargs = {'creationflags': 0x08000000} if WINDOWS else {'start_new_session': True}  # no console window / own process group
            process = await asyncio.create_subprocess_exec(*entry['command'], cwd=entry.get('cwd') or str(self.home), env=env, stdin=asyncio.subprocess.DEVNULL, stdout=out, stderr=asyncio.subprocess.STDOUT, **kwargs)
        finally:
            out.close()
        self.running[entry['name']] = {'process': process, 'since': time.monotonic(), 'entry': entry}
        old = self.state.get(entry['name'], {})
        self.state[entry['name']] = {'state': 'running', 'pid': process.pid, 'restarts': old.get('restarts', 0), 'since': time.time(), 'last_exit': old.get('last_exit')}
        self.log.info('Started component %s (pid %s)', entry['name'], process.pid)

    async def stop_one(self, name, reason):
        info = self.running.pop(name, None)
        if info is None: return
        pid = info['process'].pid
        await asyncio.to_thread(kill_tree, pid)
        try: await asyncio.wait_for(info['process'].wait(), 5)
        except asyncio.TimeoutError: pass
        self.state[name] = {**self.state.get(name, {}), 'state': 'stopped', 'pid': None, 'reason': reason}
        self.log.info('Stopped component %s (%s)', name, reason)

    async def tick(self):
        try: entries = {e['name']: e for e in load(self.home)}
        except ComponentError as exc:
            self.log.error('%s', exc)
            entries = {name: info['entry'] for name, info in self.running.items()}  # keep what runs when the file is broken
        for name in [n for n in self.running if n not in entries]: await self.stop_one(name, 'removed')
        for name in [n for n in self.state if n not in entries and n not in self.running]: self.state.pop(name, None)
        for name, entry in entries.items():
            stop, restart = control_dir(self.home) / f'{name}.stop', control_dir(self.home) / f'{name}.restart'
            if stop.exists():
                await self.stop_one(name, 'stopped by command')
                self.state[name] = {**self.state.get(name, {}), 'state': 'stopped', 'pid': None, 'reason': 'stopped by command'}
                continue
            if restart.exists():
                restart.unlink(missing_ok=True)
                await self.stop_one(name, 'restart requested')
                self.backoff.pop(name, None)
            running = self.running.get(name)
            if running and running['entry'] != entry:  # edited while running
                await self.stop_one(name, 'configuration changed')
                running = None
            if running and running['process'].returncode is not None:
                code = running['process'].returncode
                self.running.pop(name, None)
                failures, _ = self.backoff.get(name, [0, 0])
                failures = 0 if time.monotonic() - running['since'] > STABLE_AFTER else failures
                delay = BACKOFF[min(failures, len(BACKOFF) - 1)]
                self.backoff[name] = [failures + 1, time.monotonic() + delay]
                old = self.state.get(name, {})
                self.state[name] = {**old, 'state': 'crashed', 'pid': None, 'restarts': old.get('restarts', 0) + 1, 'last_exit': code, 'retry_in': delay}
                self.log.warning('Component %s exited with %s; restarting in %s s', name, code, delay)
                running = None
            if running is None:
                if time.monotonic() < self.backoff.get(name, [0, 0])[1]: continue
                try: await self.start(entry)
                except (OSError, ValueError) as exc:
                    failures = self.backoff.get(name, [0, 0])[0]
                    delay = BACKOFF[min(failures, len(BACKOFF) - 1)]
                    self.backoff[name] = [failures + 1, time.monotonic() + delay]
                    self.state[name] = {**self.state.get(name, {}), 'state': 'crashed', 'pid': None, 'error': str(exc)[:200], 'retry_in': delay}
                    self.log.error('Could not start component %s: %s', name, exc)
        self.write_status()

    async def run(self):
        self.reap_stale()
        try:
            while not self.stopping:
                try: await self.tick()
                except asyncio.CancelledError: raise
                except Exception: self.log.exception('Component supervision failed')
                await asyncio.sleep(self.interval)
        finally:
            for name in list(self.running): await asyncio.shield(self.stop_one(name, 'worker stopping'))
            self.write_status()
