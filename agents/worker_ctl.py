"""agent-worker: manage this PC's execution worker (installed by the Fleet tab's one-line command).

    agent-worker status [--json]       what is installed, running and connected
    agent-worker start|stop|restart [NAME]
                                       the whole worker, or one component (any engine worker the runner supervises)
    agent-worker update [--force]      fetch the server's current worker, wait for running jobs, swap it in, restart; rolls back on failure
    agent-worker uninstall [--yes]     stop it, remove its startup entry and files, and remove this PC from the fleet
    agent-worker logs [-n 50]          the end of the worker's log
    agent-worker version
    agent-worker component list | add [--cwd DIR] [--env K=V] NAME -- COMMAND ... | remove NAME

The command uses only the standard library, so it still works when the worker's own packages are broken.
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from flowkit import components as comp  # noqa: E402
from flowkit.procutil import WINDOWS, kill_tree, pid_alive, python_for, spawn_detached  # noqa: E402

HOME_DEFAULT = Path(__file__).resolve().parents[1]
DRAIN_TIMEOUT = 600
HEALTH_TIMEOUT = 90
NO_WINDOW = subprocess.CREATE_NO_WINDOW if WINDOWS else 0


class CtlError(Exception):
    """A problem the user can act on; the message is printed and the exit code is 1."""


# --------------------------------------------------------------------------- the installed worker
def load_config(home):
    try: return json.loads((Path(home) / 'worker.json').read_text(encoding='utf-8'))
    except FileNotFoundError: raise CtlError(f'No worker is installed in {home}')
    except (OSError, ValueError) as exc: raise CtlError(f'worker.json is unreadable: {exc}')


def read_text(path, default=''):
    try: return Path(path).read_text(encoding='utf-8').strip()
    except OSError: return default


def installed_version(home): return read_text(Path(home) / 'version')
def token_of(home): return read_text(Path(home) / 'token')


def http(method, url, token=None, body=None, timeout=15):
    """One JSON request with urllib. Returns (status, parsed body or None)."""
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(url, data=data, method=method, headers={**({'Authorization': f'Bearer {token}'} if token else {}), **({'Content-Type': 'application/json'} if data else {})})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
            return response.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        try: return exc.code, json.loads(exc.read() or b'null')
        except ValueError: return exc.code, None


def local_health(home, cfg):
    try:
        status, data = http('GET', f'http://127.0.0.1:{int(cfg.get("port", 8096))}/health', token_of(home), timeout=5)
        return data if status == 200 else None
    except (OSError, ValueError): return None


# --------------------------------------------------------------------------- service managers
class Service:
    """The platform's startup entry for the runner. `pidfile` is written by the runner itself on every platform."""
    kind = 'process'

    def __init__(self, home, cfg):
        self.home, self.cfg = Path(home), cfg
        self.unit = cfg.get('unit') or f'agent-worker-{cfg.get("id", "worker")}'

    def pid(self):
        try: return int(read_text(self.home / 'worker.pid') or 0)
        except ValueError: return 0

    def running(self): return pid_alive(self.pid())
    def exists(self): return True
    def argv(self): return [python_for(self.home), '-u', str(self.home / 'agents' / 'worker_runner.py'), '--home', str(self.home), '--log', str(self.home / 'worker.log')]

    def wait_running(self, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.running(): return True
            time.sleep(.5)
        return self.running()

    def start(self):
        if self.running(): return
        spawn_detached(self.argv(), cwd=str(self.home / 'agents'))
        if not self.wait_running(): raise CtlError(f'The worker did not start; see {self.home / "worker.log"}')

    def stop(self):
        pid = self.pid()
        if pid and not kill_tree(pid): raise CtlError('The worker did not stop')
        (self.home / 'worker.pid').unlink(missing_ok=True)

    def remove(self): pass
    def describe(self): return 'background process (no startup entry)'


class WindowsTask(Service):
    kind = 'task'

    def run(self, *args):
        return subprocess.run(['schtasks', *args], capture_output=True, text=True, creationflags=NO_WINDOW)

    def exists(self): return self.run('/Query', '/TN', self.unit).returncode == 0

    def start(self):
        if self.running(): return
        result = self.run('/Run', '/TN', self.unit)
        if result.returncode != 0: raise CtlError(f'Could not start the scheduled task {self.unit}: {result.stderr.strip() or result.stdout.strip()}')
        if not self.wait_running(): raise CtlError(f'The worker did not start; see {self.home / "worker.log"}')

    def stop(self):
        self.run('/End', '/TN', self.unit)
        super().stop()

    def remove(self):
        self.run('/End', '/TN', self.unit)
        self.run('/Delete', '/TN', self.unit, '/F')

    def describe(self): return f'Windows scheduled task {self.unit} (starts when you log in; restarted if it fails)'


class SystemdUser(Service):
    kind = 'systemd'

    def ctl(self, *args):
        return subprocess.run(['systemctl', '--user', *args], capture_output=True, text=True)

    def exists(self): return (Path.home() / '.config/systemd/user' / f'{self.unit}.service').exists()

    def running(self): return self.ctl('is-active', '--quiet', f'{self.unit}.service').returncode == 0

    def start(self):
        result = self.ctl('start', f'{self.unit}.service')
        if result.returncode != 0: raise CtlError(f'systemctl start failed: {result.stderr.strip()}')
        if not self.wait_running(): raise CtlError(f'The worker did not start; see {self.home / "worker.log"}')

    def stop(self):
        self.ctl('stop', f'{self.unit}.service')
        (self.home / 'worker.pid').unlink(missing_ok=True)

    def remove(self):
        self.ctl('disable', '--now', f'{self.unit}.service')
        (Path.home() / '.config/systemd/user' / f'{self.unit}.service').unlink(missing_ok=True)
        self.ctl('daemon-reload')

    def describe(self): return f'systemd user service {self.unit} (restarted if it fails)'


class Launchd(Service):
    kind = 'launchd'

    def plist(self): return Path.home() / 'Library/LaunchAgents' / f'{self.unit}.plist'
    def domain(self): return f'gui/{os.getuid()}'
    def ctl(self, *args): return subprocess.run(['launchctl', *args], capture_output=True, text=True)
    def exists(self): return self.plist().exists()

    def start(self):
        self.ctl('bootstrap', self.domain(), str(self.plist()))
        self.ctl('kickstart', f'{self.domain()}/{self.unit}')
        if not self.wait_running(): raise CtlError(f'The worker did not start; see {self.home / "worker.log"}')

    def stop(self):
        self.ctl('bootout', f'{self.domain()}/{self.unit}')
        super().stop()

    def remove(self):
        self.ctl('bootout', f'{self.domain()}/{self.unit}')
        self.plist().unlink(missing_ok=True)

    def describe(self): return f'launchd agent {self.unit} (restarted if it fails)'


def service_for(home, cfg):
    return {'task': WindowsTask, 'systemd': SystemdUser, 'launchd': Launchd}.get(cfg.get('service'), Service)(home, cfg)


# --------------------------------------------------------------------------- commands
def snapshot(home):
    cfg = load_config(home)
    svc = service_for(home, cfg)
    running = svc.running()
    health = local_health(home, cfg) if running else None
    connector = {}
    try: connector = json.loads((Path(home) / 'runtime' / 'connector.json').read_text(encoding='utf-8'))
    except (OSError, ValueError): pass
    fresh = time.time() - connector.get('at', 0) < 30
    return {'id': cfg.get('id'), 'version': installed_version(home), 'home': str(home), 'server': cfg.get('server'), 'service': svc.describe(), 'service_exists': svc.exists(),
            'running': running, 'pid': svc.pid() if svc.kind != 'systemd' else None, 'serving': bool(health), 'active_jobs': (health or {}).get('active'),
            'draining': bool((health or {}).get('draining')), 'connected': bool(running and fresh and connector.get('connected')),
            'components': comp.read_status(home), 'update': read_json(Path(home) / 'update-status.json')}


def read_json(path):
    try: return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError): return None


def cmd_status(args):
    s = snapshot(args.home)
    if args.json:
        print(json.dumps(s, indent=2))
    else:
        print(f'Worker      {s["id"]}  version {s["version"] or "unknown"}')
        print(f'Service     {s["service"]}')
        print(f'State       {"running" if s["running"] else "stopped"}' + (f' (pid {s["pid"]})' if s['pid'] and s['running'] else '')
              + (f', {s["active_jobs"]} job(s) running' if s['active_jobs'] else '') + (', draining for an update' if s['draining'] else ''))
        print(f'Server      {s["server"]}  {"connected" if s["connected"] else "not connected"}')
        for name, info in sorted(s['components'].items()):
            print(f'Component   {name}: {info.get("state")}' + (f' (pid {info["pid"]})' if info.get('pid') else '') + (f', restarted {info["restarts"]}x' if info.get('restarts') else ''))
        if s['update']: print(f'Last update {s["update"].get("state")}' + (f': {s["update"].get("error")}' if s['update'].get('error') else '') + (f' -> {s["update"].get("version")}' if s['update'].get('version') else ''))
    return 0 if s['running'] else 3


def cmd_start(args):
    if args.name:
        comp.control(args.home, args.name, 'start')
        print(f'Component {args.name} will start within a few seconds')
        return 0
    cfg = load_config(args.home)
    svc = service_for(args.home, cfg)
    if not svc.exists(): raise CtlError(f'The startup entry is missing ({svc.describe()}); reinstall with a new install command')
    svc.start()
    print('Worker started')
    return 0


def cmd_stop(args):
    if args.name:
        comp.control(args.home, args.name, 'stop')
        print(f'Component {args.name} will stop within a few seconds')
        return 0
    service_for(args.home, load_config(args.home)).stop()
    print('Worker stopped')
    return 0


def cmd_restart(args):
    if args.name:
        comp.control(args.home, args.name, 'restart')
        print(f'Component {args.name} will restart within a few seconds')
        return 0
    svc = service_for(args.home, load_config(args.home))
    svc.stop()
    svc.start()
    print('Worker restarted')
    return 0


def cmd_logs(args):
    path = Path(args.home) / 'worker.log'
    if not path.exists(): raise CtlError('There is no log yet')
    lines = path.read_text(encoding='utf-8', errors='replace').splitlines()
    print('\n'.join(lines[-args.lines:]))
    return 0


def cmd_version(args):
    print(installed_version(args.home) or 'unknown')
    return 0


def parse_env(items):
    env = {}
    for item in items or []:
        key, sep, value = item.partition('=')
        if not sep or not key: raise CtlError(f'--env wants NAME=VALUE, got {item!r}')
        env[key] = value
    return env


def cmd_component(args):
    try:
        if args.action != 'list' and not args.name: raise CtlError('A component name is required')
        if args.action == 'list':
            status = comp.read_status(args.home)
            entries = comp.load(args.home)
            if not entries: print('No components. Add one with: agent-worker component add NAME -- COMMAND ...')
            for e in entries:
                print(f'{e["name"]}: {status.get(e["name"], {}).get("state", "not started yet")}  {" ".join(e["command"])}')
        elif args.action == 'add':
            command = args.argv[1:] if args.argv and args.argv[0] == '--' else args.argv
            entry = comp.add(args.home, {'name': args.name, 'command': command, **({'cwd': args.cwd} if args.cwd else {}), **({'env': parse_env(args.env)} if args.env else {})})
            print(f'Added {entry["name"]}: the worker starts it within a few seconds and restarts it if it exits')
        elif args.action == 'remove':
            comp.remove(args.home, args.name)
            print(f'Removed {args.name}')
    except comp.ComponentError as exc:
        raise CtlError(str(exc))
    return 0


# --------------------------------------------------------------------------- update
def hash_tree(agents_dir):
    """The worker version of a folder: paths and contents of its files (the server computes the same from the bundle's members)."""
    digest = hashlib.sha256()
    root = Path(agents_dir)
    files = sorted((p for p in root.rglob('*') if p.is_file() and 'node_modules' not in p.relative_to(root).parts and '__pycache__' not in p.relative_to(root).parts
                    and p.suffix not in ('.pyc', '.pyo')), key=lambda p: p.relative_to(root).as_posix())
    for path in files: digest.update(path.relative_to(root).as_posix().encode() + b'\0' + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()[:16]


def sha(path):
    try: return hashlib.sha256(Path(path).read_bytes()).hexdigest()
    except OSError: return None


def write_update_status(home, state, **extra):
    data = {'state': state, 'at': time.time(), **extra}
    temporary = Path(home) / 'update-status.json.tmp'
    temporary.write_text(json.dumps(data), encoding='utf-8')
    os.replace(temporary, Path(home) / 'update-status.json')


def download(url, destination, token=None):
    request = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'} if token else {})
    with urllib.request.urlopen(request, timeout=120) as response, open(destination, 'wb') as out:
        shutil.copyfileobj(response, out)
        return response.headers.get('X-Worker-Version', '')


def extract(archive, target):
    with tarfile.open(archive) as tar:
        if hasattr(tarfile, 'data_filter'): tar.extractall(target, filter='data')
        else:
            for member in tar.getmembers():
                if member.name.startswith(('/', '..')) or '..' in Path(member.name).parts: raise CtlError('The downloaded worker contains an unsafe path')
            tar.extractall(target)


def drain(home, cfg, on, wait=0):
    """Ask the service to stop admitting work and wait for running jobs (up to `wait` seconds). True when idle."""
    port = int(cfg.get('port', 8096))
    try: status, data = http('POST', f'http://127.0.0.1:{port}/drain', token_of(home), {'drain': on}, timeout=10)
    except (OSError, ValueError): return True  # the service is not running: nothing to wait for
    if status != 200 or not on: return status == 200
    deadline = time.monotonic() + wait
    while (data or {}).get('active'):
        if time.monotonic() >= deadline: return False
        time.sleep(2)
        try: status, data = http('POST', f'http://127.0.0.1:{port}/drain', token_of(home), {'drain': True}, timeout=10)
        except (OSError, ValueError): return True
    return True


def install_dependencies(home, old, new, log):
    python = python_for(home)
    if sha(old / 'requirements-worker.txt') != sha(new / 'requirements-worker.txt'):
        log('Python packages changed: installing them')
        result = subprocess.run([python, '-m', 'pip', 'install', '--quiet', '-r', str(new / 'requirements-worker.txt')], capture_output=True, text=True, env={**os.environ, 'PIP_DISABLE_PIP_VERSION_CHECK': '1'}, creationflags=NO_WINDOW)
        if result.returncode != 0: raise CtlError(f'pip could not install the new packages: {result.stderr.strip()[-300:]}')
    if sha(old / 'package-lock.json') != sha(new / 'package-lock.json') and (new / 'package-lock.json').exists():
        log('Agent adapters changed: installing them')
        npm = shutil.which('npm.cmd' if WINDOWS else 'npm') or shutil.which('npm')
        if not npm: raise CtlError('npm is needed to update the agent adapters but was not found')
        result = subprocess.run([npm, 'ci', '--prefix', str(new), '--no-audit', '--no-fund', '--loglevel=error'], capture_output=True, text=True, creationflags=NO_WINDOW)
        if result.returncode != 0: raise CtlError(f'npm could not install the new adapters: {result.stderr.strip()[-300:]}')


def wait_serving(home, cfg, version, timeout=HEALTH_TIMEOUT):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        health = local_health(home, cfg)
        if health and (not version or health.get('worker_version') == version): return True
        time.sleep(1)
    return False


def apply_update(home, cfg, new_root, version, drain_timeout, log):
    """Swap `new_root/agents` in for the installed agents folder. Raises CtlError (after rolling back) on failure."""
    home = Path(home)
    svc = service_for(home, cfg)
    was_running = svc.running()
    if was_running:
        log(f'Waiting up to {drain_timeout} s for running jobs to finish')
        if not drain(home, cfg, True, drain_timeout):
            drain(home, cfg, False)
            raise CtlError(f'Jobs were still running after {drain_timeout} s; nothing was changed. Try again later or use --drain-timeout')
    log('Stopping the worker')
    svc.stop()
    agents, prev = home / 'agents', home / 'agents.prev'
    old_version = installed_version(home)
    if prev.exists(): shutil.rmtree(prev)
    os.replace(agents, prev)
    try:
        shutil.move(str(new_root / 'agents'), str(agents))
        if (prev / 'node_modules').exists() and not (agents / 'node_modules').exists(): os.replace(prev / 'node_modules', agents / 'node_modules')
        install_dependencies(home, prev, agents, log)
        (home / 'version').write_text(version, encoding='utf-8')
        if was_running:
            log('Starting the new worker')
            svc.start()
            if not wait_serving(home, cfg, version): raise CtlError(f'The new worker did not become healthy within {HEALTH_TIMEOUT} s; see {home / "worker.log"}')
    except BaseException as exc:
        log(f'Update failed ({exc}); restoring the previous worker')
        try: svc.stop()
        except CtlError: pass
        if agents.exists(): shutil.rmtree(agents, ignore_errors=True)
        os.replace(prev, agents)
        (home / 'version').write_text(old_version, encoding='utf-8')
        if was_running:
            try: svc.start()
            except CtlError as again: log(f'The previous worker did not restart either: {again}')
        raise
    return old_version


def cmd_update(args):
    home = Path(args.home)
    cfg = load_config(home)
    log = (lambda text: print(text, flush=True))
    started = time.time()
    token = token_of(home)
    scratch = Path(tempfile.mkdtemp(prefix='update-', dir=str(home)))
    try:
        if args.from_dir:
            new_root, version = Path(args.from_dir), args.version or hash_tree(Path(args.from_dir) / 'agents')
        else:
            if not cfg.get('server'): raise CtlError('worker.json has no server address')
            log(f'Fetching the current worker from {cfg["server"]}')
            archive = scratch / 'bundle.tgz'
            try: version = download(f'{cfg["server"].rstrip("/")}/relay/{cfg["id"]}/update/bundle.tgz', archive, token)
            except urllib.error.HTTPError as exc: raise CtlError(f'The server refused the download (HTTP {exc.code}): this PC may no longer be registered')
            except OSError as exc: raise CtlError(f'Could not reach the server: {exc}')
            new_root = scratch / 'new'
            extract(archive, new_root)
        if not (new_root / 'agents' / 'worker_runner.py').exists(): raise CtlError('The downloaded worker is incomplete')
        actual = hash_tree(new_root / 'agents')
        if version and actual != version: raise CtlError(f'The downloaded worker does not match its version (expected {version}, got {actual})')
        version = actual
        current = installed_version(home)
        if version == current and not args.force:
            log(f'Already up to date (version {version})')
            write_update_status(home, 'updated', version=version, started_at=started)
            return 0
        log(f'Updating {current or "unknown"} -> {version}')
        write_update_status(home, 'running', version=version, started_at=started)
        apply_update(home, cfg, new_root, version, args.drain_timeout, log)
        write_update_status(home, 'updated', version=version, started_at=started)
        log(f'Updated to {version}')
        return 0
    except BaseException as exc:
        message = str(exc) or exc.__class__.__name__
        write_update_status(home, 'failed', error=message[:300], started_at=started)
        if isinstance(exc, CtlError): raise
        raise CtlError(message)
    finally:
        shutil.rmtree(scratch, ignore_errors=True)


# --------------------------------------------------------------------------- uninstall
def remove_cli(home, cfg):
    for link in cfg.get('cli_links') or []:
        try: Path(link).unlink()
        except OSError: pass
    entry = cfg.get('path_entry')
    if WINDOWS and entry:
        script = ("$e = [Environment]::GetEnvironmentVariable('Path','User'); if ($e) { $p = ($e -split ';') | Where-Object { $_ -and ($_ -ne '%s') }; "
                  "[Environment]::SetEnvironmentVariable('Path', ($p -join ';'), 'User') }") % entry.replace("'", "''")
        subprocess.run(['powershell', '-NoProfile', '-NonInteractive', '-Command', script], capture_output=True, creationflags=NO_WINDOW)


def cmd_uninstall(args):
    home = Path(args.home)
    cfg = load_config(home)
    if not args.yes:
        answer = input(f'Remove worker {cfg.get("id")} from this PC and from the fleet? [y/N] ').strip().lower()
        if answer not in ('y', 'yes'):
            print('Nothing was changed')
            return 1
    token = token_of(home)
    registered = False
    if not args.keep_registration and cfg.get('server'):
        try:
            status, _ = http('DELETE', f'{cfg["server"].rstrip("/")}/relay/{cfg["id"]}/registration', token, timeout=20)
            registered = status == 200
            if status not in (200, 401, 404): print(f'Warning: the server answered {status} when removing this PC from the fleet; remove it in the Fleet tab')
        except OSError as exc:
            print(f'Warning: could not reach the server to remove this PC from the fleet ({exc}); remove it in the Fleet tab')
    svc = service_for(home, cfg)
    try: svc.stop()
    except CtlError as exc: print(f'Warning: {exc}')
    svc.remove()
    remove_cli(home, cfg)
    if WINDOWS:
        # This process runs from inside the folder: remove it from a detached helper once we have exited.
        subprocess.Popen(['cmd', '/c', f'ping -n 4 127.0.0.1 >nul & rmdir /s /q "{home}"'], creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP | NO_WINDOW,
                         stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
    else:
        shutil.rmtree(home, ignore_errors=True)
    print(f'Worker {cfg.get("id")} removed from this PC' + ('' if args.keep_registration else (' and from the fleet' if registered else '')))
    return 0


# --------------------------------------------------------------------------- entry point
def build_parser():
    parser = argparse.ArgumentParser(prog='agent-worker', description=__doc__.split('\n\n')[0])
    parser.add_argument('--home', default=os.environ.get('AGENT_WORKER_HOME') or str(HOME_DEFAULT), help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest='command', required=True)
    s = sub.add_parser('status', help='what is installed, running and connected'); s.add_argument('--json', action='store_true'); s.set_defaults(run=cmd_status)
    for name, run in (('start', cmd_start), ('stop', cmd_stop), ('restart', cmd_restart)):
        s = sub.add_parser(name, help=f'{name} the worker, or one component'); s.add_argument('name', nargs='?', help='a component (default: the whole worker)'); s.set_defaults(run=run)
    s = sub.add_parser('update', help="fetch the server's current worker and restart into it")
    s.add_argument('--force', action='store_true', help='reinstall even when the version matches')
    s.add_argument('--drain-timeout', type=int, default=DRAIN_TIMEOUT, help='seconds to wait for running jobs (default 600)')
    s.add_argument('--background', action='store_true', help='used when the server starts the update; no difference to the result')
    s.add_argument('--from-dir', help=argparse.SUPPRESS); s.add_argument('--version', help=argparse.SUPPRESS); s.set_defaults(run=cmd_update)
    s = sub.add_parser('uninstall', help='remove this worker and its fleet registration')
    s.add_argument('--yes', action='store_true'); s.add_argument('--keep-registration', action='store_true', help='leave the PC registered in the fleet'); s.set_defaults(run=cmd_uninstall)
    s = sub.add_parser('logs', help="the end of the worker's log"); s.add_argument('-n', '--lines', type=int, default=50); s.set_defaults(run=cmd_logs)
    s = sub.add_parser('version', help='the installed worker version'); s.set_defaults(run=cmd_version)
    s = sub.add_parser('component', help='extra processes the worker keeps running (any engine worker)')
    s.add_argument('action', choices=('list', 'add', 'remove')); s.add_argument('name', nargs='?')
    s.add_argument('--cwd'); s.add_argument('--env', action='append', metavar='NAME=VALUE'); s.add_argument('argv', nargs=argparse.REMAINDER)
    s.set_defaults(run=cmd_component)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try: return args.run(args)
    except CtlError as exc:
        print(f'error: {exc}', file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    sys.exit(main())
