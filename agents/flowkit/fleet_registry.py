"""Coordinator-owned worker fleet registry. Connection secrets stay in local files."""
import os
import tempfile
import threading
import time
from contextlib import contextmanager
from dotenv import dotenv_values
from pathlib import Path
from urllib.parse import urlsplit
import yaml

ROOT = Path(__file__).resolve().parents[2]
_LOCK = threading.RLock()

def _path():
    return Path(os.environ.get('FLOW_WORKERS_FILE', ROOT / 'workers.local.yaml'))

@contextmanager
def _locked():
    """Serialize readers and read-modify-write transactions across coordinator processes."""
    with _LOCK:
        path = _path(); path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(path) + '.lock', 'a+b') as lockfile:
            if os.name == 'nt':
                import msvcrt
                lockfile.seek(0)
                while True:
                    try:
                        msvcrt.locking(lockfile.fileno(), msvcrt.LK_NBLCK, 1)
                        break
                    except OSError:
                        time.sleep(0.05)
            else:
                import fcntl
                fcntl.flock(lockfile.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                if os.name == 'nt':
                    lockfile.seek(0); msvcrt.locking(lockfile.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    fcntl.flock(lockfile.fileno(), fcntl.LOCK_UN)

def _default():
    return {'id': os.environ.get('FLOW_WORKER_ID', 'local'), 'pool': 'default', 'tags': [],
            'url': 'http://127.0.0.1:8096', 'token_env': 'FLOW_WORKER_TOKEN'}

def _normalize(item):
    if not isinstance(item, dict): raise ValueError('Invalid registry worker')
    w = dict(item)
    if not isinstance(w.get('id'), str) or not w['id'] or len(w['id']) > 100: raise ValueError('Invalid worker id')
    for field, default in [('pools', [w.get('pool', 'default')]), ('tags', [])]:
        value = w.get(field, default)
        if not isinstance(value, list) or len(value) > 50 or (field == 'pools' and not value) or any(not isinstance(v, str) or not v or len(v) > 100 for v in value) or len(set(value)) != len(value):
            raise ValueError('Invalid worker ' + field)
        w[field] = value
    w.pop('pool', None)
    for field, default in [('slots', 1), ('cpu_limit', 90), ('ram_limit', 90)]:
        value = w.get(field, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0 or (field == 'slots' and (not isinstance(value, int) or value > 10000)) or (field != 'slots' and value > 100): raise ValueError('Invalid worker ' + field)
        w[field] = value
    if 'url' in w:
        u = urlsplit(w['url'])
        if u.scheme not in ('http', 'https') or not u.hostname or u.username or u.password or u.query or u.fragment: raise ValueError('Invalid execution-worker endpoint')
    return w

def _read():
    path = _path()
    data = yaml.safe_load(path.read_text(encoding='utf-8')) if path.exists() else {'workers': [_default()]}
    if not isinstance(data, dict) or data.get('version', 1) not in (1, 2) or not isinstance(data.get('workers'), list): raise ValueError('Invalid worker registry')
    workers = [_normalize(w) for w in data['workers']]
    if len({w['id'] for w in workers}) != len(workers): raise ValueError('Worker ids must be unique')
    return workers

def _public(w):
    return {k: w[k] for k in ('id', 'pools', 'tags', 'slots', 'cpu_limit', 'ram_limit')} | {'engines': {}, 'online': False}

def load_fleet():
    with _locked(): return [_public(w) for w in _read()]

def _write(workers):
    path = _path(); path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f:
            yaml.safe_dump({'version': 2, 'workers': workers}, f, sort_keys=False)
            f.flush(); os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name): os.unlink(name)

def migrate_legacy_registry():
    with _locked():
        workers = _read(); path = _path()
        if path.exists():
            data = yaml.safe_load(path.read_text(encoding='utf-8'))
            if data.get('version') == 2: return {'migrated': False, 'workers': len(workers)}
        _write(workers)
        return {'migrated': True, 'workers': len(workers)}

def save_worker(worker):
    if not isinstance(worker, dict): raise ValueError('Invalid worker')
    allowed = {'id', 'pools', 'tags', 'slots', 'cpu_limit', 'ram_limit', 'url', 'token_env'}
    if set(worker) - allowed: raise ValueError('Unknown worker fields')
    with _locked():
        workers = _read(); existing = next((w for w in workers if w['id'] == worker.get('id')), {})
        updated = _normalize({**existing, **worker})
        if updated.get('url'):
            u = urlsplit(updated['url'])
            tokens = {**dotenv_values(ROOT / 'workers.local.env'), **os.environ}
            if u.hostname not in ('127.0.0.1', 'localhost', '::1') and not tokens.get(updated.get('token_env', '')):
                raise ValueError('Network worker needs token_env credentials')
        workers = [w for w in workers if w['id'] != updated['id']] + [updated]
        _write(workers)
        return _public(updated)

def delete_worker(worker_id):
    with _locked():
        workers = _read(); remaining = [w for w in workers if w['id'] != worker_id]
        if len(workers) == len(remaining): raise ValueError('Unknown worker')
        _write(remaining)
