"""Coordinator-owned worker fleet registry. Connection secrets stay in local files."""
import os
import re
import tempfile
import time
import threading
import time
from contextlib import contextmanager
from dotenv import dotenv_values
from pathlib import Path
from urllib.parse import urlsplit
import yaml

from .capacity import MAX_SLOTS

ROOT = Path(__file__).resolve().parents[2]
_LOCK = threading.RLock()
ENGINES = ('hatchet', 'prefect', 'trigger')  # every engine reaches a PC through its execution service
TOKEN_ENV = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,99}$')


class AlreadyRegistered(ValueError):
    """A registration for an existing PC: edit it (for example to add an engine) instead."""


def label(value, what='tag'):
    """A new tag or pool name: visible text without separators, so it reads the same everywhere."""
    if (not isinstance(value, str) or not value or len(value) > 100 or value != value.strip() or ',' in value
            or any(ord(c) < 32 or ord(c) == 127 for c in value)):
        raise ValueError(f'A {what} is 1 to 100 characters without commas, line breaks or surrounding spaces')
    return value

def env_path():
    """The coordinator's token file (git-ignored); FLOW_WORKERS_ENV_FILE overrides it."""
    return Path(os.environ.get('FLOW_WORKERS_ENV_FILE', ROOT / 'workers.local.env'))

def _path():
    return Path(os.environ.get('FLOW_WORKERS_FILE', ROOT / 'workers.local.yaml'))

@contextmanager
def _locked():
    """Serialize read-modify-write transactions across coordinator processes."""
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

def _normalize(item, stored=False):
    """Validate one registry entry. `stored` entries (read from the file) above the slot
    maximum are clamped to it with a visible warning, so an old registry keeps working; a save
    above the maximum is refused."""
    if not isinstance(item, dict): raise ValueError('Invalid registry worker')
    w = dict(item)
    if not isinstance(w.get('id'), str) or not w['id'] or len(w['id']) > 100: raise ValueError('Invalid worker id')
    for field, default in [('pools', [w.get('pool', 'default')]), ('tags', [])]:
        value = w.get(field, default)
        if not isinstance(value, list) or len(value) > 50 or (field == 'pools' and not value) or any(not isinstance(v, str) or not v or len(v) > 100 for v in value) or len(set(value)) != len(value):
            raise ValueError('Invalid worker ' + field)
        w[field] = value
    w.pop('pool', None)
    engines = w.get('engines', list(ENGINES))
    if not isinstance(engines, list) or not engines or len(set(engines)) != len(engines) or any(e not in ENGINES for e in engines):
        raise ValueError('Invalid worker engines: choose one or more of ' + ', '.join(ENGINES))
    w['engines'] = engines
    if 'token_env' in w and (not isinstance(w['token_env'], str) or not TOKEN_ENV.match(w['token_env'])):
        raise ValueError('token_env must be an environment variable name (letters, digits and _)')
    revision = w.get('settings_revision', 0)
    if type(revision) is not int or revision < 0: raise ValueError('Invalid worker settings revision')
    w['settings_revision'] = revision
    w.pop('_slots_configured', None)
    for field, default in [('slots', 1), ('cpu_limit', 90), ('ram_limit', 90)]:
        value = w.get(field, default)
        if field == 'slots' and isinstance(value, int) and not isinstance(value, bool) and value > MAX_SLOTS:
            if not stored: raise ValueError(f'Invalid worker slots: at most {MAX_SLOTS} (the worker capacity gate maximum)')
            w['_slots_configured'], value = value, MAX_SLOTS
        if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0 or (field == 'slots' and not isinstance(value, int)) or (field != 'slots' and value > 100): raise ValueError('Invalid worker ' + field)
        w[field] = value
    if 'url' in w:
        u = urlsplit(w['url'])
        if u.scheme not in ('http', 'https') or not u.hostname or u.username or u.password or u.query or u.fragment: raise ValueError('Invalid execution-worker endpoint')
    return w

def _load():
    """The registry document: normalized workers plus the tag catalog (tags offered before use)."""
    path = _path()
    data = yaml.safe_load(path.read_text(encoding='utf-8')) if path.exists() else {'workers': [_default()]}
    if not isinstance(data, dict) or data.get('version', 1) not in (1, 2) or not isinstance(data.get('workers'), list): raise ValueError('Invalid worker registry')
    workers = [_normalize(w, stored=True) for w in data['workers']]
    if len({w['id'] for w in workers}) != len(workers): raise ValueError('Worker ids must be unique')
    tags = data.get('tags', [])
    if not isinstance(tags, list) or len(tags) > 1000 or any(not isinstance(t, str) or not t or len(t) > 100 for t in tags): raise ValueError('Invalid tag catalog')
    return {'workers': workers, 'tags': list(dict.fromkeys(tags))}

def _read():
    return _load()['workers']

def warnings(w):
    """Problems an operator should see for a stored entry (for example clamped slots)."""
    if w.get('_slots_configured'):
        return [f"slots {w['_slots_configured']} in the registry is above the maximum of {MAX_SLOTS}; {MAX_SLOTS} is enforced "
                'and stored at the next registry save']
    return []

def relayed(url):
    from .relay import is_relayed
    return is_relayed(url)

def _public(w):
    # Connection details never leave the coordinator: only whether an endpoint is configured.
    return ({k: w[k] for k in ('id', 'pools', 'tags', 'engines', 'slots', 'cpu_limit', 'ram_limit')}
            | {'connection': 'relay' if relayed(w.get('url')) else 'configured' if w.get('url') else 'awaiting', 'online': False}
            | ({'warnings': warnings(w)} if warnings(w) else {}))

def load_fleet():
    return [_public(w) for w in _read()]

def tag_catalog():
    doc = _load()
    return sorted(set(doc['tags']) | {t for w in doc['workers'] for t in w['tags']})

def add_tag(name):
    label(name)
    with _locked():
        doc = _load()
        if name not in doc['tags']: _write(doc['workers'], doc['tags'] + [name])
    return tag_catalog()

def _write(workers, tags=()):
    path = _path(); path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=path.parent)
    try:
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as f:
            stored = [{k: v for k, v in w.items() if not k.startswith('_')} for w in workers]
            yaml.safe_dump({'version': 2, 'workers': stored, **({'tags': list(tags)} if tags else {})}, f, sort_keys=False)
            f.flush(); os.fsync(f.fileno())
        # Windows readers may briefly hold the old file without delete sharing.
        for attempt in range(100):
            try:
                os.replace(name, path)
                break
            except PermissionError:
                if os.name != 'nt' or attempt == 99: raise
                time.sleep(0.01)
    finally:
        if os.path.exists(name): os.unlink(name)

def migrate_legacy_registry():
    with _locked():
        doc = _load(); workers = doc['workers']; path = _path()
        if path.exists():
            data = yaml.safe_load(path.read_text(encoding='utf-8'))
            if data.get('version') == 2: return {'migrated': False, 'workers': len(workers)}
        _write(workers, doc['tags'])
        return {'migrated': True, 'workers': len(workers)}

def save_worker(worker, create=False):
    """Create or update one PC's registration. With create, an existing id is refused: one PC
    is one registration however many engines and pools it serves."""
    if not isinstance(worker, dict): raise ValueError('Invalid worker')
    allowed = {'id', 'pools', 'tags', 'engines', 'slots', 'cpu_limit', 'ram_limit', 'url', 'token_env'}
    if set(worker) - allowed: raise ValueError('Unknown worker fields')
    with _locked():
        doc = _load(); workers = doc['workers']; existing = next((w for w in workers if w['id'] == worker.get('id')), None)
        if create and existing is not None:
            raise AlreadyRegistered(f"Worker {worker['id']} is already registered; edit it to add engines, pools or tags")
        updated = _normalize({**(existing or {}), **worker})
        updated['settings_revision'] = max(time.time_ns(), (existing or {}).get('settings_revision', 0) + 1)
        if updated.get('url'):
            u = urlsplit(updated['url'])
            tokens = {**dotenv_values(env_path()), **os.environ}
            if u.hostname not in ('127.0.0.1', 'localhost', '::1') and not tokens.get(updated.get('token_env', '')):
                raise ValueError(f"A network endpoint needs its token on the coordinator: set {updated.get('token_env') or 'a token_env variable'} "
                                 'in workers.local.env (or the environment) first, or save without an endpoint to wait for the connection')
        workers = [w for w in workers if w['id'] != updated['id']] + [updated]
        _write(workers, doc['tags'])
        return _public(updated)

def delete_worker(worker_id):
    with _locked():
        doc = _load(); workers = doc['workers']; remaining = [w for w in workers if w['id'] != worker_id]
        if len(workers) == len(remaining): raise ValueError('Unknown worker')
        _write(remaining, doc['tags'])
