"""One-line PC install: records, validation, the worker bundle, the scripts and the /install routes.

A user fills in the Fleet tab's form; `create` validates it and stores a one-time install record. The
PC runs one command that downloads the script for that record's code, installs the worker from the
bundle this server serves and calls back to `complete`, which registers the PC (a relayed PC: see
relay.py) with the settings from the form. The code is the only credential on these routes: 256 random
bits, single use, expiring, compared in constant time.
"""
import asyncio
import hashlib
import hmac
import io
import json
import os
import re
import secrets
import tarfile
import time
from pathlib import Path
from urllib.parse import urlsplit

from aiohttp import web

from .restart import atomic_json

ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = Path(__file__).resolve().parent / 'install_templates'
VALID_SECONDS = 30 * 60
KEEP_SECONDS = 24 * 3600  # finished records stay visible (and the code unusable) this long
NAME = re.compile(r'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$')
CODE = re.compile(r'^[A-Za-z0-9_-]{43}$')
OSES = ('windows', 'linux', 'macos')
ENGINES = ('hatchet', 'prefect', 'trigger')
MIN_TOKEN = 32
WRONG_CODE_DELAY = 0.5  # seconds a wrong code is held before the generic answer
STEPS = ('check', 'download', 'python', 'npm', 'token', 'service', 'register', 'connect')
FAILED_ATTEMPTS = {}  # source address -> (count, window start): wrong codes are slowed per source


class InstallError(ValueError):
    """A form or code problem the user can fix; the message is safe to show."""


def state_dir():
    return Path(os.environ.get('FLOW_STATE_DIR') or ROOT / '.flow-state').resolve() / 'installs'


def record_path(code):
    return state_dir() / f'{code}.json'


def now():
    return time.time()


# --------------------------------------------------------------------------- the form
def server_address(value):
    """The address the PC uses to reach this server: scheme://host[:port], never a loopback name."""
    if not isinstance(value, str) or not value.strip(): raise InstallError('Enter the address this PC uses to reach this server')
    parts = urlsplit(value.strip())
    if parts.scheme not in ('http', 'https') or not parts.hostname: raise InstallError('The server address must start with http:// or https:// and name a host')
    if parts.username or parts.password or parts.query or parts.fragment or parts.path not in ('', '/'):
        raise InstallError('The server address must be only scheme, host and port, for example http://192.168.1.10:8090')
    if parts.hostname.lower() in ('localhost', '127.0.0.1', '::1', '0.0.0.0') or parts.hostname.lower().endswith('.localhost'):
        raise InstallError('The PC cannot reach this server at localhost; use the name or address the PC can reach')
    try: port = parts.port
    except ValueError: raise InstallError('The server address has an invalid port')
    host = f'[{parts.hostname}]' if ':' in parts.hostname else parts.hostname
    return f'{parts.scheme}://{host}' + (f':{port}' if port else '')


def percent(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 < value <= 100: raise InstallError(f'{name} must be above 0 and at most 100')
    return value


def validate_form(body, registered=(), pending=()):
    """The normalized, checked install request. `registered` and `pending` are names already taken."""
    from .capacity import MAX_SLOTS
    from .fleet_registry import label
    if not isinstance(body, dict): raise InstallError('The install request must be an object')
    unknown = set(body) - {'id', 'os', 'engines', 'pool', 'tags', 'slots', 'cpu_limit', 'ram_limit', 'server'}
    if unknown: raise InstallError(f'Unknown fields: {", ".join(sorted(unknown))}')
    name = body.get('id')
    if not isinstance(name, str) or not NAME.match(name): raise InstallError('The name is 1 to 64 letters, digits, dashes or underscores, starting with a letter or digit')
    if name in set(registered): raise InstallError(f'{name} is already registered; edit it instead')
    if name in set(pending): raise InstallError(f'{name} already has an install link waiting; cancel it first')
    if body.get('os') not in OSES: raise InstallError('Choose the PC\'s operating system')
    engines = body.get('engines')
    if not isinstance(engines, list) or not engines or any(e not in ENGINES for e in engines): raise InstallError('Choose at least one engine')
    pool = body.get('pool')
    try:
        pool = label(pool, 'pool')
        tags = body.get('tags', [])
        if not isinstance(tags, list) or len(tags) > 50: raise ValueError('Tags must be a list of at most 50')
        tags = [label(t, 'tag') for t in tags]
    except ValueError as exc: raise InstallError(str(exc))
    slots = body.get('slots', 1)
    if isinstance(slots, bool) or not isinstance(slots, int) or not 1 <= slots <= MAX_SLOTS: raise InstallError(f'Slots must be a whole number from 1 to {MAX_SLOTS}')
    return {'id': name, 'os': body['os'], 'engines': [e for e in ENGINES if e in engines], 'pools': [pool], 'tags': list(dict.fromkeys(tags)), 'slots': slots,
            'cpu_limit': percent(body.get('cpu_limit', 90), 'The CPU limit'), 'ram_limit': percent(body.get('ram_limit', 90), 'The RAM limit'),
            'server': server_address(body.get('server'))}


def token_env_for(name, taken=()):
    base = 'FLOW_WORKER_TOKEN_' + re.sub(r'[^A-Za-z0-9]', '_', name).upper()
    candidate, n = base, 2
    while candidate in set(taken): candidate, n = f'{base}_{n}', n + 1
    return candidate


# --------------------------------------------------------------------------- records
def read_record(code):
    if not isinstance(code, str) or not CODE.match(code): return None
    try: return json.loads(record_path(code).read_text(encoding='utf-8'))
    except (OSError, ValueError): return None


def write_record(record):
    state_dir().mkdir(parents=True, exist_ok=True)
    atomic_json(record_path(record['code']), record)


def view(record):
    """What the Fleet tab may see: never the code's secret parts beyond the code itself, no token."""
    state = record['state']
    if state == 'pending' and record['expires_at'] <= now(): state = 'expired'
    return {'code': record['code'], 'id': record['id'], 'os': record['os'], 'state': state, 'created_at': record['created_at'], 'expires_at': record['expires_at'],
            'step': record.get('step'), 'error': record.get('error'), 'hostname': record.get('hostname'), 'registered_at': record.get('registered_at')}


def all_records():
    out = []
    try: paths = list(state_dir().glob('*.json'))
    except OSError: return out
    for path in paths:
        try: record = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError): continue
        if now() - record.get('expires_at', 0) > KEEP_SECONDS and record.get('state') != 'pending': path.unlink(missing_ok=True); continue
        out.append(record)
    return out


def pending_names():
    return {r['id'] for r in all_records() if r['state'] == 'pending' and r['expires_at'] > now()}


def create(body, registered=()):
    form = validate_form(body, registered, pending_names())
    record = {'code': secrets.token_urlsafe(32), 'state': 'pending', 'created_at': now(), 'expires_at': now() + VALID_SECONDS, **form}
    write_record(record)
    return record


def cancel(code):
    record = read_record(code)
    if record is None: raise InstallError('Unknown install link')
    if record['state'] == 'pending': record.update(state='cancelled', error=None); write_record(record)
    return view(record)


def commands(record, server=None):
    """The one command per shell. The link carries only the code; settings stay on the server."""
    base = f'{(server or record["server"]).rstrip("/")}/install/{record["code"]}'
    return {'powershell': f"irm '{base}/install.ps1' | iex",
            'cmd': f"powershell -NoProfile -ExecutionPolicy Bypass -Command \"irm '{base}/install.ps1' | iex\"",
            'bash': f"curl -fsSL '{base}/install.sh' | bash",
            'elevation': 'Run as a normal user. Do not use Administrator or sudo: everything installs for this user and nothing on the PC needs to be opened.'}


# --------------------------------------------------------------------------- the worker bundle
BUNDLE_FILES = ('flow_execution_worker.py', 'relay_connector.py', 'worker_runner.py', 'worker_ctl.py', 'requirements-worker.txt', 'package.json', 'package-lock.json')
BUNDLE_DIRS = ('flowkit', 'agentdriver')
_bundle = {'stamp': None, 'data': b'', 'version': ''}


def bundle_members():
    agents = ROOT / 'agents'
    for name in BUNDLE_FILES:
        if (agents / name).is_file(): yield agents / name
    for directory in BUNDLE_DIRS:
        for path in sorted((agents / directory).rglob('*')):
            if path.is_file() and '__pycache__' not in path.parts and path.suffix not in ('.pyc', '.pyo') and 'tests' not in path.relative_to(agents).parts: yield path


def build_bundle():
    members = list(bundle_members())
    stamp = tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) for p in members)
    if _bundle['stamp'] == stamp: return _bundle['data']
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w:gz', compresslevel=6) as archive:
        for path in members:
            info = archive.gettarinfo(str(path), arcname=f'agents/{path.relative_to(ROOT / "agents").as_posix()}')
            info.uid = info.gid = 0; info.uname = info.gname = ''
            with path.open('rb') as handle: archive.addfile(info, handle)
    digest = hashlib.sha256()
    for path in sorted(members, key=lambda p: p.relative_to(ROOT / 'agents').as_posix()):  # paths and contents only, never timestamps (worker_ctl.hash_tree does the same)
        digest.update(path.relative_to(ROOT / 'agents').as_posix().encode() + b'\x00' + hashlib.sha256(path.read_bytes()).digest())
    _bundle.update(stamp=stamp, data=buffer.getvalue(), version=digest.hexdigest()[:16])
    return _bundle['data']


def bundle_version():
    """The worker version: a short hash of what the bundle contains. A PC reports the one it installed."""
    build_bundle()
    return _bundle['version']


# --------------------------------------------------------------------------- scripts
def sh_quote(value):
    return "'" + str(value).replace("'", "'\\''") + "'"


def ps_quote(value):
    return "'" + str(value).replace("'", "''") + "'"


def render(kind, record):
    """The installer script for a record: the template with this install's values filled in."""
    template = (TEMPLATES / ('install.ps1' if kind == 'ps1' else 'install.sh')).read_text(encoding='utf-8')
    quote = ps_quote if kind == 'ps1' else sh_quote
    values = {'SERVER': record['server'], 'CODE': record['code'], 'NAME': record['id'], 'SLOTS': str(record['slots']), 'OS': record['os']}
    template = template.replace('@@NAME_RAW@@', record['id'])  # validated to letters, digits, dash and underscore
    for key, value in values.items(): template = template.replace(f'@@{key}@@', quote(value))
    text = template.replace('\r\n', '\n')
    return text.replace('\n', '\r\n') if kind == 'ps1' else text


# --------------------------------------------------------------------------- registration
def set_env_value(key, value, path=None):
    """Write NAME=value to the coordinator's workers.local.env (git-ignored, readable by its owner only)."""
    from .fleet_registry import _locked
    from .fleet_registry import env_path
    path = Path(path or env_path())
    with _locked():
        lines = path.read_text(encoding='utf-8').splitlines() if path.exists() else []
        lines = [l for l in lines if not l.startswith(f'{key}=')] + [f'{key}={value}']
        temporary = path.with_name(path.name + '.tmp')
        fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as handle: handle.write('\n'.join(lines) + '\n')
        os.replace(temporary, path)


def register(record, token, hostname=None):
    """Store the PC's token and register it as a relayed worker. Raises InstallError for a refusal."""
    from .fleet_registry import AlreadyRegistered, _read, save_worker
    from .relay import worker_url
    if not isinstance(token, str) or len(token) < MIN_TOKEN or len(token) > 200 or not re.fullmatch(r'[A-Za-z0-9_-]+', token): raise InstallError('The PC sent an invalid token')
    taken = {w.get('token_env') for w in _read() if w.get('token_env')}
    env = token_env_for(record['id'], taken)
    set_env_value(env, token)
    try:
        save_worker({'id': record['id'], 'pools': record['pools'], 'tags': record['tags'], 'engines': record['engines'], 'slots': record['slots'],
                     'cpu_limit': record['cpu_limit'], 'ram_limit': record['ram_limit'], 'url': worker_url(record['id']), 'token_env': env}, create=True)
    except AlreadyRegistered as exc: raise InstallError(str(exc))
    return env


# --------------------------------------------------------------------------- routes
def json_error(status, message):
    return web.json_response({'error': message}, status=status)


async def usable(request, require_pending=True):
    """The record for the request's code, or a generic 404 (slowed after repeated misses from one source)."""
    source = request.headers.get('X-Relay-Client') or request.remote or '?'  # the board proxy names the real peer
    count, since = FAILED_ATTEMPTS.get(source, (0, now()))
    if now() - since > 600: count, since = 0, now()
    if count >= 10: raise web.HTTPTooManyRequests(text=json.dumps({'error': 'Too many wrong links'}), content_type='application/json')
    record = read_record(request.match_info['code'])
    ok = record is not None and (not require_pending or (record['state'] == 'pending' and record['expires_at'] > now()))
    if ok and hmac.compare_digest(record['code'], request.match_info['code']): return record
    FAILED_ATTEMPTS[source] = (count + 1, since)
    await asyncio.sleep(WRONG_CODE_DELAY)
    raise web.HTTPNotFound(text=json.dumps({'error': 'This install link is unknown, expired or already used. Create a new one in the Fleet tab.'}), content_type='application/json')


async def script(request):
    record = await usable(request)
    kind = request.match_info['kind']
    body = render('ps1' if kind == 'ps1' else 'sh', record)
    return web.Response(text=body, content_type='text/plain', charset='utf-8', headers={'Cache-Control': 'no-store'})


async def bundle(request):
    await usable(request)
    data = await asyncio.to_thread(build_bundle)
    return web.Response(body=data, content_type='application/gzip', headers={'Cache-Control': 'no-store'})


async def progress(request):
    record = await usable(request)
    try: body = await request.json()
    except ValueError: return json_error(400, 'Invalid JSON')
    step, state = body.get('step') if isinstance(body, dict) else None, body.get('state') if isinstance(body, dict) else None
    if step not in STEPS: return json_error(400, 'Unknown step')
    record['step'] = step
    if state == 'failed':
        record['error'] = str(body.get('message') or 'The install failed')[:500]
    else: record['error'] = None
    write_record(record)
    return web.Response(status=204)


async def complete(request):
    record = await usable(request)
    try: body = await request.json()
    except ValueError: return json_error(400, 'Invalid JSON')
    if not isinstance(body, dict): return json_error(400, 'Invalid request')
    try: await asyncio.to_thread(register, record, body.get('token'), body.get('hostname'))
    except InstallError as exc:
        record.update(error=str(exc)); write_record(record)
        return json_error(409, str(exc))
    record.update(state='registered', step='register', error=None, registered_at=now(), hostname=str(body.get('hostname') or '')[:100])
    write_record(record)
    return web.json_response({'ok': True, 'id': record['id']})


async def state(request):
    """The PC (and the Fleet tab, through the flow host) asks whether the relay connection is up."""
    record = await usable(request, require_pending=False)
    relay = request.app['relay']
    peer = relay.peers.get(record['id'])
    return web.json_response({**view(record), 'connected': bool(peer and peer.online())})


def add_routes(app):
    app.router.add_get('/install/{code}/install.{kind:ps1|sh}', script)
    app.router.add_get('/install/{code}/bundle.tgz', bundle)
    app.router.add_post('/install/{code}/progress', progress)
    app.router.add_post('/install/{code}/complete', complete)
    app.router.add_get('/install/{code}/state', state)
