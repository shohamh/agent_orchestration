"""Updating relayed PCs: the worker version, the fleet-wide target, update links and the /update routes.

The worker version is a hash of the bundle's files (install.bundle_version). `start_fleet_update` records it as
the fleet's target. A connected PC that reports another version is told to update by the relay (a control call its
connector runs as `agent-worker update`); a PC that is down updates itself the next time it connects, or its user
can run `agent-worker update` or paste an update link, which works without the service running.

An installed PC fetches the new bundle with its own token (`/relay/<id>/update/...`); a link carries a one-time
code instead (`/update/<code>/...`), so it also works when the PC's installed command is broken.
"""
import asyncio
import hashlib
import json
import os
import re
import secrets
import time
from pathlib import Path

from aiohttp import web

from . import install
from .restart import atomic_json

LINK_VALID_SECONDS = 30 * 60
RETRY_AFTER_SECONDS = 600  # a PC still on the old version this long after being told to update is told again
MAX_ATTEMPTS = 3
TEMPLATES = Path(__file__).resolve().parent / 'update_templates'
CODE = re.compile(r'[A-Za-z0-9_-]{43}')  # the 256-bit URL-safe update credential, not an install UUID


def state_file():
    return Path(os.environ.get('FLOW_STATE_DIR') or install.ROOT / '.flow-state').resolve() / 'updates.json'


def links_dir():
    return Path(os.environ.get('FLOW_STATE_DIR') or install.ROOT / '.flow-state').resolve() / 'update-links'


def read_state():
    try:
        data = json.loads(state_file().read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError): return {}


def write_state(data):
    state_file().parent.mkdir(parents=True, exist_ok=True)
    atomic_json(state_file(), data)


# --------------------------------------------------------------------------- fleet target
def start_fleet_update():
    """Make the server's current workers the fleet's target; connected PCs update within seconds, others when they connect."""
    versions = {name: install.binary_version(name) for name in install.TARGETS}
    state = read_state()
    if state.get('targets') != versions:
        state = {'targets': versions, 'auto': True, 'started_at': time.time()}
    else:
        state.update(auto=True)
    write_state(state)
    return state


def stop_fleet_update():
    state = read_state()
    state['auto'] = False
    write_state(state)
    return state


def target(os_name):
    """The version PCs of this OS are being brought to, or None when no fleet update is active."""
    state = read_state()
    return (state.get('targets') or {}).get(os_name) if state.get('auto') else None


# --------------------------------------------------------------------------- links for PCs that are down
def create_link(worker_id, os_name):
    if not install.NAME.match(str(worker_id)): raise install.InstallError('Unknown PC')
    if os_name not in install.OSES: raise install.InstallError("Choose the PC's operating system")
    if install.binary_path(os_name) is None: raise install.InstallError(f'This server has no worker program for {os_name} yet')
    record = {'code': secrets.token_urlsafe(32), 'id': worker_id, 'os': os_name, 'state': 'pending', 'created_at': time.time(),
              'expires_at': time.time() + LINK_VALID_SECONDS, 'version': install.binary_version(os_name)}
    links_dir().mkdir(parents=True, exist_ok=True)
    atomic_json(links_dir() / f"{record['code']}.json", record)
    return record


def read_link(code):
    if not isinstance(code, str) or not CODE.fullmatch(code): return None
    try: return json.loads((links_dir() / f'{code}.json').read_text(encoding='utf-8'))
    except (OSError, ValueError): return None


def link_commands(record, server):
    """For a PC that cannot update itself (it was down, or its installed program is too old): download the new worker
    program and let it install itself over the old one. `agent-worker update` does the same from an installed worker."""
    origin = server.rstrip("/")
    link = f'{origin}/update/{record["code"]}/agent-worker'
    return {'powershell': f"$f = Join-Path $env:TEMP 'agent-worker-update.exe'; iwr -UseBasicParsing '{link}' -OutFile $f; & $f update --server '{origin}' --code '{record['code']}' --force",
            'cmd': f'curl.exe -fsSL "{link}" -o "%TEMP%\\agent-worker-update.exe" && "%TEMP%\\agent-worker-update.exe" update --server "{origin}" --code "{record["code"]}" --force',
            'bash': f"f=$(mktemp) && curl -fsSL '{link}' -o \"$f\" && chmod +x \"$f\" && \"$f\" update --server '{origin}' --code '{record['code']}' --force",
            'local': 'agent-worker update'}


# --------------------------------------------------------------------------- routes
def server_for(request):
    """The address a PC used to reach this server, as the board proxy saw it (scheme://host)."""
    host = request.headers.get('X-Relay-Host') or request.host
    scheme = request.headers.get('X-Relay-Scheme') or request.scheme
    return f'{scheme}://{host}'


def authorized_pc(request):
    from .relay import authorization_status, revoked_payload
    status = authorization_status(request.app['relay'], request, request.match_info['id'])
    if status == 'revoked':
        raise web.HTTPGone(text=json.dumps(revoked_payload()), content_type='application/json')
    if status != 'active':
        raise web.HTTPUnauthorized(text=json.dumps({'error': 'Unauthorized'}), content_type='application/json')


def requested_os(request):
    name = request.query.get('os', '')
    if name not in install.TARGETS or request.query.get('arch', 'x86_64') != 'x86_64':
        raise web.HTTPBadRequest(text=json.dumps({'error': 'Only x86_64 workers for windows and linux are built'}), content_type='application/json')
    return name


async def pc_version(request):
    authorized_pc(request)
    return web.json_response({'version': await asyncio.to_thread(install.binary_version, requested_os(request))})


async def pc_binary(request):
    authorized_pc(request)
    name = requested_os(request)
    path = install.binary_path(name)
    if path is None: raise web.HTTPNotFound(text=json.dumps({'error': f'This server has no worker program for {name} yet'}), content_type='application/json')
    return web.Response(body=await asyncio.to_thread(path.read_bytes), content_type='application/octet-stream', headers={'Content-Disposition': f'attachment; filename="{path.name}"', 'X-Worker-Version': install.binary_version(name), 'Cache-Control': 'no-store'})


async def pc_confluence_runtime(request):
    """A PC's agent-worker fetches this the first time a run needs Confluence; `have` skips the download."""
    authorized_pc(request)
    name = requested_os(request)
    path = install.runtime_path(name)
    if path is None: raise web.HTTPNotFound(text=json.dumps({'error': f'This server has no Confluence runtime for {name} yet'}), content_type='application/json')
    version = await asyncio.to_thread(install.runtime_version, name)
    if request.query.get('have') == version: return web.Response(status=304)
    return web.FileResponse(path, headers={'Content-Type': 'application/octet-stream', 'X-Runtime-Version': version})


async def pc_deregister(request):
    """`agent-worker uninstall`: the PC asks to be removed from the fleet, authenticated by its own token."""
    authorized_pc(request)
    key = request.match_info['id']
    from .fleet_registry import unregister_worker
    removed = await asyncio.to_thread(unregister_worker, relay_id=key)
    request.app['relay'].revoke_peer(key)
    return web.json_response({'removed': removed is not None})


async def link_record(request):
    record = read_link(request.match_info['code'])
    ok = record is not None and record['state'] == 'pending' and record['expires_at'] > time.time()
    if ok: return record
    await asyncio.sleep(install.WRONG_CODE_DELAY)
    raise web.HTTPNotFound(text=json.dumps({'error': 'This update link is unknown, expired or already used. Create a new one in the Fleet tab.'}), content_type='application/json')


async def link_binary(request):
    record = await link_record(request)
    path = install.binary_path(record['os'])
    if path is None: raise web.HTTPNotFound(text=json.dumps({'error': f"This server has no worker program for {record['os']} yet"}), content_type='application/json')
    return web.Response(body=await asyncio.to_thread(path.read_bytes), content_type='application/octet-stream', headers={'Content-Disposition': f'attachment; filename="{path.name}"', 'X-Worker-Version': install.binary_version(record['os']), 'Cache-Control': 'no-store'})


async def link_done(request):
    record = await link_record(request)
    try: body = await request.json()
    except ValueError: return web.json_response({'error': 'Invalid JSON'}, status=400)
    record.update(state='done' if body.get('ok') else 'failed', error=str(body.get('error') or '')[:300] or None, finished_at=time.time())
    atomic_json(links_dir() / f"{record['code']}.json", record)
    return web.Response(status=204)


# --------------------------------------------------------------------------- auto update from the relay
def on_status(app, worker_id, status):
    """Called by the relay when a PC pushes its status: tell it to update if the fleet target says so."""
    goal = target(status.get('worker_os'))
    if not goal: return
    version = str(status.get('worker_version') or '')
    peer = app['relay'].peer(worker_id)
    if version == goal or status.get('draining') is None: return
    if status.get('update_state') == 'running': return
    if getattr(peer, 'update_target', None) != goal:
        peer.update_target, peer.update_attempts, peer.update_asked_at = goal, 0, None
    now = time.monotonic()
    asked_at = getattr(peer, 'update_asked_at', None)
    if asked_at is not None and now - asked_at < RETRY_AFTER_SECONDS: return
    if getattr(peer, 'update_attempts', 0) >= MAX_ATTEMPTS: return
    peer.update_asked_at = now
    peer.update_attempts = getattr(peer, 'update_attempts', 0) + 1
    app['relay'].queue_control(worker_id, 'update')


def add_routes(app):
    from .relay import Relay  # noqa: F401  (the relay app must exist)
    app.router.add_get('/relay/{id}/update/version', pc_version)
    app.router.add_get('/relay/{id}/update/binary', pc_binary)
    app.router.add_get('/relay/{id}/update/confluence-runtime', pc_confluence_runtime)
    app.router.add_delete('/relay/{id}/registration', pc_deregister)
    app.router.add_get('/update/{code}/agent-worker', link_binary)
    app.router.add_post('/update/{code}/done', link_done)
    app['on_status'] = on_status


def view(relay_state):
    """The fleet's update picture: the target and, per relayed PC, its version and what happens next."""
    from .fleet_registry import _read, relay_key
    from .relay import is_relayed
    goal = read_state()
    latest = {name: install.binary_version(name) for name in install.TARGETS}
    out = []
    for worker in _read():
        if not is_relayed(worker.get('url')): continue
        live = (relay_state or {}).get(relay_key(worker), {})
        version = live.get('version')
        online = bool(live.get('online'))
        if version and version == latest.get(live.get('os')): status = 'up_to_date'
        elif live.get('update_state') == 'running': status = 'updating'
        elif live.get('update_state') == 'failed': status = 'failed'
        elif not online: status = 'waiting' if goal.get('auto') else 'offline'
        elif goal.get('auto'): status = 'failed' if live.get('attempts', 0) >= MAX_ATTEMPTS else 'updating'
        else: status = 'outdated'
        out.append({'id': worker['id'], 'version': version, 'os': live.get('os'), 'online': online, 'status': status, 'error': live.get('update_error')})
    return {'latest': latest, 'auto': bool(goal.get('auto')), 'started_at': goal.get('started_at'), 'workers': out}
