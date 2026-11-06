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
CODE = install.CODE


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
    """Make the current bundle the fleet's target; connected PCs update within seconds, others when they connect."""
    version = install.bundle_version()
    state = read_state()
    if state.get('target') != version:
        state = {'target': version, 'auto': True, 'started_at': time.time()}
    else:
        state.update(auto=True)
    write_state(state)
    return state


def stop_fleet_update():
    state = read_state()
    state['auto'] = False
    write_state(state)
    return state


def target():
    """The version PCs are being brought to, or None when no fleet update is active."""
    state = read_state()
    return state.get('target') if state.get('auto') else None


# --------------------------------------------------------------------------- links for PCs that are down
def create_link(worker_id, os_name):
    if not install.NAME.match(str(worker_id)): raise install.InstallError('Unknown PC')
    if os_name not in install.OSES: raise install.InstallError("Choose the PC's operating system")
    record = {'code': secrets.token_urlsafe(32), 'id': worker_id, 'os': os_name, 'state': 'pending', 'created_at': time.time(),
              'expires_at': time.time() + LINK_VALID_SECONDS, 'version': install.bundle_version()}
    links_dir().mkdir(parents=True, exist_ok=True)
    atomic_json(links_dir() / f"{record['code']}.json", record)
    return record


def read_link(code):
    if not isinstance(code, str) or not CODE.match(code): return None
    try: return json.loads((links_dir() / f'{code}.json').read_text(encoding='utf-8'))
    except (OSError, ValueError): return None


def link_commands(record, server):
    base = f'{server.rstrip("/")}/update/{record["code"]}'
    return {'powershell': f"irm '{base}/update.ps1' | iex",
            'cmd': f"powershell -NoProfile -ExecutionPolicy Bypass -Command \"irm '{base}/update.ps1' | iex\"",
            'bash': f"curl -fsSL '{base}/update.sh' | bash",
            'local': 'agent-worker update'}


def render_link_script(kind, record, server):
    name = 'update.ps1' if kind == 'ps1' else 'update.sh'
    quote = install.ps_quote if kind == 'ps1' else install.sh_quote
    text = (TEMPLATES / name).read_text(encoding='utf-8')
    for key, value in {'SERVER': server.rstrip('/'), 'CODE': record['code'], 'NAME': record['id'], 'VERSION': record['version']}.items():
        text = text.replace(f'@@{key}@@', quote(value))
    text = text.replace('\r\n', '\n')
    return text.replace('\n', '\r\n') if kind == 'ps1' else text


# --------------------------------------------------------------------------- routes
def server_for(request):
    """The address a PC used to reach this server, as the board proxy saw it (scheme://host)."""
    host = request.headers.get('X-Relay-Host') or request.host
    scheme = request.headers.get('X-Relay-Scheme') or request.scheme
    return f'{scheme}://{host}'


def authorized_pc(request):
    from .relay import authorized
    if not authorized(request.app['relay'], request, request.match_info['id']):
        raise web.HTTPUnauthorized(text=json.dumps({'error': 'Unauthorized'}), content_type='application/json')


async def pc_version(request):
    authorized_pc(request)
    return web.json_response({'version': await asyncio.to_thread(install.bundle_version)})


async def pc_bundle(request):
    authorized_pc(request)
    data = await asyncio.to_thread(install.build_bundle)
    return web.Response(body=data, content_type='application/gzip', headers={'X-Worker-Version': await asyncio.to_thread(install.bundle_version), 'Cache-Control': 'no-store'})


async def pc_deregister(request):
    """`agent-worker uninstall`: the PC asks to be removed from the fleet, authenticated by its own token."""
    authorized_pc(request)
    worker_id = request.match_info['id']
    from .fleet_registry import _locked, delete_worker
    def remove():
        try: delete_worker(worker_id)
        except ValueError: return False
        return True
    removed = await asyncio.to_thread(remove)
    peer = request.app['relay'].peers.pop(worker_id, None)
    if peer is not None:
        for call in list(peer.pending): call.closed = True
    return web.json_response({'removed': removed})


async def link_record(request):
    record = read_link(request.match_info['code'])
    ok = record is not None and record['state'] == 'pending' and record['expires_at'] > time.time()
    if ok: return record
    await asyncio.sleep(install.WRONG_CODE_DELAY)
    raise web.HTTPNotFound(text=json.dumps({'error': 'This update link is unknown, expired or already used. Create a new one in the Fleet tab.'}), content_type='application/json')


async def link_script(request):
    record = await link_record(request)
    return web.Response(text=render_link_script('ps1' if request.match_info['kind'] == 'ps1' else 'sh', record, server_for(request)), content_type='text/plain', charset='utf-8', headers={'Cache-Control': 'no-store'})


async def link_bundle(request):
    record = await link_record(request)
    data = await asyncio.to_thread(install.build_bundle)
    return web.Response(body=data, content_type='application/gzip', headers={'X-Worker-Version': record['version'], 'Cache-Control': 'no-store'})


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
    goal = target()
    if not goal: return
    version = str(status.get('worker_version') or '')
    peer = app['relay'].peer(worker_id)
    if version == goal or status.get('draining') is None: return
    if status.get('update_state') == 'running': return
    now = time.monotonic()
    if now - getattr(peer, 'update_asked_at', 0.0) < RETRY_AFTER_SECONDS: return
    if getattr(peer, 'update_attempts', 0) >= MAX_ATTEMPTS: return
    peer.update_asked_at = now
    peer.update_attempts = getattr(peer, 'update_attempts', 0) + 1
    app['relay'].queue_control(worker_id, 'update')


def add_routes(app):
    from .relay import Relay  # noqa: F401  (the relay app must exist)
    app.router.add_get('/relay/{id}/update/version', pc_version)
    app.router.add_get('/relay/{id}/update/bundle.tgz', pc_bundle)
    app.router.add_delete('/relay/{id}/registration', pc_deregister)
    app.router.add_get('/update/{code}/update.{kind:ps1|sh}', link_script)
    app.router.add_get('/update/{code}/bundle.tgz', link_bundle)
    app.router.add_post('/update/{code}/done', link_done)
    app['on_status'] = on_status


def view(relay_state):
    """The fleet's update picture: the target and, per relayed PC, its version and what happens next."""
    from .fleet_registry import _read
    from .relay import is_relayed
    goal = read_state()
    latest = install.bundle_version()
    out = []
    for worker in _read():
        if not is_relayed(worker.get('url')): continue
        live = (relay_state or {}).get(worker['id'], {})
        version = live.get('version')
        online = bool(live.get('online'))
        if version == latest: status = 'up_to_date'
        elif live.get('update_state') == 'running': status = 'updating'
        elif live.get('update_state') == 'failed': status = 'failed'
        elif not online: status = 'waiting' if goal.get('auto') else 'offline'
        elif goal.get('auto'): status = 'failed' if live.get('attempts', 0) >= MAX_ATTEMPTS else 'updating'
        else: status = 'outdated'
        out.append({'id': worker['id'], 'version': version, 'online': online, 'status': status, 'error': live.get('update_error')})
    return {'latest': latest, 'auto': bool(goal.get('auto')), 'started_at': goal.get('started_at'), 'workers': out}
