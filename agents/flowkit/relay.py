"""Outbound-only relay: a PC connects out to this server and the coordinator reaches the PC through it.

The coordinator's code treats `worker['url']` as the PC's HTTP address. For a relayed PC that address
is `<relay>/w/<worker id>`: a call there is queued, a connector on the PC collects it with a long poll,
makes it against the PC's own localhost execution service and sends the answer back. Request and
response bodies stream through the relay and are never buffered (job bodies reach 190 MB).

An unreachable PC is reported by dropping the coordinator's connection, never with an error status:
the coordinator already reads a dropped connection as "worker offline" and a status as a failure.
"""
import asyncio
import hmac
import json
import os
import time
from collections import deque
from pathlib import Path

from aiohttp import web

ROOT = Path(__file__).resolve().parents[2]
POLL_WAIT = 25.0  # how long an idle long poll is held before it returns empty
ONLINE_WINDOW = 40.0  # a PC that has not polled for this long is offline
PICKUP_TIMEOUT = 30.0  # a queued call nobody collected within this long fails as offline
RESPONSE_TIMEOUT = 125.0  # the coordinator's own client gives up after 120 s
STATUS_FRESH = 15.0  # a pushed /health answer younger than this is served without a round trip
MAX_BATCH = 16
BODY_CHUNK = 64 * 1024
FORWARD = ('authorization', 'content-type', 'accept')  # request headers handed to the PC's service
MAX_BODY = 200 * 1024 * 1024

DEFAULT_PORT = 8097


def relay_base():
    """Where coordinators reach the relay (loopback; the board proxies PCs to the same listener)."""
    return os.environ.get('FLOW_RELAY_URL') or f"http://127.0.0.1:{os.environ.get('FLOW_RELAY_PORT', DEFAULT_PORT)}"


def worker_url(worker_id):
    return f'{relay_base().rstrip("/")}/w/{worker_id}'


def is_relayed(url):
    """True for a registry URL that points at this machine's relay."""
    from urllib.parse import urlsplit
    if not url: return False
    parts, base = urlsplit(url), urlsplit(relay_base())
    return (parts.hostname, parts.port) == (base.hostname, base.port) and parts.path.startswith('/w/')


class Call:
    """One coordinator request waiting for a connector."""
    def __init__(self, method, path, query, headers, source):
        self.id = os.urandom(8).hex()
        self.method, self.path, self.query, self.headers = method, path, query, headers
        self.source = source  # the coordinator's request: its body is piped to the connector
        self.has_body = bool(source.can_read_body and source.content_length != 0)
        self.picked = asyncio.Event()
        self.response = asyncio.get_running_loop().create_future()  # (status, headers, connector request)
        self.finished = asyncio.Event()  # the coordinator's response has been written out
        self.closed = False  # the coordinator went away or gave up


class Peer:
    def __init__(self):
        self.pending = deque()
        self.calls = {}
        self.wake = asyncio.Event()
        self.holding = 0
        self.last_poll = 0.0
        self.status, self.status_at = None, 0.0

    def online(self, now=None):
        return self.holding > 0 or (self.last_poll and (now or time.monotonic()) - self.last_poll < ONLINE_WINDOW)

    def fresh_status(self, now=None):
        return self.status if self.status is not None and (now or time.monotonic()) - self.status_at < STATUS_FRESH and self.online(now) else None


class Relay:
    def __init__(self, token_for):
        self.peers = {}
        self.closing = False  # held long polls return at once so the service can stop
        self.token_for = token_for  # worker id -> token (None when unknown)

    def peer(self, worker_id):
        return self.peers.setdefault(worker_id, Peer())

    def state(self):
        now = time.monotonic()
        return {wid: {'online': bool(p.online(now)), 'last_poll_seconds': None if not p.last_poll else round(now - p.last_poll, 1), 'queued': len(p.pending)}
                for wid, p in self.peers.items()}


def registry_token_for():
    """Tokens come from the registry's `token_env`, resolved from workers.local.env and the environment."""
    from dotenv import dotenv_values
    from .fleet_registry import _read, env_path
    cache = {'at': 0.0, 'tokens': {}}
    def lookup(worker_id):
        now = time.monotonic()
        if now - cache['at'] > 3.0:
            try:
                values = {**dotenv_values(env_path()), **os.environ}
                cache['tokens'] = {w['id']: values.get(w.get('token_env', ''), '') for w in _read()}
            except (OSError, ValueError): cache['tokens'] = {}
            cache['at'] = now
        return cache['tokens'].get(worker_id) or None
    return lookup


def bearer(request):
    value = request.headers.get('Authorization', '')
    return value[7:] if value.startswith('Bearer ') else ''


def authorized(relay, request, worker_id):
    token = relay.token_for(worker_id)
    return bool(token) and hmac.compare_digest(bearer(request).encode(), token.encode())


def drop(request):
    """Report the PC as unreachable by closing the coordinator's connection without answering."""
    if request.transport is not None: request.transport.abort()
    return web.Response(status=502)


def json_error(status, message):
    return web.json_response({'error': message}, status=status)


# --------------------------------------------------------------------------- coordinator face
async def coordinator_call(request):
    relay, worker_id = request.app['relay'], request.match_info['id']
    if not authorized(relay, request, worker_id): return json_error(401, 'Unauthorized')
    peer = relay.peer(worker_id)
    tail = request.match_info.get('tail', '')
    if request.method == 'GET' and tail == 'health':
        cached = peer.fresh_status()
        if cached is not None: return web.json_response(cached)
    if not peer.online(): return drop(request)
    headers = {k: v for k, v in request.headers.items() if k.lower() in FORWARD}
    call = Call(request.method, '/' + tail, request.query_string, headers, request)
    peer.calls[call.id] = call
    peer.pending.append(call)
    peer.wake.set()
    try:
        try: await asyncio.wait_for(call.picked.wait(), PICKUP_TIMEOUT)
        except asyncio.TimeoutError: return drop(request)
        try: answer = await asyncio.wait_for(asyncio.shield(call.response), RESPONSE_TIMEOUT)
        except asyncio.TimeoutError: return drop(request)
        if answer is None: return drop(request)  # the connector could not reach the PC's own service
        status, response_headers, source = answer
        response = web.StreamResponse(status=status, headers=response_headers)
        await response.prepare(request)
        async for chunk in source.content.iter_chunked(BODY_CHUNK): await response.write(chunk)
        await response.write_eof()
        return response
    finally:
        call.closed = True
        call.finished.set()
        peer.calls.pop(call.id, None)
        if not call.response.done(): call.response.cancel()


# --------------------------------------------------------------------------- PC face
def pc_peer(request):
    relay, worker_id = request.app['relay'], request.match_info['id']
    if not authorized(relay, request, worker_id): raise web.HTTPUnauthorized(text=json.dumps({'error': 'Unauthorized'}), content_type='application/json')
    return relay.peer(worker_id)


async def next_calls(request):
    peer = pc_peer(request)
    try: wait = min(max(float(request.query.get('wait', POLL_WAIT)), 0.0), 30.0)
    except ValueError: wait = POLL_WAIT
    peer.last_poll = time.monotonic()
    peer.holding += 1
    try:
        deadline = time.monotonic() + wait
        while True:
            batch = []
            while peer.pending and len(batch) < MAX_BATCH:
                call = peer.pending.popleft()
                if not call.closed: batch.append(call)
            if batch: break
            remaining = deadline - time.monotonic()
            if remaining <= 0 or request.app['relay'].closing: return web.Response(status=204)
            peer.wake.clear()
            try: await asyncio.wait_for(peer.wake.wait(), remaining)
            except asyncio.TimeoutError: return web.Response(status=204)
        for call in batch: call.picked.set()
        return web.json_response([{'id': c.id, 'method': c.method, 'path': c.path, 'query': c.query, 'headers': c.headers, 'has_body': c.has_body} for c in batch])
    finally:
        peer.holding -= 1
        peer.last_poll = time.monotonic()


def find_call(request):
    peer = pc_peer(request)
    call = peer.calls.get(request.match_info['cid'])
    if call is None or call.closed: raise web.HTTPGone(text=json.dumps({'error': 'The call is no longer wanted'}), content_type='application/json')
    return call


async def call_body(request):
    call = find_call(request)
    response = web.StreamResponse()
    await response.prepare(request)
    async for chunk in call.source.content.iter_chunked(BODY_CHUNK): await response.write(chunk)
    await response.write_eof()
    return response


async def call_response(request):
    call = find_call(request)
    if request.headers.get('X-Relay-Drop'):
        if not call.response.done(): call.response.set_result(None)
        return web.Response(status=204)
    try: status = int(request.headers.get('X-Relay-Status', '200'))
    except ValueError: status = 502
    headers = {k: v for k, v in request.headers.items() if k.lower() in ('content-type',)}
    if not call.response.done(): call.response.set_result((status, headers, request))
    await call.finished.wait()  # the body is piped from this request while the coordinator's response is written
    return web.Response(status=204)


async def push_status(request):
    peer = pc_peer(request)
    try: body = await request.json()
    except ValueError: return json_error(400, 'Invalid JSON')
    if not isinstance(body, dict): return json_error(400, 'Status must be an object')
    peer.status, peer.status_at = body, time.monotonic()
    peer.last_poll = peer.last_poll or time.monotonic()
    return web.Response(status=204)


async def relay_state(request):
    return web.json_response(request.app['relay'].state())


def make_app(token_for=None, install=None):
    """`install`: a callable that adds the /install routes (see flowkit.install)."""
    app = web.Application(client_max_size=MAX_BODY)
    app['relay'] = Relay(token_for or registry_token_for())
    async def release_polls(app):
        app['relay'].closing = True
        for peer in app['relay'].peers.values(): peer.wake.set()
    app.on_shutdown.append(release_polls)
    app.router.add_route('*', '/w/{id}/{tail:.*}', coordinator_call)
    app.router.add_get('/relay/{id}/next', next_calls)
    app.router.add_get('/relay/{id}/calls/{cid}/body', call_body)
    app.router.add_put('/relay/{id}/calls/{cid}/response', call_response)
    app.router.add_post('/relay/{id}/status', push_status)
    app.router.add_get('/relay-state', relay_state)
    if install is not None: install(app)
    return app
