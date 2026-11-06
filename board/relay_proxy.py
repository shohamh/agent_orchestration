"""Passes a PC's install, update and relay calls to the relay service on the coordinator.

The PCs can reach only this board, so `/install/*` (the one-line installer), `/update/*` (one-time
update links) and `/relay/*` (the outbound connector's long poll) are proxied to the relay (env
FLOW_RELAY_URL, default http://host.docker.internal:8097). These paths skip the board login: they
authenticate with the install code, update code or PC's token, which the relay checks. Bodies stream
in both directions; a job's workspace can be 190 MB, so nothing is buffered and a request body is capped at MAX_BODY.
"""
import os

import aiohttp
from aiohttp import web

MAX_BODY = 200 * 1024 * 1024
CHUNK = 64 * 1024
FORWARD_REQUEST = ('authorization', 'content-type', 'accept', 'x-relay-status', 'x-relay-drop')
FORWARD_RESPONSE = ('content-type', 'content-disposition', 'cache-control', 'x-worker-version')
PREFIXES = ('/install/', '/update/', '/relay/')


def relay_url() -> str:
    return os.environ.get('FLOW_RELAY_URL', 'http://host.docker.internal:8097').rstrip('/')


async def capped(stream):
    """The request body in chunks, refusing more than MAX_BODY bytes."""
    limit, total = MAX_BODY, 0
    async for chunk in stream.iter_chunked(CHUNK):
        total += len(chunk)
        if total > limit:
            raise web.HTTPRequestEntityTooLarge(max_size=limit, actual_size=total)
        yield chunk


async def proxy(request: web.Request) -> web.StreamResponse:
    session: aiohttp.ClientSession = request.app['relay_session']
    headers = {k: v for k, v in request.headers.items() if k.lower() in FORWARD_REQUEST}
    # Only the board knows the real peer; the relay rate-limits wrong install codes by it.
    headers['X-Relay-Client'] = request.remote or ''
    has_body = request.can_read_body and request.content_length != 0
    try:
        async with session.request(request.method, relay_url() + request.rel_url.raw_path_qs, headers=headers,
                                   data=capped(request.content) if has_body else None) as upstream:
            response = web.StreamResponse(status=upstream.status, headers={k: v for k, v in upstream.headers.items() if k.lower() in FORWARD_RESPONSE})
            await response.prepare(request)
            async for chunk in upstream.content.iter_chunked(CHUNK):
                await response.write(chunk)
            await response.write_eof()
            return response
    except aiohttp.ClientError:
        return web.json_response({'error': 'The server could not reach its relay service; start the flow host on the coordinator'}, status=502)


def install(app: web.Application) -> None:
    async def open_session(app: web.Application) -> None:
        # No total timeout: a long poll is held open on purpose.
        app['relay_session'] = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, connect=3, sock_read=75), auto_decompress=False)

    async def close_session(app: web.Application) -> None:
        await app['relay_session'].close()

    app.on_startup.append(open_session)
    app.on_cleanup.append(close_session)
    for prefix in PREFIXES:
        app.router.add_route('*', prefix + '{tail:.*}', proxy)
