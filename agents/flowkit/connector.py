"""PC side of the relay: carry the coordinator's calls to this PC's own execution service.

Everything is outbound. A long poll to the server collects queued calls; each is made against the
service on 127.0.0.1 and its answer is sent back, with bodies streamed both ways. The PC's /health
answer is pushed every few seconds so the coordinator can read it without a round trip.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path

import aiohttp

log = logging.getLogger('relay-connector')
POLL_WAIT = 25
STATUS_EVERY = 5.0
BACKOFF = (1, 2, 5, 10, 30)
CHUNK = 64 * 1024

REVOCATION_MESSAGE = 'This worker has been unregistered from the fleet. Run agent-worker uninstall, then install again with a new Fleet install command.'


class WorkerRevoked(RuntimeError):
    pass


class Connector:
    def __init__(self, server, worker_id, token, local_port=8096, local_host='127.0.0.1', home=None):
        self.home = Path(home) if home else None  # the install folder: lets the server ask this PC to update itself
        self.server = server.rstrip('/')
        self.worker_id, self.token = worker_id, token
        self.local = f'http://{local_host}:{local_port}'
        self.base = f'{self.server}/relay/{worker_id}'
        self.headers = {'Authorization': f'Bearer {token}'}
        self.connected = False  # the last poll reached the server
        self.stopping = asyncio.Event()
        self.revoked_event = asyncio.Event()
        self.revocation = None
        if self.home is not None:
            try:
                state = json.loads((self.home / 'runtime' / 'connector.json').read_text(encoding='utf-8'))
                saved = state.get('revocation')
                if (isinstance(saved, dict) and saved.get('reason') == 'worker_revoked'
                        and isinstance(saved.get('message'), str) and isinstance(saved.get('at'), (int, float))):
                    self.revocation = {'reason': 'worker_revoked', 'message': saved['message'], 'at': saved['at']}
                    self.revoked_event.set()
            except (OSError, ValueError, TypeError, AttributeError):
                pass

    async def run(self):
        if self.revocation is not None:
            await self.stopping.wait()
            return
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10)
        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as client:
            status_task = asyncio.create_task(self.push_status(client))
            tasks = {status_task}
            failures = 0
            try:
                while not self.stopping.is_set() and self.revocation is None:
                    try:
                        calls = await self.poll_until_revoked(client)
                        if self.revocation is not None:
                            break
                        self.connected, failures = True, 0
                    except WorkerRevoked as exc:
                        self.mark_revoked(str(exc))
                        break
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        if self.connected or failures == 0:
                            log.warning('Relay poll failed: %s', exc)
                        self.connected = False
                        self.write_state()
                        try:
                            await asyncio.wait_for(self.revoked_event.wait(), BACKOFF[min(failures, len(BACKOFF) - 1)])
                        except asyncio.TimeoutError:
                            pass
                        failures += 1
                        continue
                    for call in calls:
                        if self.revocation is not None:
                            break
                        task = asyncio.create_task(self.handle(client, call))
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
            finally:
                if self.revocation is not None:
                    status_task.cancel()
                    await asyncio.gather(status_task, return_exceptions=True)
                else:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            if self.revocation is not None:
                await self.stopping.wait()

    async def poll_until_revoked(self, client):
        poll_task = asyncio.create_task(self.poll(client))
        revoked_task = asyncio.create_task(self.revoked_event.wait())
        try:
            done, _ = await asyncio.wait((poll_task, revoked_task), return_when=asyncio.FIRST_COMPLETED)
            if revoked_task in done:
                raise WorkerRevoked(self.revocation['message'])
            return await poll_task
        finally:
            for task in (poll_task, revoked_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(poll_task, revoked_task, return_exceptions=True)

    async def poll(self, client):
        async with client.get(f'{self.base}/next', params={'wait': POLL_WAIT}, timeout=aiohttp.ClientTimeout(total=POLL_WAIT + 15, connect=10)) as response:
            if response.status == 204: return []
            if response.status == 401: raise RuntimeError("The server refused this PC's token")
            if response.status == 410:
                try: body = await response.json()
                except (aiohttp.ClientError, ValueError, TypeError): body = {}
                if isinstance(body, dict) and body.get('error') == 'worker_revoked':
                    message = body.get('message')
                    raise WorkerRevoked(message if isinstance(message, str) and message.strip() else REVOCATION_MESSAGE)
                raise RuntimeError('Server answered 410')
            if response.status >= 400: raise RuntimeError(f'Server answered {response.status}')
            return await response.json()

    def mark_revoked(self, message):
        if self.revocation is None:
            self.connected = False
            self.revocation = {'reason': 'worker_revoked', 'message': message or REVOCATION_MESSAGE, 'at': time.time()}
            self.write_state()
            log.warning('Worker unregistered from the Fleet: %s', self.revocation['message'])
        self.revoked_event.set()

    def write_state(self):
        if self.home is None: return
        try:
            target = self.home / 'runtime' / 'connector.json'
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_suffix('.tmp')
            temporary.write_text(json.dumps({
                'connected': self.connected,
                'at': time.time(),
                'revoked': self.revocation is not None,
                'revocation': self.revocation,
            }), encoding='utf-8')
            os.replace(temporary, target)
        except OSError: pass

    async def push_status(self, client):
        while self.revocation is None:
            self.write_state()
            try:
                async with client.get(f'{self.local}/health', timeout=aiohttp.ClientTimeout(total=10)) as local:
                    if local.status == 200:
                        health = {**await local.json(), **self.update_state()}
                        if self.revocation is not None: return
                        async with client.post(f'{self.base}/status', json=health, timeout=aiohttp.ClientTimeout(total=10)) as response:
                            if response.status == 410:
                                try: body = await response.json()
                                except (aiohttp.ClientError, ValueError, TypeError): body = {}
                                if isinstance(body, dict) and body.get('error') == 'worker_revoked':
                                    message = body.get('message')
                                    self.mark_revoked(message if isinstance(message, str) and message.strip() else REVOCATION_MESSAGE)
                                    return
                            else:
                                await response.read()
            except asyncio.CancelledError: raise
            except Exception as exc: log.debug('Status push skipped: %s', exc)
            if self.revocation is not None: return
            try: await asyncio.wait_for(self.revoked_event.wait(), STATUS_EVERY)
            except asyncio.TimeoutError: pass


    def update_state(self):
        """What `agent-worker update` last wrote about itself: running, updated or failed, with the reason."""
        try:
            data = json.loads((self.home / 'update-status.json').read_text(encoding='utf-8'))
            return {'update_state': data.get('state'), **({'update_error': str(data['error'])[:300]} if data.get('error') else {})}
        except (OSError, ValueError, TypeError, AttributeError): return {}

    async def control(self, client, url, call):
        """A request from the server about this PC itself (never forwarded to the execution service)."""
        action = call['path'].rsplit('/', 1)[-1]
        status, body = 404, {'error': f'Unknown control action {action}'}
        if action == 'update':
            if self.home is None: status, body = 501, {'error': 'This connector does not know its install folder'}
            elif self.update_state().get('update_state') == 'running': status, body = 202, {'started': False, 'reason': 'an update is already running'}
            else:
                from .procutil import python_for, spawn_detached
                ctl = self.home / 'agents' / 'worker_ctl.py'
                spawn_detached([python_for(self.home), '-u', str(ctl), '--home', str(self.home), 'update', '--background'], cwd=str(self.home), log=str(self.home / 'update.log'))  # not inside agents/, which the update replaces
                status, body = 202, {'started': True}
        async with client.put(f'{url}/response', headers={'X-Relay-Status': str(status), 'Content-Type': 'application/json'}, data=json.dumps(body).encode()) as sent:
            await sent.read()

    async def handle(self, client, call):
        """Make one relayed call locally and send the answer back; one call never ends the loop."""
        url = f'{self.base}/calls/{call["id"]}'
        if call['path'].startswith('/_control/'):
            try: await self.control(client, url, call)
            except asyncio.CancelledError: raise
            except Exception as exc: log.warning('Control call %s failed: %s', call.get('id'), exc)
            return
        try:
            async def request_body():
                async with client.get(f'{url}/body') as source:
                    if source.status >= 400: raise RuntimeError(f'Request body unavailable ({source.status})')
                    async for chunk in source.content.iter_chunked(CHUNK): yield chunk
            headers = dict(call.get('headers') or {})
            target = f'{self.local}{call["path"]}' + (f'?{call["query"]}' if call.get('query') else '')
            data = request_body() if call.get('has_body') else None
            try:
                async with client.request(call['method'], target, headers=headers, data=data, timeout=aiohttp.ClientTimeout(total=None, connect=5)) as local:
                    async def response_body():
                        async for chunk in local.content.iter_chunked(CHUNK): yield chunk
                    reply = {'X-Relay-Status': str(local.status)}
                    if local.content_type: reply['Content-Type'] = local.headers.get('Content-Type', local.content_type)
                    async with client.put(f'{url}/response', headers=reply, data=response_body()) as sent:
                        await sent.read()
            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as exc:
                log.warning('Local service unreachable for %s %s: %s', call['method'], call['path'], exc)
                async with client.put(f'{url}/response', headers={'X-Relay-Drop': '1'}) as sent:
                    await sent.read()
        except asyncio.CancelledError: raise
        except Exception as exc:
            log.warning('Relayed call %s failed: %s', call.get('id'), exc)


def from_environment():
    server = os.environ.get('FLOW_RELAY_SERVER', '')
    worker_id, token = os.environ.get('FLOW_WORKER_ID', ''), os.environ.get('FLOW_WORKER_TOKEN', '')
    if not (server and worker_id and token): raise SystemExit('FLOW_RELAY_SERVER, FLOW_WORKER_ID and FLOW_WORKER_TOKEN are required')
    return Connector(server, worker_id, token, int(os.environ.get('FLOW_WORKER_PORT', '8096')))
