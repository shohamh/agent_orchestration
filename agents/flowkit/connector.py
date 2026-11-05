"""PC side of the relay: carry the coordinator's calls to this PC's own execution service.

Everything is outbound. A long poll to the server collects queued calls; each is made against the
service on 127.0.0.1 and its answer is sent back, with bodies streamed both ways. The PC's /health
answer is pushed every few seconds so the coordinator can read it without a round trip.
"""
import asyncio
import logging
import os

import aiohttp

log = logging.getLogger('relay-connector')
POLL_WAIT = 25
STATUS_EVERY = 5.0
BACKOFF = (1, 2, 5, 10, 30)
CHUNK = 64 * 1024


class Connector:
    def __init__(self, server, worker_id, token, local_port=8096, local_host='127.0.0.1'):
        self.server = server.rstrip('/')
        self.worker_id, self.token = worker_id, token
        self.local = f'http://{local_host}:{local_port}'
        self.base = f'{self.server}/relay/{worker_id}'
        self.headers = {'Authorization': f'Bearer {token}'}
        self.connected = False  # the last poll reached the server
        self.stopping = asyncio.Event()

    async def run(self):
        timeout = aiohttp.ClientTimeout(total=None, connect=10, sock_connect=10)
        async with aiohttp.ClientSession(headers=self.headers, timeout=timeout) as client:
            tasks = {asyncio.create_task(self.push_status(client))}
            failures = 0
            try:
                while not self.stopping.is_set():
                    try:
                        calls = await self.poll(client)
                        self.connected, failures = True, 0
                    except asyncio.CancelledError: raise
                    except Exception as exc:
                        if self.connected or failures == 0: log.warning('Relay poll failed: %s', exc)
                        self.connected = False
                        await asyncio.sleep(BACKOFF[min(failures, len(BACKOFF) - 1)])
                        failures += 1
                        continue
                    for call in calls:
                        task = asyncio.create_task(self.handle(client, call))
                        tasks.add(task)
                        task.add_done_callback(tasks.discard)
            finally:
                for task in tasks: task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def poll(self, client):
        async with client.get(f'{self.base}/next', params={'wait': POLL_WAIT}, timeout=aiohttp.ClientTimeout(total=POLL_WAIT + 15, connect=10)) as response:
            if response.status == 204: return []
            if response.status == 401: raise RuntimeError("The server refused this PC's token")
            if response.status >= 400: raise RuntimeError(f'Server answered {response.status}')
            return await response.json()

    async def push_status(self, client):
        while True:
            try:
                async with client.get(f'{self.local}/health', timeout=aiohttp.ClientTimeout(total=10)) as local:
                    if local.status == 200:
                        health = await local.json()
                        async with client.post(f'{self.base}/status', json=health, timeout=aiohttp.ClientTimeout(total=10)) as response:
                            await response.read()
            except asyncio.CancelledError: raise
            except Exception as exc: log.debug('Status push skipped: %s', exc)
            await asyncio.sleep(STATUS_EVERY)

    async def handle(self, client, call):
        """Make one relayed call locally and send the answer back; one call never ends the loop."""
        url = f'{self.base}/calls/{call["id"]}'
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
