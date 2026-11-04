"""Authenticated execution PC. Engine coordinators submit isolated step jobs here."""
import asyncio
import hmac
import json
import os
import re
import socket
import shutil
import sys
import time
import uuid
from pathlib import Path

from aiohttp import web
from .artifacts import restore, snapshot
from .definition import parse

ROOT = Path(__file__).resolve().parents[2]


class Job:
    def __init__(self, app, job_id, config):
        self.app, self.id, self.config = app, job_id, config
        self.events, self.output, self.error, self.state = [], None, None, 'queued'
        self.inbox = asyncio.Queue()
        self.process = None
        self.last_seen = time.monotonic()
        self.dropped = 0
        self.started = False
        self.task = asyncio.create_task(self.run())

    async def run(self):
        tree = None
        lease = None
        heartbeat = None
        try:
            async with self.app['slots']:
                directory = self.app['root'] / self.id
                workspaces, assets = directory/'workspaces', directory/'assets'
                restore(self.config['files'], workspaces/self.config['runId'])
                restore(self.config['assets'], assets)
                while True:
                    lease = self.app['capacity'].try_acquire(self.id)
                    if lease['acquired']: break
                    self.waiting_reason = lease['reason']
                    await asyncio.sleep(1)
                self.waiting_reason = None
                async def renew():
                    while True:
                        await asyncio.sleep(5)
                        if not self.app['capacity'].renew(self.id, lease['fence']):
                            self.task.cancel()
                            return
                heartbeat = asyncio.create_task(renew())
                self.app['stats']['active'] += 1
                try:
                    self.state = 'running'
                    self.started = True
                    self.events.append({'type': 'worker', 'worker': self.app['identity']})
                    env = {**os.environ, 'FLOW_WORKSPACES': str(workspaces), 'FLOW_ASSETS_DIR': str(assets), 'FLOW_MANAGED_JOB':'1', 'PYTHONUTF8': '1', 'PYTHONUNBUFFERED': '1'}
                    self.process = await asyncio.create_subprocess_exec(sys.executable, '-u', '-m', 'flowkit.worker_stdio', cwd=ROOT/'agents', env=env,
                        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                        limit=512*1024, start_new_session=os.name != 'nt')
                    from .executor import _own
                    tree = _own(self.process.pid)
                    self.process.stdin.write((json.dumps(self.config['start'])+'\n').encode())
                    await self.process.stdin.drain()
                    async def inputs():
                        while True:
                            item = await self.inbox.get()
                            self.process.stdin.write((json.dumps(item)+'\n').encode())
                            await self.process.stdin.drain()
                    async def stderr():
                        tail = ''
                        while line := await self.process.stderr.readline(): tail = (tail + line.decode('utf-8', 'replace'))[-8000:]
                        return tail
                    input_task = asyncio.create_task(inputs())
                    errors = asyncio.create_task(stderr())
                    try:
                        got_result = False
                        while line := await self.process.stdout.readline():
                            event = json.loads(line)
                            if event.get('type') == 'result': self.output, got_result = event.get('output'), True
                            else:
                                self.events.append(event)
                                # Bound resident history; cursor reports the retained starting position.
                                if len(self.events) > 5000:
                                    self.events.pop(0)
                                    self.dropped += 1
                        code = await self.process.wait()
                        error = await errors
                        if code or not got_result: raise RuntimeError(error or f'Worker process exited {code} without a result')
                        files = await asyncio.to_thread(snapshot, workspaces/self.config['runId'])
                        if os.name == 'nt':
                            for name,entry in files.items():
                                original=self.config['files'].get(name)
                                if original and original['data']==entry['data']: entry['mode']=original['mode']
                        from .restart import atomic_json
                        self.app['capacity'].publish(self.id, lease['fence'], lambda: atomic_json(directory/'result-files.json', files))
                        self.result_path = directory/'result-files.json'
                        self.config.clear()  # release transferred file payloads after execution
                        self.state = 'done'
                    finally:
                        input_task.cancel()
                        errors.cancel()
                        await asyncio.gather(input_task, errors, return_exceptions=True)
                finally: self.app['stats']['active'] -= 1
        except asyncio.CancelledError:
            self.state, self.error = 'cancelled', 'Execution cancelled'
            raise
        except Exception as exc:
            self.state, self.error = 'failed', str(exc)[-8000:]
        finally:
            self.last_seen = time.monotonic()
            if heartbeat:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            if self.process:
                from .executor import _kill
                _kill(self.process, tree)
                if tree: tree.close()
                await self.process.wait()
            if lease and lease.get('acquired'): self.app['capacity'].release(self.id)


def make_app(worker_id=None, root=None, slots=None, pool=None, tags=None, token=None, capacity=None):
    token = os.environ.get('FLOW_WORKER_TOKEN', '') if token is None else token
    @web.middleware
    async def auth(request, handler):
        if token and not hmac.compare_digest(request.headers.get('Authorization', ''), f'Bearer {token}'):
            raise web.HTTPUnauthorized()
        try: return await handler(request)
        except (ValueError, KeyError) as exc: return web.json_response({'error': str(exc)}, status=400)
    app = web.Application(middlewares=[auth], client_max_size=190*1024*1024)
    count = slots or int(os.environ.get('FLOW_WORKER_SLOTS', '4'))
    app['identity'] = {'id': worker_id or os.environ.get('FLOW_WORKER_ID', 'local'), 'hostname': socket.gethostname(),
                       'pool': pool or os.environ.get('FLOW_WORKER_POOL', 'default'),
                       'tags': tags if tags is not None else [t for t in os.environ.get('FLOW_WORKER_TAGS', '').split(',') if t]}
    if not 1 <= count <= 100: raise ValueError('Worker slots must be from 1 to 100')
    app['slots'], app['count'], app['stats'] = asyncio.Semaphore(count), count, {'active':0}
    app['root'] = Path(root or os.environ.get('FLOW_WORKER_ROOT', ROOT/'agents/runtime/execution'))
    from .capacity import CapacityGate
    app['capacity'] = capacity if capacity is not None else CapacityGate(slots=count)
    app['jobs'] = {}
    inventory_path = app['root'] / 'capabilities.json'
    try:
        loaded = json.loads(inventory_path.read_text(encoding='utf-8'))
        if not isinstance(loaded, dict) or not isinstance(loaded.get('commands', {}), dict): raise ValueError('invalid inventory')
        app['inventory'] = loaded
    except (OSError, ValueError): app['inventory'] = {}
    inventory_lock = asyncio.Lock()
    async def refresh_inventory(commands=()):
        from .capabilities import refresh
        from .restart import atomic_json
        async with inventory_lock:
            existing = app['inventory'].get('commands', {})
            names = {name for name in existing if isinstance(name, str) and name and len(name) <= 260}
            updated = await asyncio.to_thread(refresh, app['inventory'], names | set(commands))
            await asyncio.to_thread(atomic_json, inventory_path, updated)
            app['inventory'] = updated
            return updated
    async def inventory(request):
        from .capabilities import overdue
        if overdue(app['inventory']): await refresh_inventory()
        return web.json_response(app['inventory'])
    async def refresh_capabilities(request):
        try: data = await request.json() if request.can_read_body else {}
        except (ValueError, UnicodeDecodeError): raise ValueError('Invalid JSON body') from None
        if not isinstance(data, dict): raise ValueError('Body must be an object')
        commands = data.get('commands', [])
        if not isinstance(commands, list) or any(not isinstance(x, str) or not x or len(x) > 260 or any(c in x for c in '\x00\r\n') for x in commands):
            raise ValueError('commands must be a list of executable names')
        return web.json_response(await refresh_inventory(commands))
    async def daily_inventory():
        while True:
            await asyncio.sleep(3600)
            from .capabilities import overdue
            if overdue(app['inventory']): await refresh_inventory()
    async def health(request): return web.json_response({**app['identity'], 'slots': count, 'active': app['stats']['active'], 'capacity': app['capacity'].metrics()})
    async def create(request):
        config = await request.json()
        job_id = str(uuid.UUID(config['id']))
        if job_id not in app['jobs']:
            if len(app['jobs']) >= 100: raise ValueError('Execution worker has too many retained jobs; retry later')
            start = config['start']
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,150}', start['runId']): raise ValueError('Invalid run id')
            flow = parse(start['flow'])
            if start['step'] not in flow.steps: raise ValueError('Unknown step')
            step = flow.steps[start['step']]
            if step.kind == 'program':
                from .capabilities import infer_command
                requirement = step.requires_command or infer_command(step.run)
                if requirement: await refresh_inventory([requirement])
            from .artifacts import validate
            validate(config['files']); validate(config['assets'])
            config['runId'] = start['runId']
            app['jobs'][job_id] = Job(app, job_id, config)
        return web.json_response({'id': job_id})
    def job(request):
        item = app['jobs'].get(request.match_info['id'])
        if item is None: raise web.HTTPNotFound()
        return item
    async def status(request):
        item = job(request)
        item.last_seen = time.monotonic()
        dropped = item.dropped
        after = max(0, int(request.query.get('after', '0')) - dropped)
        result = {'state': item.state, 'events': item.events[after:], 'cursor': dropped+len(item.events), 'error': item.error,
                  'worker':app['identity'] if item.started else None, 'waiting_reason': getattr(item, 'waiting_reason', None)}
        if item.state == 'done': result.update(output=item.output, files=json.loads(await asyncio.to_thread(item.result_path.read_text, encoding='utf-8')))
        return web.json_response(result)
    async def receive(request):
        item = job(request)
        if item.state not in ('queued', 'running'): raise ValueError('Job has finished')
        await item.inbox.put(await request.json())
        return web.json_response({'ok': True})
    async def cancel(request):
        item = job(request)
        item.task.cancel()
        await asyncio.gather(item.task, return_exceptions=True)
        if item.state in ('queued','running'): item.state, item.error = 'cancelled', 'Execution cancelled'
        return web.json_response({'ok': True})
    async def reap():
        while True:
            await asyncio.sleep(5)
            now=time.monotonic()
            for jid,item in list(app['jobs'].items()):
                if item.state in ('queued','running') and now-item.last_seen>45:
                    item.task.cancel()
                    await asyncio.gather(item.task,return_exceptions=True)
                    item.state,item.error='cancelled','Coordinator lease expired'
                elif item.task.done() and now-item.last_seen>300:
                    app['jobs'].pop(jid,None)
                    await asyncio.to_thread(shutil.rmtree,app['root']/jid,ignore_errors=True)
    async def sample_capacity():
        from .capacity import MachineSampler
        sampler = MachineSampler()
        while True:
            try:
                sample = sampler.sample()
                if sample is not None: app['capacity'].observe(*sample, time.monotonic())
            except (OSError, ValueError): pass  # Missing measurements remain unknown, never zero.
            await asyncio.sleep(1)
    async def startup(app):
        app['capacity_task'] = asyncio.create_task(sample_capacity())
        await refresh_inventory()
        app['inventory_task'] = asyncio.create_task(daily_inventory())
        app['reaper']=asyncio.create_task(reap())
    async def cleanup(app):
        app['capacity_task'].cancel()
        await asyncio.gather(app['capacity_task'], return_exceptions=True)
        app['inventory_task'].cancel()
        await asyncio.gather(app['inventory_task'], return_exceptions=True)
        app['reaper'].cancel()
        await asyncio.gather(app['reaper'],return_exceptions=True)
        for item in app['jobs'].values(): item.task.cancel()
        await asyncio.gather(*(j.task for j in app['jobs'].values()), return_exceptions=True)
    app.on_cleanup.append(cleanup)
    app.on_startup.append(startup)
    app.router.add_get('/health', health)
    app.router.add_get('/capabilities', inventory)
    app.router.add_post('/capabilities/refresh', refresh_capabilities)
    app.router.add_post('/jobs', create)
    app.router.add_get('/jobs/{id}', status)
    app.router.add_post('/jobs/{id}/input', receive)
    app.router.add_delete('/jobs/{id}', cancel)
    return app


if __name__ == '__main__':
    bind = os.environ.get('FLOW_WORKER_BIND', '127.0.0.1')
    if bind not in ('127.0.0.1', 'localhost', '::1') and not os.environ.get('FLOW_WORKER_TOKEN'):
        raise SystemExit('FLOW_WORKER_TOKEN is required for a network execution service')
    web.run_app(make_app(), host=bind, port=int(os.environ.get('FLOW_WORKER_PORT', '8096')))
