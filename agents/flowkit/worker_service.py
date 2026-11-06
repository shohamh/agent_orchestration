"""Authenticated execution PC. Engine coordinators submit isolated step jobs here."""
import asyncio
import hmac
import json
import os
import re
import socket
import sqlite3
import shutil
import sys
import time
import uuid
from pathlib import Path

from aiohttp import web
from .artifacts import restore, snapshot
from .definition import parse

ROOT = Path(__file__).resolve().parents[2]
AUTHORIZATION_TIMEOUT = 10  # an admitted job releases its reservation if not authorized in time
COORDINATOR_STALE = 10  # an authorizing job only seeks admission while its coordinator polls


async def capacity_call(function, *args, deadline=None, cancelled=None):
    """SQLite may block briefly; retries never block the event loop."""
    while True:
        if cancelled is not None and cancelled.is_set(): raise asyncio.CancelledError
        try: return await asyncio.to_thread(function, *args)
        except sqlite3.OperationalError as exc:
            if 'locked' not in str(exc).lower() and 'busy' not in str(exc).lower(): raise
            if deadline is not None and time.monotonic() >= deadline:
                raise RuntimeError('Capacity database unavailable; lease lost') from exc
            if cancelled is not None and cancelled.is_set(): raise asyncio.CancelledError
            await asyncio.sleep(.05)


async def finish_despite_cancellation(task):
    """Join a separately supervised task despite repeated cancellation requests."""
    while not task.done():
        try: await asyncio.shield(task)
        except asyncio.CancelledError: continue
    return task.result()


def termination_path(app, job_id):
    """Durable proof exists only for a job whose local cleanup finished successfully."""
    return app['root'] / '.terminated' / (str(uuid.UUID(job_id)) + '.json')


def record_termination(app, job_id):
    from .restart import atomic_json
    if not app['capacity'].revoke_absent(job_id, app['gate_id']):
        raise RuntimeError('The job still has an active capacity lease')
    atomic_json(termination_path(app, job_id), {'terminated': True, 'gate_id': app['gate_id']})


class Job:
    def __init__(self, app, job_id, config):
        self.app, self.id, self.config = app, job_id, config
        self.events, self.output, self.error, self.state = [], None, None, 'queued'
        self.inbox = asyncio.Queue()
        self.process = None
        self.last_seen = time.monotonic()
        self.dropped = 0
        self.started = False
        # Coordinators that opt in must authorize each admitted start; legacy submitters do not.
        self.authorize = bool(config.get('authorize'))
        self.authorized = asyncio.Event()
        self.nonce = self.reason = None
        self.task = asyncio.create_task(self.run())

    async def missing_software(self):
        """Recheck the step's software immediately before launch, as execution resolves it."""
        requirement = self.config.get('requirement') or {}  # set by the service, never by the client
        for kind in ('agent', 'command'):
            name = requirement.get(kind)
            if not name: continue
            result = await asyncio.to_thread(self.app['probe'], kind, name)
            if result['status'] != 'installed':
                try: await self.app['refresh_inventory']([name] if kind == 'command' else [])  # record the absence
                except (OSError, ValueError): pass
                return f'Required software {name!r} is {result["status"]} on this worker'
        return None

    async def run(self):
        work = asyncio.create_task(self._run())
        try:
            await asyncio.shield(work)
            await asyncio.to_thread(record_termination, self.app, self.id)
        except asyncio.CancelledError:
            work.cancel()  # Exactly one cancellation reaches execution/cleanup.
            try: await finish_despite_cancellation(work)
            except asyncio.CancelledError: pass
            await asyncio.to_thread(record_termination, self.app, self.id)
            raise
        except Exception as exc:
            self.state, self.error = 'failed', str(exc)[-8000:]
            raise

    async def _run(self):
        completed = False
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
                    if self.app.get('draining'):
                        # An update is waiting for running work to finish: never admit more, so the coordinator places it elsewhere.
                        if lease is not None: await capacity_call(self.app['capacity'].release, self.id)
                        lease = None
                        self.state, self.error, self.reason = ('rejected' if self.authorize else 'failed'), 'This PC is updating', 'updating'
                        return
                    if self.authorize and time.monotonic() - self.last_seen > COORDINATOR_STALE:
                        # Never take capacity for a coordinator that may have cancelled while unreachable.
                        self.waiting_reason = 'coordinator_unreachable'
                        await asyncio.sleep(.2)
                        continue
                    cancel_acquire = asyncio.Event()
                    acquire = asyncio.create_task(capacity_call(lambda: self.app['capacity'].try_acquire(self.id, expected_gate_id=self.config.get('gate_id'), expected_settings_revision=self.config.get('settings_revision')), cancelled=cancel_acquire))
                    try: lease = await asyncio.shield(acquire)
                    except asyncio.CancelledError:
                        cancel_acquire.set()
                        lease = await acquire  # Account for a reservation committed during cancellation.
                        raise
                    if not lease['acquired']:
                        if lease['reason'] in ('revoked', 'gate_changed', 'settings_changed'):
                            self.state, self.error, self.reason = 'rejected', 'Job was revoked or worker settings changed before execution', lease['reason']
                            return
                        self.waiting_reason = lease['reason']
                        await asyncio.sleep(1)
                        continue
                    if self.authorize:
                        # The coordinator must confirm, after admission, that this attempt is still wanted.
                        # One nonce per admission: a late confirmation can never authorize a later one.
                        self.authorized.clear()
                        self.nonce, self.waiting_reason = uuid.uuid4().hex, 'awaiting_authorization'
                        try: await asyncio.wait_for(self.authorized.wait(), AUTHORIZATION_TIMEOUT)
                        except asyncio.TimeoutError:
                            self.nonce = None  # before any await, so no confirmation can still land
                            await capacity_call(self.app['capacity'].release, self.id)
                            lease = None
                            continue
                        finally: self.nonce = None
                    missing = await self.missing_software()
                    if missing:
                        await capacity_call(self.app['capacity'].release, self.id)
                        lease = None
                        self.state, self.error, self.reason = ('rejected' if self.authorize else 'failed'), missing, 'software_missing'
                        return
                    break
                self.waiting_reason = None
                async def renew():
                    while True:
                        await asyncio.sleep(5)
                        try:
                            live = await capacity_call(self.app['capacity'].renew, self.id, lease['fence'], deadline=lease['expires'])
                            if not live: raise RuntimeError('Capacity lease expired')
                            lease['expires'] = time.monotonic() + 30
                        except Exception as exc:
                            self.lease_loss = str(exc)
                            self.task.cancel()
                            return
                heartbeat = asyncio.create_task(renew())
                self.app['stats']['active'] += 1
                try:
                    self.waiting_reason = 'starting'
                    env = {**os.environ, 'FLOW_WORKSPACES': str(workspaces), 'FLOW_ASSETS_DIR': str(assets), 'FLOW_MANAGED_JOB':'1', 'FLOW_CAPACITY_HANDSHAKE':'1', 'PYTHONUTF8': '1', 'PYTHONUNBUFFERED': '1'}
                    launch = asyncio.create_task(asyncio.create_subprocess_exec(sys.executable, '-u', '-m', 'flowkit.worker_stdio', cwd=ROOT/'agents', env=env,
                        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                        limit=512*1024, start_new_session=os.name != 'nt'))
                    try: self.process = await asyncio.shield(launch)
                    except asyncio.CancelledError:
                        self.process = await launch
                        raise
                    tree_name = None
                    if os.name == 'nt':
                        from agentdriver.process_tree import ProcessTree
                        tree_name = 'Local\\FlowCapacity-' + lease['fence']
                        tree = ProcessTree(self.process.pid, name=tree_name)
                    self.process.stdin.write((json.dumps(self.config['start'])+'\n').encode())
                    await self.process.stdin.drain()
                    ready = json.loads(await self.process.stdout.readline())
                    if ready != {'type':'capacity_ready'}: raise RuntimeError('Worker readiness handshake failed')
                    attach = asyncio.create_task(capacity_call(self.app['capacity'].attach_execution, self.id, lease['fence'], self.process.pid, tree_name, deadline=lease['expires']))
                    try: await asyncio.shield(attach)
                    except asyncio.CancelledError:
                        await attach
                        raise
                    self.process.stdin.write(b'{"type":"capacity_go"}\n')
                    await self.process.stdin.drain()
                    started = json.loads(await self.process.stdout.readline())
                    if started != {'type':'capacity_started'}: raise RuntimeError('Worker start handshake failed')
                    confirm = asyncio.create_task(capacity_call(self.app['capacity'].confirm_started, self.id, lease['fence'], self.process.pid, tree_name, deadline=lease['expires']))
                    try: await asyncio.shield(confirm)
                    except asyncio.CancelledError:
                        await confirm
                        raise
                    self.waiting_reason = None
                    self.state = 'running'
                    self.started = True
                    self.events.append({'type': 'worker', 'worker': self.app['identity']})
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
                        publication = asyncio.create_task(capacity_call(self.app['capacity'].publish, self.id, lease['fence'], lambda: atomic_json(directory/'result-files.json', files), deadline=lease['expires']))
                        try: await asyncio.shield(publication)
                        except asyncio.CancelledError:
                            await publication
                            raise
                        self.result_path = directory/'result-files.json'
                        self.config.clear()  # release transferred file payloads after execution
                        completed = True
                    finally:
                        input_task.cancel()
                        errors.cancel()
                        await asyncio.gather(input_task, errors, return_exceptions=True)
                finally: self.app['stats']['active'] -= 1
        except asyncio.CancelledError:
            self.state, self.error = 'cancelled', getattr(self, 'lease_loss', 'Execution cancelled')
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
            if lease and lease.get('acquired'):
                await capacity_call(self.app['capacity'].finish, self.id, self.app['gate_id'])
        if completed: self.state = 'done'


def make_app(worker_id=None, root=None, slots=None, pool=None, tags=None, token=None, capacity=None, probe=None):
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
    app['draining'], app['version'] = False, os.environ.get('FLOW_WORKER_VERSION', '')
    app['root'] = Path(root or os.environ.get('FLOW_WORKER_ROOT', ROOT/'agents/runtime/execution'))
    from .capacity import CapacityGate
    app['capacity'] = capacity
    from .capabilities import probe as default_probe
    app['probe'] = probe or default_probe
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
            updated = await asyncio.to_thread(refresh, app['inventory'], names | set(commands), app['probe'])
            await asyncio.to_thread(atomic_json, inventory_path, updated)
            app['inventory'] = updated
            return updated
    app['refresh_inventory'] = refresh_inventory
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
    async def health(request):
        return web.json_response({**app['identity'], 'slots': count, 'active': app['stats']['active'], 'capacity': await capacity_call(app['capacity'].metrics),
                                  'draining': bool(app.get('draining')), **({'worker_version': app['version']} if app.get('version') else {})})
    async def drain(request):
        """An update asks the service to stop admitting work; running jobs finish. {"drain": false} lifts it."""
        try: data = await request.json()
        except (ValueError, UnicodeDecodeError): raise ValueError('Invalid JSON body') from None
        if not isinstance(data, dict) or set(data) != {'drain'} or not isinstance(data['drain'], bool): raise ValueError('Body must be {"drain": true|false}')
        app['draining'] = data['drain']
        return web.json_response({'draining': app['draining'], 'active': app['stats']['active']})
    async def configure(request):
        """The coordinator's registry pushes this PC's admission limits. Only limits are accepted;
        they apply to future admissions, never to running work (the gate reads them per admission)."""
        try: data = await request.json()
        except (ValueError, UnicodeDecodeError): raise ValueError('Invalid JSON body') from None
        number = lambda v: isinstance(v, (int, float)) and not isinstance(v, bool)
        if not isinstance(data, dict) or set(data) != {'slots', 'cpu_limit', 'ram_limit'}:
            raise ValueError('Body must contain exactly slots, cpu_limit and ram_limit')
        from .capacity import MAX_SLOTS
        if not isinstance(data['slots'], int) or isinstance(data['slots'], bool) or not 1 <= data['slots'] <= MAX_SLOTS:
            raise ValueError(f'slots must be a whole number from 1 to {MAX_SLOTS}')
        if not all(number(data[k]) and 0 < data[k] <= 100 for k in ('cpu_limit', 'ram_limit')):
            raise ValueError('cpu_limit and ram_limit must be percentages above 0 and at most 100')
        expected = request.headers.get('X-Flow-Gate-ID')
        if expected is not None and expected != app['gate_id']: raise ValueError('Worker capacity gate identity changed')
        raw_revision = request.headers.get('X-Flow-Settings-Revision')
        if raw_revision is not None and (not raw_revision.isascii() or not raw_revision.isdigit() or len(raw_revision) > 30):
            raise ValueError('Invalid worker settings revision')
        revision = int(raw_revision) if raw_revision is not None else None
        await capacity_call(lambda: app['capacity'].configure(**data, expected_gate_id=expected, settings_revision=revision))
        return web.json_response({'settings': (await capacity_call(app['capacity'].metrics))['settings']})
    async def create(request):
        config = await request.json()
        job_id = str(uuid.UUID(config['id']))
        if config.get('gate_id') is not None and (config['gate_id'] != app['gate_id'] or config['gate_id'] != (await capacity_call(app['capacity'].metrics))['gate_id']):
            raise ValueError('Worker capacity gate identity changed')
        if job_id not in app['jobs']:
            if termination_path(app, job_id).exists(): raise ValueError('A terminated job ID cannot be reused')
            if len(app['jobs']) >= 100: raise ValueError('Execution worker has too many retained jobs; retry later')
            start = config['start']
            if not re.fullmatch(r'[A-Za-z0-9_-]{1,150}', start['runId']): raise ValueError('Invalid run id')
            from .fleet_matching import card_session, software_requirement
            if 'session' in start:  # a standalone card; it never uses a coordinator path
                if set(start) != {'session', 'runId'}: raise ValueError('Invalid card job')
                config['requirement'] = {'agent': card_session(start['session']).get('agent', 'codex')}
            else:
                flow = parse(start['flow'])
                if start['step'] not in flow.steps: raise ValueError('Unknown step')
                config['requirement'] = software_requirement(flow.steps[start['step']])
            if config['requirement'].get('command'): await refresh_inventory([config['requirement']['command']])
            from .artifacts import validate
            validate(config['files']); validate(config['assets'])
            config['runId'] = start['runId']
            # Capability refresh above yields; a concurrent submit or cancel may have
            # registered or terminated this ID while it was in flight.
            if config.get('gate_id') is not None and config['gate_id'] != (await capacity_call(app['capacity'].metrics))['gate_id']:
                raise ValueError('Worker capacity gate identity changed')
            if job_id in app['jobs']: return web.json_response({'id': job_id})
            if termination_path(app, job_id).exists(): raise ValueError('A terminated job ID cannot be reused')
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
                  'worker':app['identity'] if item.started else None, 'waiting_reason': getattr(item, 'waiting_reason', None),
                  'authorization': item.nonce, 'reason': item.reason}
        if item.state == 'done': result.update(output=item.output, files=json.loads(await asyncio.to_thread(item.result_path.read_text, encoding='utf-8')))
        return web.json_response(result)
    async def receive(request):
        item = job(request)
        if item.state not in ('queued', 'running'): raise ValueError('Job has finished')
        await item.inbox.put(await request.json())
        return web.json_response({'ok': True})
    async def authorize(request):
        item = job(request)
        item.last_seen = time.monotonic()
        body = await request.json()
        nonce = body.get('nonce') if isinstance(body, dict) else None
        if not isinstance(nonce, str) or not item.nonce or not hmac.compare_digest(nonce, item.nonce):
            return web.json_response({'error': 'No start authorization is pending for this job'}, status=409)
        item.authorized.set()
        return web.json_response({'ok': True})
    async def cancel(request):
        expected = request.headers.get('X-Flow-Gate-ID')
        current = (await capacity_call(app['capacity'].metrics))['gate_id']
        if current != app['gate_id'] or (expected is not None and expected != current):
            return web.json_response({'error': 'Worker capacity gate identity changed'}, status=409)
        jid = request.match_info['id']
        if jid not in app['jobs']:
            try:
                proof = json.loads(termination_path(app, jid).read_text(encoding='utf-8'))
                terminated = proof.get('terminated') is True and proof.get('gate_id') == current
            except (OSError, ValueError, AttributeError): terminated = False
            if terminated: return web.json_response({'ok': True, 'terminated': True})
            try: canonical = str(uuid.UUID(jid))
            except (ValueError, AttributeError): raise web.HTTPNotFound()
            if not await capacity_call(app['capacity'].revoke_absent, canonical, current):
                return web.json_response({'error': 'A previous execution may still be running'}, status=409)
            # The durable revocation prevents any delayed submit from acquiring capacity.
            # Recheck this process after the database await, and do not yield before the marker.
            if jid not in app['jobs']:
                record_termination(app, canonical)
                return web.json_response({'ok': True, 'terminated': True})
        item = job(request)
        item.task.cancel()
        results = await asyncio.gather(item.task, return_exceptions=True)
        failure = results[0]
        if (isinstance(failure, BaseException) and not isinstance(failure, asyncio.CancelledError)) or (item.process and item.process.returncode is None):
            return web.json_response({'error': 'Worker cleanup did not confirm process termination'}, status=503)
        if not await capacity_call(app['capacity'].revoke_absent, jid, current):
            return web.json_response({'error': 'A previous execution may still be running'}, status=409)
        if item.state in ('queued','running'): item.state, item.error = 'cancelled', 'Execution cancelled'
        await asyncio.to_thread(record_termination, app, jid)
        return web.json_response({'ok': True, 'terminated': True})
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
                sample = await asyncio.to_thread(sampler.sample)
                if sample is not None: await capacity_call(app['capacity'].observe, *sample, time.monotonic())
            except (OSError, ValueError, sqlite3.Error): pass  # Missing measurements remain unknown, never zero.
            await asyncio.sleep(1)
    async def startup(app):
        if app['capacity'] is None:
            app['capacity'] = await capacity_call(lambda: CapacityGate(slots=count))
        app['gate_id'] = (await capacity_call(app['capacity'].metrics))['gate_id']
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
    app.router.add_post('/drain', drain)
    app.router.add_get('/capabilities', inventory)
    app.router.add_post('/capabilities/refresh', refresh_capabilities)
    app.router.add_post('/capacity/settings', configure)
    app.router.add_post('/jobs', create)
    app.router.add_get('/jobs/{id}', status)
    app.router.add_post('/jobs/{id}/input', receive)
    app.router.add_post('/jobs/{id}/start', authorize)
    app.router.add_delete('/jobs/{id}', cancel)
    return app


if __name__ == '__main__':
    bind = os.environ.get('FLOW_WORKER_BIND', '127.0.0.1')
    if bind not in ('127.0.0.1', 'localhost', '::1') and not os.environ.get('FLOW_WORKER_TOKEN'):
        raise SystemExit('FLOW_WORKER_TOKEN is required for a network execution service')
    web.run_app(make_app(), host=bind, port=int(os.environ.get('FLOW_WORKER_PORT', '8096')))
