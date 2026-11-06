"""Step placement and transfer extension. Native engines still invoke tasks in DAG order."""
import asyncio
import contextlib
import json
import os
import threading
import time
import uuid
from pathlib import Path
from types import SimpleNamespace

import aiohttp

from .artifacts import ancestor_files, snapshot, publish, validate
from .definition import FLOWS_DIR
from .restart import prepare_workspace, read_result, save_result, atomic_json, descendants, workspace_path, pin_definition, publication_lock
from .fleet_matching import requirements_for, validate_launch, with_engine
from .relay import is_relayed
from .workers import fleet_status, routing, selection_for, selector, with_last_known

PLACEMENT_POLL = 2.0  # seconds between placement checks while a step waits for a worker
PROBE_INTERVAL = 15.0  # minimum seconds between on-demand capability probes of one worker
TIMEOUT_SLACK = 5.0  # the engine's step timeout starts slightly before this coroutine does
RELAYED_POLL = 1.0  # seconds between job polls through the relay (each poll is a round trip through the PC's connector)
START_TIMEOUT = 5.0  # a start confirmation holds the cancel and settings locks, so it is short


def execution_info(run_id):
    root = workspace_path(run_id)
    result = {}
    for path in (root/'.flow-workers').glob('*.json'):
        try: result[path.stem] = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError): pass
    return result


def worker_jobs(worker_id, limit=200):
    """Every card and step placed on a worker that this coordinator still has a record of, newest first."""
    base = workspace_path('run').parent
    jobs = []
    try:
        runs = [p for p in base.iterdir() if p.is_dir()]
    except OSError:
        return []
    for run in runs:
        for path in (run/'.flow-workers').glob('*.json'):
            try:
                info = json.loads(path.read_text(encoding='utf-8'))
                placed = info.get('worker')
                if isinstance(placed, dict) and placed.get('id') == worker_id:
                    jobs.append({'run': run.name, 'step': path.stem, 'state': info.get('state'), 'engine': info.get('engine'), 'at': path.stat().st_mtime})
            except (OSError, ValueError, AttributeError):
                pass
    jobs.sort(key=lambda job: job['at'], reverse=True)
    return jobs[:limit]


def set_selection(run_id, step, wanted):
    from .definition import STEP_ID
    if not STEP_ID.fullmatch(step): raise ValueError('Invalid step id')
    root = workspace_path(run_id)
    if not root.is_dir(): raise ValueError('This run has no distributed workspace')
    wanted=selector(wanted)
    path=root/'.flow-selections'/f'{step}.json'
    if wanted: atomic_json(path,wanted)
    else: path.unlink(missing_ok=True)


def mark_cancelled(run_id):
    """The board's cancel API records a user cancellation before cancelling the engine run.

    The marker is written under the lock a coordinator holds while it confirms a start, so every
    start is either confirmed before the cancellation (and then cancelled with the job) or sees
    the marker and is never confirmed."""
    root = workspace_path(run_id)
    root.mkdir(parents=True, exist_ok=True)
    with publication_lock(root, '_cancel'):
        atomic_json(root/'.flow-cancel.json', {'at': time.time()})
    return True


def check_cancelled(root):
    if (root / '.flow-cancel.json').exists():
        raise asyncio.CancelledError('Execution was cancelled')


def step_timeout_seconds(step):
    return step.timeout_minutes * 60


def cancellation_state(root, step, since_wall, since_monotonic):
    """cancelled: through the board after this attempt began; timed_out: the engine's step
    timeout (timeout_minutes) expired; interrupted: any other engine-side cancellation."""
    try: marked = json.loads((root/'.flow-cancel.json').read_text(encoding='utf-8'))['at'] >= since_wall - 1
    except (OSError, ValueError, KeyError, TypeError): marked = False
    if marked: return 'cancelled'
    if time.monotonic() - since_monotonic + TIMEOUT_SLACK >= step_timeout_seconds(step): return 'timed_out'
    return 'interrupted'


@contextlib.asynccontextmanager
async def held(*locks):
    """Hold synchronous (cross-process) locks across awaits.

    The locks are taken and released on one dedicated thread, as thread-owned locks require, so
    the event loop never blocks on them. Cancellation while waiting still releases them."""
    loop = asyncio.get_running_loop()
    acquired, release = loop.create_future(), threading.Event()
    def settle(call, value):
        def apply():
            if not acquired.done(): call(value)
        try: loop.call_soon_threadsafe(apply)
        except RuntimeError: pass  # the loop is gone: nobody is waiting
    def hold():
        try:
            with contextlib.ExitStack() as stack:
                for lock in locks: stack.enter_context(lock())
                settle(acquired.set_result, None)
                release.wait()
        except BaseException as exc: settle(acquired.set_exception, exc)
    threading.Thread(target=hold, daemon=True).start()
    try:
        await acquired
        yield
    finally: release.set()


class _Replace(Exception):
    """The submitted job was never authorized to start, so the step can be placed again."""
    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


CONTEXT_KEY = 'context-setup'  # execution records of the jobs that fetch what was attached to a run
CONFLUENCE_KEY = 'context-confluence'
CONTEXT_FILE = '.flow-launch-context.json'


async def attached_files(root, context, wanted, engine, receive, sink):
    """The files attached to this run. The first step to get here prepares them, the others wait:
    every step must see the same files, or parallel clones would conflict where branches join."""
    from . import launch_context
    if not launch_context.has_sources(context): return {}
    path, owner = root / CONTEXT_FILE, None
    while not path.exists():
        check_cancelled(root)
        owner = _try_owner(root, '_context')
        if owner: break
        await asyncio.sleep(.5)
    try:
        if not path.exists():
            files = await asyncio.to_thread(launch_context.upload_files, context)
            remote, chosen = launch_context.remote_sources(context), {k: v for k, v in wanted.items() if v}
            async def fetch(key, start, needs, relay):
                payload = {'files': {}, 'assets': {}, 'start': {**start, 'runId': root.name}}
                await _placed(root, key, with_engine({**chosen, **needs}, engine), wanted, payload, CARD_TIMEOUT, receive,
                              relay.forward if relay else sink, engine, lambda data: files.update(data['files']), announce=False)
            try:
                if remote.get('repositories'):
                    relay = launch_context.SourceRelay(sink, 'context:repositories', 'Clone repositories', launch_context.targets({'r': remote['repositories']}))
                    try: await fetch(CONTEXT_KEY, launch_context.clone_job(remote['repositories']), {'command': 'git'}, relay)
                    except Exception as exc:
                        await relay.failed(exc, announce=False)
                        raise
                    await relay.finished()
                if remote.get('confluence'):
                    await fetch(CONFLUENCE_KEY, {'attached': {'confluence': remote['confluence']}}, {}, None)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                message = f'Could not prepare what was attached to this run ({launch_context.targets(remote)}): {exc}'
                await launch_context.Emitter(sink)('error', message=message[:2000], will_retry=False)
                raise RuntimeError(message) from exc
            atomic_json(path, validate(files))
    finally:
        if owner: _release_owner(owner)
    return json.loads(await asyncio.to_thread(path.read_text, encoding='utf-8'))


def uses_fleet(flow, configured, engine=None):
    return engine is not None or configured is not None or bool(flow.worker) or any(s.worker for s in flow.steps.values()) or bool(getattr(flow, 'environment', None))


async def _delete_job(client, url, gate_id):
    async with client.delete(url, headers={'X-Flow-Gate-ID': gate_id}) as response:
        try: result = await response.json(content_type=None)
        except (ValueError, aiohttp.ClientError): result = None
        if response.status != 200 or not isinstance(result, dict) or result.get('terminated') is not True:
            raise RuntimeError(f'Worker cancellation did not confirm termination ({response.status})')


def _waiting(decision):
    if decision['status'] == 'blocked':  # e.g. software disappeared from every registered worker
        return {'state': 'waiting', 'waiting_reason': 'intervention_needed', 'intervention_needed': True,
                'waiting': decision['reasons'], 'message': decision['message'], 'resolution': decision['resolution']}
    return {'state': 'waiting', 'waiting_reason': decision['waiting_reason'], 'intervention_needed': False,
            'waiting': decision['reasons'], 'message': decision['message'], 'resolution': None}


async def wait_for_worker(requirements, record, cancel_check=lambda: None):
    """Wait, cancellably, until a compatible worker is available; the reason is recorded meanwhile."""
    probed = {}
    while True:
        cancel_check()
        public, private = await fleet_status([requirements], probed, PROBE_INTERVAL)
        cancel_check()
        decision = validate_launch(requirements, public)
        if decision['status'] == 'ready':
            load = {w['id']: ((w.get('active') or 0) / (w.get('slots') or 1), w['id']) for w in public}
            return private[min(decision['candidates'], key=load.get)]
        record(_waiting(decision))
        await asyncio.sleep(PLACEMENT_POLL)


async def validate_flow_launch(flow, configured=None, engine=None):
    """Launch check for every step that the fleet places: blocked, wait or ready."""
    if not uses_fleet(flow, configured, engine):
        return {'status': 'ready', 'fleet': False, 'steps': {}}
    routes = routing(configured or {}, flow.steps)
    needs = {sid: with_engine(requirements_for(flow, sid, routes), engine) for sid in flow.steps}
    public, _ = await fleet_status(list(needs.values()))
    with_last_known(public)
    steps = {sid: validate_launch(req, public) for sid, req in needs.items()}
    states = {d['status'] for d in steps.values()}
    status = 'blocked' if 'blocked' in states else 'wait' if 'wait' in states else 'ready'
    return {'status': status, 'fleet': True, 'steps': steps}


async def validate_card_launch(agent, selection, engine=None, command=None):
    """Launch check for a standalone card placed on the fleet: its selection plus its agent or program."""
    from .definition import AGENTS
    from .fleet_matching import PROGRAM, card_software
    wanted = {k: v for k, v in selector(selection).items() if v}
    if not isinstance(selection, dict) or not selection: raise ValueError('Choose a worker, a pool or tags to place a card on the fleet')
    if agent not in (*AGENTS, PROGRAM): raise ValueError(f"agent must be one of {', '.join(AGENTS)}")
    requirements = with_engine({**wanted, **card_software(agent, command)}, engine)
    public, _ = await fleet_status([requirements])
    decision = validate_launch(requirements, with_last_known(public))
    return {'status': decision['status'], 'fleet': True, 'card': decision}


async def execute_step(flow, step_id, context, run_id, receive, sink, engine=None):
    """Run one flow step: here for flows without fleet placement, otherwise on a placed worker.
    `engine` names the engine adapter that runs the step's task (hatchet, prefect, trigger)."""
    from .executor import run_step
    configured = context.get('_flow_execution')
    attached = context.get('_flow_context')
    if not uses_fleet(flow, configured, engine) and not attached:
        return await run_step(flow, step_id, context, run_id, receive, sink)
    root = prepare_workspace(flow, run_id, context.get('_flow_restart'))
    pin_definition(root, flow)  # one run, one revision, whichever engine worker serves its steps
    plan = context.get('_flow_restart')
    if plan and step_id not in descendants(flow, plan['step']):
        output = read_result(root, step_id)
        await sink({'type': 'step', 'state': 'done', 'kind': flow.steps[step_id].kind, 'reused': True})
        return output
    wanted = selection_for(flow,step_id,configured)
    override = root/'.flow-selections'/f'{step_id}.json'
    if override.exists(): wanted = json.loads(override.read_text(encoding='utf-8'))
    requirements = with_engine(requirements_for(flow, step_id, selection=wanted), engine)
    if requirements.get('ambiguous_command'):
        raise ValueError(f'Step {step_id}: the program executable cannot be determined safely; declare requires_command')
    from .launch_context import note
    base = await attached_files(root, attached, wanted, engine, receive, sink)
    before = await asyncio.to_thread(ancestor_files, flow, step_id, root, base)
    # One run owns one asset snapshot. Restart copies it with the immutable publications.
    with publication_lock(root, '_assets'):
        asset_path=root/'.flow-assets.json'
        if not asset_path.exists():
            ref = getattr(flow, '_asset_snapshot', None)
            if ref is not None:
                from .activation import Store
                assets = Store().snapshot_assets(ref)
            else:
                assets = snapshot(FLOWS_DIR)  # direct library callers without an active revision
            atomic_json(asset_path, assets)
    assets=json.loads(await asyncio.to_thread(asset_path.read_text,encoding='utf-8'))
    # The job carries this exact revision and the run's files: a worker never reads either locally.
    native_context = {**context, '_flow_restart': None, '_flow_execution': None, '_flow_context': None, '_context_note': note(attached)}
    payload = {'files': before, 'assets': assets,
               'start': {'flow': flow.to_json(), 'step': step_id, 'context': native_context, 'runId': run_id}}

    def finish(data):
        publish(root, step_id, before, data['files'])
        save_result(flow, run_id, step_id, data['output'])

    return await _placed(root, step_id, requirements, wanted, payload, flow.steps[step_id], receive, sink, engine, finish)


CARD = 'card'  # execution record key of a standalone card (`/execution/<run>` -> steps.card)
CARD_TIMEOUT = SimpleNamespace(timeout_minutes=1440)  # the engines bound a card at 24 hours


async def execute_session(config, run_id, receive, sink, engine=None):
    """Run a standalone agent card. Without a `worker` selection it runs here exactly as before;
    with one it is placed like a flow step: same requirement object, wait, admission gate and
    fencing, and its execution record shows the executing PC, engine and waiting reason. A placed
    card runs in a new workspace on that PC: host paths and resumed sessions are refused."""
    from .fleet_matching import PROGRAM, card_requirements
    from . import launch_context
    config = dict(config)
    wanted = selector(config.pop('worker', None))
    attached = config.pop('context', None)
    program = config.get('agent') == PROGRAM
    if not program:
        config.pop('programInput', None)
        if launch_context.has_sources(attached): config['goal'] += '\n\n' + launch_context.note(attached)
    root = workspace_path(run_id)
    if not wanted:
        from agentdriver.session import run_session
        if not program and not launch_context.has_sources(attached):
            return await run_session(config, receive, sink)
        workspace = Path(config.get('workspace') or root)
        workspace.mkdir(parents=True, exist_ok=True)
        if attached: await launch_context.prepare_locally(attached, workspace, sink)
        if program: return await launch_context.run_program(config, workspace, sink)
        return await run_session({**config, 'workspace': str(workspace)}, receive, sink)
    requirements = with_engine(card_requirements(config, wanted), engine)
    root.mkdir(parents=True, exist_ok=True)
    files = await attached_files(root, attached, wanted, engine, receive, sink)
    if program:
        relay = launch_context.CardRelay(sink, summary=config['goal'])
        start = {**launch_context.program_job(config['goal'], config.get('programInput') or ''), 'runId': run_id}
        try:
            output = await _placed(root, CARD, requirements, wanted, {'files': files, 'assets': {}, 'start': start}, CARD_TIMEOUT,
                                   receive, relay.forward, engine, lambda data: None, announce=False)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await relay.failed(exc)
            raise
        return await relay.done(output)
    if launch_context.has_sources(attached): config['attached'] = True
    payload = {'files': files, 'assets': {}, 'start': {'session': config, 'runId': run_id}}
    return await _placed(root, CARD, requirements, wanted, payload, CARD_TIMEOUT, receive, sink, engine, lambda data: None,
                         announce=False)  # a card transcript keeps its event schema; the PC is in the record


async def _claim_attempt(root, key, job_id, allow_unsubmitted=False):
    """Claim a step, first cancelling an earlier remotely submitted attempt if necessary.

    A successful DELETE joins the worker's execution and cleanup. An unreachable worker or a
    missing job is uncertain: its process may have survived a service restart, so retain the fence.
    """
    from .workers import registry
    fence = root / '.flow-attempts' / f'{key}.json'
    record_path = root / '.flow-workers' / f'{key}.json'
    with publication_lock(root, key):
        if not fence.exists():
            atomic_json(fence, job_id)
            return fence
        old = json.loads(fence.read_text(encoding='utf-8'))
        try: record = json.loads(record_path.read_text(encoding='utf-8'))
        except FileNotFoundError: record = {}
        except (OSError, ValueError) as exc:
            raise RuntimeError('Previous attempt record cannot be read; its execution state is unknown') from exc
        if not isinstance(record, dict):
            raise RuntimeError('Previous attempt record is invalid; its execution state is unknown')
        worker_id = record.get('submitted_worker') if record.get('job') == old else None
    if not worker_id and allow_unsubmitted:
        # The previous process no longer holds the owner lock. It persisted a worker ID
        # before every submit, so a missing matching record means no remote POST was sent.
        with publication_lock(root, key):
            if fence.exists() and json.loads(fence.read_text(encoding='utf-8')) == old:
                atomic_json(fence, job_id)
                return fence
        raise RuntimeError('Previous attempt changed while recovering')
    worker = next((w for w in registry() if w['id'] == worker_id and w.get('url') and not w.get('_error')), None)
    if not worker:
        raise RuntimeError('A previous attempt may still be running; its worker cannot be confirmed')
    try:
        target = json.loads((root / '.flow-attempt-targets' / f'{old}.json').read_text(encoding='utf-8'))
    except (OSError, ValueError) as exc:
        raise RuntimeError('Previous attempt endpoint cannot be confirmed') from exc
    if not isinstance(target, dict) or target.get('worker') != worker_id or target.get('url') != worker['url']:
        raise RuntimeError('Previous attempt endpoint changed; its execution state is unknown')
    try:
        async with aiohttp.ClientSession(headers=worker['_headers'], timeout=aiohttp.ClientTimeout(total=10, connect=5)) as client:
            async with client.get(worker['url'] + '/health') as response:
                health = await response.json()
                if response.status != 200 or health.get('id') != worker_id or not target.get('gate_id') or (health.get('capacity') or {}).get('gate_id') != target['gate_id']:
                    raise RuntimeError('Previous worker identity could not be confirmed')
            await _delete_job(client, worker['url'] + '/jobs/' + old, target['gate_id'])
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as exc:
        raise RuntimeError('A previous attempt may still be running; worker cancellation could not be confirmed') from exc
    with publication_lock(root, key):
        if fence.exists() and json.loads(fence.read_text(encoding='utf-8')) != old:
            raise RuntimeError('Previous attempt changed while cancelling; retry the operation')
        atomic_json(fence, job_id)
    return fence


async def reconcile_source_attempts(run_id):
    """Stop any known remote writers before building a restart from a terminal engine run."""
    root = workspace_path(run_id)
    for path in (root / '.flow-attempts').glob('*.json'):
        key, claim = path.stem, str(uuid.uuid4())
        owner = _try_owner(root, key)
        try:
            fence = await _claim_attempt(root, key, claim, allow_unsubmitted=owner is not None)
            if owner is None:
                deadline = time.monotonic() + 10
                while owner is None and time.monotonic() < deadline:
                    await asyncio.sleep(.1)
                    owner = _try_owner(root, key)
                if owner is None: raise RuntimeError('Previous attempt did not release ownership after cancellation')
            with publication_lock(root, key):
                if fence.exists() and json.loads(fence.read_text(encoding='utf-8')) == claim:
                    fence.unlink()
        finally:
            if owner is not None: _release_owner(owner)


def _try_owner(root, key):
    directory = root / '.flow-owners'
    directory.mkdir(parents=True, exist_ok=True)
    handle = (directory / f'{key}.lock').open('a+b')
    handle.seek(0); handle.write(b'0'); handle.flush(); handle.seek(0)
    try:
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return handle
    except OSError:
        handle.close()
        return None


def _release_owner(handle):
    try:
        handle.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    finally:
        handle.close()


async def _placed(root, key, requirements, wanted, payload, bound, receive, sink, engine, finish, announce=True):
    owner = _try_owner(root, key)
    claim = None
    if owner is None:
        # A live owner may already have submitted work; terminate it before superseding.
        claim = str(uuid.uuid4())
        await _claim_attempt(root, key, claim)
        deadline = time.monotonic() + 10
        while owner is None and time.monotonic() < deadline:
            await asyncio.sleep(.1)
            owner = _try_owner(root, key)
        if owner is None: raise RuntimeError('Previous attempt did not release ownership after cancellation')
    try:
        return await _run_placed(root, key, requirements, wanted, payload, bound, receive, sink, engine, finish, announce, claim)
    finally:
        _release_owner(owner)


async def _run_placed(root, key, requirements, wanted, payload, bound, receive, sink, engine, finish, announce, claim):
    """Wait for a compatible worker, submit through its admission gate and follow the job.
    `finish(data)` publishes a finished job's results while this attempt's fence is held."""
    since_wall, since_monotonic = time.time(), time.monotonic()
    attempt = {'job': claim or str(uuid.uuid4())}
    fence = root / '.flow-attempts' / f'{key}.json' if claim else await _claim_attempt(root, key, attempt['job'], allow_unsubmitted=True)
    info = {'state': 'queued', 'requested': wanted, 'worker': None, 'job': attempt['job'], 'waiting_reason': None, 'engine': engine}
    info_path = root/'.flow-workers'/f'{key}.json'

    def record(changes=None, **fields):
        """Persist visible state for the current attempt only (a superseded attempt writes nothing)."""
        updated = {**info, **(changes or {}), **fields}
        with publication_lock(root, key):
            if json.loads(fence.read_text()) != attempt['job']: return
            if updated != info or not info_path.exists():
                info.clear(); info.update(updated)
                atomic_json(info_path, info)

    job = _Job(root, key, fence, info, record, payload, receive, sink, finish, announce)
    record()
    try:
        while True:
            worker = await wait_for_worker(requirements, record, lambda: check_cancelled(root))
            try:
                return await job.run(worker, attempt['job'])
            except _Replace as exc:
                previous, next_job = attempt['job'], str(uuid.uuid4())
                if not job.safe_to_replace:
                    # The submission may have arrived. Keep its fence and retry only that
                    # worker until it confirms termination, never allocate another PC early.
                    while True:
                        check_cancelled(root)
                        with publication_lock(root, key): job.current(previous)
                        record(state='waiting', waiting_reason='offline', message='Waiting for the previous worker to confirm the attempt has stopped')
                        try:
                            await _claim_attempt(root, key, next_job)
                            break
                        except RuntimeError:
                            await asyncio.sleep(PLACEMENT_POLL)
                    attempt['job'] = next_job
                    job.safe_to_replace = True
                else:
                    with publication_lock(root, key):
                        if json.loads(fence.read_text()) != previous: raise RuntimeError('Execution superseded by another attempt') from None
                        atomic_json(fence, next_job)
                    attempt['job'] = next_job
                record(state='waiting', worker=None, job=attempt['job'], submitted_worker=None, waiting_reason=exc.reason, message=str(exc))
                await asyncio.sleep(PLACEMENT_POLL)  # no tight loop if the same worker rejects again
    except asyncio.CancelledError:
        # Keep the last waiting reason and message: they explain why a step never ran.
        record(state=cancellation_state(root, bound, since_wall, since_monotonic), timeout_minutes=bound.timeout_minutes)
        raise
    except BaseException:
        if info.get('state') != 'done': record(state='failed', waiting_reason=None)
        raise
    finally:
        if job.safe_to_replace:
            with publication_lock(root, key):
                if fence.exists() and json.loads(fence.read_text()) == attempt['job']: fence.unlink()


class _Job:
    def __init__(self, root, key, fence, info, record, payload, receive, sink, finish, announce):
        self.root, self.key, self.fence, self.info, self.record = root, key, fence, info, record
        self.payload, self.receive, self.sink, self.finish, self.announce = payload, receive, sink, finish, announce
        self.safe_to_replace = True

    def current(self, job_id):
        if json.loads(self.fence.read_text()) != job_id: raise RuntimeError('Execution superseded by another attempt')

    async def run(self, worker, job_id):
        root, key, info, record, sink = self.root, self.key, self.info, self.record, self.sink
        url = worker['url']+'/jobs/'+job_id
        # `authorize`: after admission the service waits for this coordinator to confirm the start.
        config = {'id': job_id, 'authorize': True, 'gate_id': worker['_gate_id'], 'settings_revision': worker.get('settings_revision', 0), **self.payload}
        record(state='queued', worker=None, waiting_reason=None, intervention_needed=False, waiting=[], message=None, resolution=None)
        async with aiohttp.ClientSession(headers=worker['_headers'], timeout=aiohttp.ClientTimeout(total=120, connect=5)) as client:
            async def call(method, endpoint, data=None, timeout=None):
                async with client.request(method, endpoint, json=data, **({'timeout': aiohttp.ClientTimeout(total=timeout)} if timeout else {})) as response:
                    result = await response.json(content_type=None)
                    if response.status >= 400: raise RuntimeError((result or {}).get('error') or f'Execution worker returned {response.status}')
                    return result
            async def inputs():
                while True: await call('POST', url+'/input', await self.receive())
            input_task = None
            finished = False
            authorized, granted = False, None
            evidence = False  # the worker reported the job started (it may ignore `authorize`)
            unreachable = f"{worker['id']} became unreachable before the step started"
            try:
                check_cancelled(root)
                # Store the endpoint privately before submission; it must not follow a later
                # registry edit to a different machine with the same configured worker ID.
                with publication_lock(root, key):
                    self.current(job_id)
                    atomic_json(root / '.flow-attempt-targets' / f'{job_id}.json', {'worker': worker['id'], 'url': worker['url'], 'gate_id': worker['_gate_id']})
                record(submitted_worker=worker['id'])
                self.safe_to_replace = False  # the submit may arrive even if the response is lost
                try: await call('POST', worker['url']+'/jobs', config)
                except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc: raise _Replace('offline', unreachable) from exc
                input_task = asyncio.create_task(inputs())
                cursor = 0
                while True:
                    check_cancelled(root)
                    try: data = await call('GET', url+f'?after={cursor}')
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, RuntimeError) as exc:
                        if authorized or evidence: raise
                        raise _Replace('offline', unreachable) from exc  # never authorized, so never started
                    cursor = data['cursor']
                    if data.get('worker') or data['state'] in ('running', 'done') or data['events'] or cursor:
                        evidence = True  # e.g. an older service that starts at once: never placed again
                    if data['state'] == 'rejected':
                        if evidence: raise RuntimeError(data.get('error') or 'The worker rejected a step it reported as started')
                        raise _Replace(data.get('reason') or 'software_missing', data.get('error') or 'The worker rejected the step')
                    if data['state'] == 'queued' and not data.get('worker'):
                        record(waiting_reason=data.get('waiting_reason'))
                    if data.get('authorization') and data['authorization'] != granted:
                        # The decision and the confirmation are one step under both locks: a board
                        # cancellation (cancel lock) and a settings edit (registry lock) either
                        # complete first, so this attempt sees them, or wait for the start and
                        # then apply to a job that is already running.
                        from .fleet_registry import _locked, _read
                        async with held(lambda: publication_lock(root, '_cancel'), _locked):
                            current_worker = next((w for w in _read() if w['id'] == worker['id']), None)
                            if not current_worker or current_worker.get('settings_revision', 0) != worker.get('settings_revision', 0) or current_worker.get('url', '').rstrip('/') != worker['url']:
                                raise _Replace('settings_changed', 'Worker settings changed before execution')
                            # Each admission shows a fresh nonce; confirm it only while this attempt is current.
                            with publication_lock(root, key):
                                self.current(job_id)
                                check_cancelled(root)
                            previously, authorized, granted = authorized, True, data['authorization']  # may start: never placed again
                            try: await call('POST', url+'/start', {'nonce': granted}, timeout=START_TIMEOUT)
                            except RuntimeError: authorized, granted = previously, None  # explicitly refused: not started by this
                            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError): pass  # may have arrived: assume it did
                    if data.get('worker') and info.get('worker')!=data['worker']:
                        record(state='running', worker=data['worker'], waiting_reason=None)
                        if self.announce: await sink({'type':'worker','worker':data['worker']})
                    for event in data['events']:
                        if event.get('type') == 'worker':
                            if info.get('worker')==event['worker']: continue
                            record(state='running', worker=event['worker'], waiting_reason=None)
                            if not self.announce: continue
                        await sink(event)
                    if data['state'] == 'done':
                        with publication_lock(root, key):
                            self.current(job_id)
                            self.finish(data)
                            info['state'] = 'done'
                            atomic_json(root/'.flow-workers'/f'{key}.json', info)
                        finished = True
                        self.safe_to_replace = True
                        return data['output']
                    if data['state'] in ('failed', 'cancelled'):
                        with publication_lock(root, key): self.current(job_id)
                        if data['state'] == 'cancelled' and not authorized and not evidence:
                            raise _Replace('offline', data.get('error') or 'The worker cancelled the queued step')
                        raise RuntimeError(data.get('error') or 'Remote step failed')
                    if input_task.done(): input_task.result()
                    await asyncio.sleep(RELAYED_POLL if is_relayed(url) else .2)
            finally:
                if input_task:
                    input_task.cancel()
                    await asyncio.gather(input_task, return_exceptions=True)
                if not finished:
                    try:
                        await asyncio.wait_for(_delete_job(client, url, worker['_gate_id']), timeout=10)
                        self.safe_to_replace = True
                    except Exception: pass
