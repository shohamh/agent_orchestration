"""Step placement and transfer extension. Native engines still invoke tasks in DAG order."""
import asyncio
import json
import os
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import aiohttp

from .artifacts import ancestor_files, snapshot, publish
from .definition import FLOWS_DIR
from .restart import prepare_workspace, read_result, save_result, atomic_json, descendants, workspace_path, definition_hash
from .fleet_matching import requirements_for, validate_launch
from .workers import fleet_status, routing, selection_for, selector

PLACEMENT_POLL = 2.0  # seconds between placement checks while a step waits for a worker
PROBE_INTERVAL = 15.0  # minimum seconds between on-demand capability probes of one worker
TIMEOUT_SLACK = 5.0  # the engine's step timeout starts slightly before this coroutine does


@contextmanager
def publication_lock(root, step):
    directory = root/'.flow-locks'
    directory.mkdir(parents=True, exist_ok=True)
    with (directory/f'{step}.lock').open('a+b') as file:
        file.seek(0)
        file.write(b'0'); file.flush(); file.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(file.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(file.fileno(), fcntl.LOCK_EX)
        try: yield
        finally:
            file.seek(0)
            if os.name == 'nt': msvcrt.locking(file.fileno(), msvcrt.LK_UNLCK, 1)
            else: fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def execution_info(run_id):
    root = workspace_path(run_id)
    result = {}
    for path in (root/'.flow-workers').glob('*.json'):
        try: result[path.stem] = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError): pass
    return result


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
    """The board's cancel API records a user cancellation before cancelling the engine run."""
    root = workspace_path(run_id)
    if not root.is_dir(): return False
    atomic_json(root/'.flow-cancel.json', {'at': time.time()})
    return True


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


class _Replace(Exception):
    """The submitted job was never authorized to start, so the step can be placed again."""
    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason


def uses_fleet(flow, configured):
    return configured is not None or bool(flow.worker) or any(s.worker for s in flow.steps.values()) or bool(getattr(flow, 'environment', None))


async def _delete_job(client, url):
    async with client.delete(url) as response: await response.read()


def _waiting(decision):
    if decision['status'] == 'blocked':  # e.g. software disappeared from every registered worker
        return {'state': 'waiting', 'waiting_reason': 'intervention_needed', 'intervention_needed': True,
                'waiting': decision['reasons'], 'message': decision['message'], 'resolution': decision['resolution']}
    return {'state': 'waiting', 'waiting_reason': decision['waiting_reason'], 'intervention_needed': False,
            'waiting': decision['reasons'], 'message': decision['message'], 'resolution': None}


async def wait_for_worker(requirements, record):
    """Wait, cancellably, until a compatible worker is available; the reason is recorded meanwhile."""
    probed = {}
    while True:
        public, private = await fleet_status([requirements], probed, PROBE_INTERVAL)
        decision = validate_launch(requirements, public)
        if decision['status'] == 'ready':
            load = {w['id']: ((w.get('active') or 0) / (w.get('slots') or 1), w['id']) for w in public}
            return private[min(decision['candidates'], key=load.get)]
        record(_waiting(decision))
        await asyncio.sleep(PLACEMENT_POLL)


async def validate_flow_launch(flow, configured=None):
    """Launch check for every step that the fleet places: blocked, wait or ready."""
    if not uses_fleet(flow, configured):
        return {'status': 'ready', 'fleet': False, 'steps': {}}
    routes = routing(configured or {}, flow.steps)
    needs = {sid: requirements_for(flow, sid, routes) for sid in flow.steps}
    public, _ = await fleet_status(list(needs.values()))
    steps = {sid: validate_launch(req, public) for sid, req in needs.items()}
    states = {d['status'] for d in steps.values()}
    status = 'blocked' if 'blocked' in states else 'wait' if 'wait' in states else 'ready'
    return {'status': status, 'fleet': True, 'steps': steps}


async def execute_step(flow, step_id, context, run_id, receive, sink):
    from .executor import run_step
    configured = context.get('_flow_execution')
    if not uses_fleet(flow, configured):
        return await run_step(flow, step_id, context, run_id, receive, sink)
    root = prepare_workspace(flow, run_id, context.get('_flow_restart'))
    manifest=root/'.flow-results/_definition.json'
    if manifest.exists() and json.loads(manifest.read_text(encoding='utf-8')).get('hash')!=definition_hash(flow):
        raise ValueError('The flow definition has changed; start a new flow run')
    plan = context.get('_flow_restart')
    if plan and step_id not in descendants(flow, plan['step']):
        output = read_result(root, step_id)
        await sink({'type': 'step', 'state': 'done', 'kind': flow.steps[step_id].kind, 'reused': True})
        return output
    wanted = selection_for(flow,step_id,configured)
    override = root/'.flow-selections'/f'{step_id}.json'
    if override.exists(): wanted = json.loads(override.read_text(encoding='utf-8'))
    requirements = requirements_for(flow, step_id, selection=wanted)
    if requirements.get('ambiguous_command'):
        raise ValueError(f'Step {step_id}: the program executable cannot be determined safely; declare requires_command')
    before = await asyncio.to_thread(ancestor_files, flow, step_id, root)
    # One run owns one asset snapshot. Restart copies it with the immutable publications.
    with publication_lock(root, '_assets'):
        asset_path=root/'.flow-assets.json'
        if not asset_path.exists(): atomic_json(asset_path,snapshot(FLOWS_DIR))
    assets=json.loads(await asyncio.to_thread(asset_path.read_text,encoding='utf-8'))
    since_wall, since_monotonic = time.time(), time.monotonic()
    attempt = {'job': str(uuid.uuid4())}
    fence = root/'.flow-attempts'/f'{step_id}.json'
    with publication_lock(root, step_id): atomic_json(fence, attempt['job'])
    info = {'state': 'queued', 'requested': wanted, 'worker': None, 'job': attempt['job'], 'waiting_reason': None}
    info_path = root/'.flow-workers'/f'{step_id}.json'

    def record(changes=None, **fields):
        """Persist visible state for the current attempt only (a superseded attempt writes nothing)."""
        updated = {**info, **(changes or {}), **fields}
        with publication_lock(root, step_id):
            if json.loads(fence.read_text()) != attempt['job']: return
            if updated != info or not info_path.exists():
                info.clear(); info.update(updated)
                atomic_json(info_path, info)

    record()
    try:
        while True:
            worker = await wait_for_worker(requirements, record)
            try:
                return await _run_job(flow, step_id, context, run_id, receive, sink, root, worker, attempt['job'], fence, info, record, before, assets)
            except _Replace as exc:
                previous, attempt['job'] = attempt['job'], str(uuid.uuid4())
                with publication_lock(root, step_id):
                    if json.loads(fence.read_text()) != previous: raise RuntimeError('Execution superseded by another attempt') from None
                    atomic_json(fence, attempt['job'])
                record(state='waiting', worker=None, job=attempt['job'], waiting_reason=exc.reason, message=str(exc))
                await asyncio.sleep(PLACEMENT_POLL)  # no tight loop if the same worker rejects again
    except asyncio.CancelledError:
        # Keep the last waiting reason and message: they explain why a step never ran.
        record(state=cancellation_state(root, flow.steps[step_id], since_wall, since_monotonic),
               timeout_minutes=flow.steps[step_id].timeout_minutes)
        raise
    except BaseException:
        if info.get('state') != 'done': record(state='failed', waiting_reason=None)
        raise


async def _run_job(flow, step_id, context, run_id, receive, sink, root, worker, job_id, fence, info, record, before, assets):
    url = worker['url']+'/jobs/'+job_id
    native_context = {**context, '_flow_restart': None, '_flow_execution': None}
    # `authorize`: after admission the service waits for this coordinator to confirm the start.
    config = {'id': job_id, 'authorize': True, 'files': before, 'assets': assets, 'start': {
        'flow': flow.to_json(), 'step': step_id, 'context': native_context, 'runId': run_id}}
    record(state='queued', worker=None, waiting_reason=None, intervention_needed=False, waiting=[], message=None, resolution=None)
    async with aiohttp.ClientSession(headers=worker['_headers'], timeout=aiohttp.ClientTimeout(total=120, connect=5)) as client:
        async def call(method, endpoint, data=None):
            async with client.request(method, endpoint, json=data) as response:
                result = await response.json(content_type=None)
                if response.status >= 400: raise RuntimeError((result or {}).get('error') or f'Execution worker returned {response.status}')
                return result
        async def inputs():
            while True: await call('POST', url+'/input', await receive())
        input_task = None
        finished = False
        authorized, granted = False, None
        evidence = False  # the worker reported the job started (it may ignore `authorize`)
        unreachable = f"{worker['id']} became unreachable before the step started"
        try:
            try: await call('POST', worker['url']+'/jobs', config)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc: raise _Replace('offline', unreachable) from exc
            input_task = asyncio.create_task(inputs())
            cursor = 0
            while True:
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
                    # Each admission shows a fresh nonce; confirm it only while this attempt is current.
                    with publication_lock(root, step_id):
                        if json.loads(fence.read_text()) != job_id: raise RuntimeError('Execution superseded by another attempt')
                    previously, authorized, granted = authorized, True, data['authorization']  # may start: never placed again
                    try: await call('POST', url+'/start', {'nonce': granted})
                    except RuntimeError: authorized, granted = previously, None  # explicitly refused: not started by this
                    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError): pass  # may have arrived: assume it did
                if data.get('worker') and info.get('worker')!=data['worker']:
                    record(state='running', worker=data['worker'], waiting_reason=None)
                    await sink({'type':'worker','worker':data['worker']})
                for event in data['events']:
                    if event.get('type') == 'worker':
                        if info.get('worker')==event['worker']: continue
                        record(state='running', worker=event['worker'], waiting_reason=None)
                    await sink(event)
                if data['state'] == 'done':
                    with publication_lock(root, step_id):
                        if json.loads(fence.read_text()) != job_id: raise RuntimeError('Execution superseded by another attempt')
                        publish(root, step_id, before, data['files'])
                        save_result(flow, run_id, step_id, data['output'])
                        info['state'] = 'done'
                        atomic_json(root/'.flow-workers'/f'{step_id}.json', info)
                    finished = True
                    return data['output']
                if data['state'] in ('failed', 'cancelled'):
                    if data['state'] == 'cancelled' and not authorized and not evidence:
                        raise _Replace('offline', data.get('error') or 'The worker cancelled the queued step')
                    raise RuntimeError(data.get('error') or 'Remote step failed')
                if input_task.done(): input_task.result()
                await asyncio.sleep(.2)
        finally:
            if input_task:
                input_task.cancel()
                await asyncio.gather(input_task, return_exceptions=True)
            if not finished:
                try: await asyncio.wait_for(_delete_job(client, url), timeout=10)
                except Exception: pass
