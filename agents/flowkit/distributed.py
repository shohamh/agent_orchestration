"""Step placement and transfer extension. Native engines still invoke tasks in DAG order."""
import asyncio
import json
import os
import uuid
from contextlib import contextmanager
from pathlib import Path

import aiohttp

from .artifacts import ancestor_files, snapshot, publish
from .definition import FLOWS_DIR
from .restart import prepare_workspace, read_result, save_result, atomic_json, descendants, workspace_path, definition_hash
from .workers import choose, selection_for, selector


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


async def execute_step(flow, step_id, context, run_id, receive, sink):
    from .executor import run_step
    configured = context.get('_flow_execution')
    if configured is None and not flow.worker and not any(s.worker for s in flow.steps.values()) and not getattr(flow, 'environment', None):
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
    worker = await choose(wanted)
    before = await asyncio.to_thread(ancestor_files, flow, step_id, root)
    # One run owns one asset snapshot. Restart copies it with the immutable publications.
    with publication_lock(root, '_assets'):
        asset_path=root/'.flow-assets.json'
        if not asset_path.exists(): atomic_json(asset_path,snapshot(FLOWS_DIR))
    assets=json.loads(await asyncio.to_thread(asset_path.read_text,encoding='utf-8'))
    job_id = str(uuid.uuid4())
    fence = root/'.flow-attempts'/f'{step_id}.json'
    with publication_lock(root, step_id): atomic_json(fence, job_id)
    info = {'state': 'queued', 'requested': wanted, 'worker': None, 'job': job_id}
    info_path = root/'.flow-workers'/f'{step_id}.json'
    atomic_json(info_path, info)
    url = worker['url']+'/jobs/'+job_id
    native_context = {**context, '_flow_restart': None, '_flow_execution': None}
    config = {'id': job_id, 'files': before, 'assets': assets, 'start': {
        'flow': flow.to_json(), 'step': step_id, 'context': native_context, 'runId': run_id}}
    async with aiohttp.ClientSession(headers=worker['_headers'], timeout=aiohttp.ClientTimeout(total=120, connect=5)) as client:
        async def call(method, endpoint, data=None):
            async with client.request(method, endpoint, json=data) as response:
                result = await response.json()
                if response.status >= 400: raise RuntimeError(result.get('error') or f'Execution worker returned {response.status}')
                return result
        async def inputs():
            while True: await call('POST', url+'/input', await receive())
        input_task = None
        finished = False
        try:
            await call('POST', worker['url']+'/jobs', config)
            input_task = asyncio.create_task(inputs())
            cursor = 0
            while True:
                data = await call('GET', url+f'?after={cursor}')
                cursor = data['cursor']
                if data.get('worker') and info.get('worker')!=data['worker']:
                    info.update(state='running',worker=data['worker'])
                    atomic_json(info_path,info)
                    await sink({'type':'worker','worker':data['worker']})
                for event in data['events']:
                    if event.get('type') == 'worker':
                        if info.get('worker')==event['worker']: continue
                        info.update(state='running',worker=event['worker'])
                        atomic_json(info_path,info)
                    await sink(event)
                if data['state'] == 'done':
                    with publication_lock(root, step_id):
                        if json.loads(fence.read_text()) != job_id: raise RuntimeError('Execution superseded by another attempt')
                        publish(root, step_id, before, data['files'])
                        save_result(flow, run_id, step_id, data['output'])
                        info['state'] = 'done'
                        atomic_json(info_path, info)
                    finished = True
                    return data['output']
                if data['state'] in ('failed', 'cancelled'): raise RuntimeError(data.get('error') or 'Remote step failed')
                if input_task.done(): input_task.result()
                await asyncio.sleep(.2)
        except BaseException:
            info['state'] = 'failed'
            with publication_lock(root, step_id):
                if json.loads(fence.read_text()) == job_id: atomic_json(info_path, info)
            raise
        finally:
            if input_task:
                input_task.cancel()
                await asyncio.gather(input_task, return_exceptions=True)
            if not finished:
                try: await asyncio.wait_for(call('DELETE', url), timeout=10)
                except Exception: pass
