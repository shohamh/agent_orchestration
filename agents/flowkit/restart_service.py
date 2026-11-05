"""Backend control plane for the restart extension. The board forwards IDs only."""
import asyncio
import os
from pathlib import Path

import aiohttp
from dotenv import dotenv_values

from .activation import load_active
from .restart import build_plan

ROOT = Path(__file__).resolve().parents[2]
FINAL_TRIGGER = {'COMPLETED', 'CANCELED', 'CANCELLED', 'FAILED', 'CRASHED', 'SYSTEM_FAILURE', 'TIMED_OUT', 'EXPIRED', 'INTERRUPTED'}


async def restart_node(system, run_id, ref):
    config = {**dotenv_values(ROOT / 'board' / '.env'), **os.environ}
    flows, _ = load_active("prefect" if system == "prefect" else "trigger")
    if system == 'prefect':
        base = (config.get('FLOW_PREFECT_API_URL') or os.environ.get('PREFECT_API_URL') or 'http://localhost:4200/api').rstrip('/')
        headers = {}
    elif system == 'trigger-dev':
        base = (config.get('FLOW_TRIGGER_API_URL') or os.environ.get('TRIGGER_API_URL') or 'http://localhost:8030').rstrip('/')
        key = config.get('TRIGGER_DEV_SECRET_KEY')
        if not key: raise ValueError('The flow host needs TRIGGER_DEV_SECRET_KEY in board/.env or its environment')
        headers = {'Authorization': f'Bearer {key}'}
    else:
        raise ValueError('The restart extension supports Prefect and local Trigger.dev flows')
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as client:
        async def call(method, path, body=None, params=None):
            async with client.request(method, base + path, json=body, params=params) as response:
                data = await response.json(content_type=None)
                if response.status >= 400:
                    raise RuntimeError(f'{system} {method} {path} failed ({response.status})')
                return data
        if system == 'prefect':
            run = await call('GET', f'/flow_runs/{run_id}')
            if run.get('state_type') not in ('COMPLETED', 'FAILED', 'CANCELLED', 'CRASHED'):
                raise ValueError('Stop or finish the flow before restarting a step')
            name = (await call('GET', f"/flows/{run['flow_id']}"))['name']
            flow = flows.get(name)
            if flow is None: raise ValueError('The source flow definition is unavailable')
            tasks, offset = [], 0
            while True:
                page = await call('POST', '/task_runs/filter', {'flow_runs': {'id': {'any_': [run_id]}}, 'sort': 'EXPECTED_START_TIME_ASC', 'limit': 200, 'offset': offset})
                tasks.extend(page)
                if len(page) < 200: break
                offset += len(page)
            nodes = {}
            for task in sorted(tasks, key=lambda t: (t.get('expected_start_time') or t.get('created') or '', t.get('created') or '', t['id'])):
                if task.get('state_type') not in ('COMPLETED', 'FAILED', 'CANCELLED', 'CRASHED'):
                    raise ValueError('A source step is still active; stop or finish every step first')
                step = task['name'] if task['name'] in flow.steps else task['name'].rsplit('-', 1)[0]
                if step in flow.steps:
                    status = task.get('state_type')
                    nodes[step] = {'ref': task['id'], 'column': 'done' if status == 'COMPLETED' else 'stopped' if status in ('FAILED', 'CANCELLED', 'CRASHED') else 'running'}
            params, tags, title = run.get('parameters') or {}, run.get('tags') or [], run['name']
        else:
            run = await call('GET', f'/api/v3/runs/{run_id}')
            if run['status'] not in FINAL_TRIGGER:
                raise ValueError('Stop or finish the flow before restarting a step')
            name = run['taskIdentifier'].removeprefix('flow-')
            flow = flows.get(name)
            if flow is None: raise ValueError('The source flow definition is unavailable')
            ids = (run.get('metadata') or {}).get('steps') or {}
            # A parent can crash between child trigger and metadata flush. Reconcile engine
            # child records and the native run tag so those writers cannot escape preflight.
            actual = {c['id'] for c in (run.get('relatedRuns') or {}).get('children', [])}
            cursor = None
            while True:
                query = {'filter[tag]': f'flowrun:{run_id}', 'page[size]': '100'}
                if cursor: query['page[after]'] = cursor
                listed = await call('GET', '/api/v1/runs', params=query)
                actual.update(c['id'] for c in listed.get('data', []))
                next_cursor = (listed.get('pagination') or {}).get('next')
                if not next_cursor or next_cursor == cursor: break
                cursor = next_cursor
            actual.update(ids.values())
            all_children = await asyncio.gather(*(call('GET', f'/api/v3/runs/{rid}') for rid in sorted(actual)))
            if any(c['status'] not in FINAL_TRIGGER for c in all_children):
                raise ValueError('A source step is still active; stop or finish every step first')
            if actual - set(ids.values()):
                raise ValueError('The engine has unrecorded source steps; restart the whole flow')
            by_id = {c['id']: c for c in all_children}
            children = [by_id[rid] for rid in ids.values()]
            nodes = {step: {'ref': child['id'], 'column': 'done' if child['status'] == 'COMPLETED' else 'stopped' if child['status'] in FINAL_TRIGGER else 'running'}
                     for step, child in zip(ids, children)}
            params, tags, title = run.get('payload') or {}, run.get('tags') or [], (run.get('metadata') or {}).get('title') or name
        selected = next((sid for sid, node in nodes.items() if node['ref'] == ref), None)
        if selected is None: raise ValueError('Step does not belong to the source run')
        from .distributed import reconcile_source_attempts
        await reconcile_source_attempts(run_id)
        plan = build_plan(flow, selected, nodes, run_id)
        execution = params.get('flowExecution')
        params = {key: params.get(key, default) for key, default in flow.input.items()}
        if execution is not None: params['flowExecution'] = execution
        params['flowRestart'] = plan
        if system == 'prefect':
            deployment = await call('GET', f'/deployments/name/{name}/flows')
            created = await call('POST', f"/deployments/{deployment['id']}/create_flow_run", {
                'parameters': params, 'tags': tags, 'name': f'{title} · restart {selected}'[:100]})
        else:
            created = await call('POST', f'/api/v1/tasks/flow-{name}/trigger', {'payload': params, 'options': {
                'tags': tags, 'metadata': {'title': f'{title} · restart {selected}'[:512], 'restartedFrom': run_id}}})
        return {'id': created['id'], 'mode': 'runtime', 'source': run_id}
