"""Restart extension for engines without native dependent-task invalidation.

Lives with the orchestration runtime, independently of the board. Plans contain a selected
step and source run, never reconstructed log previews. Every backend still schedules tasks
through its normal dependency mechanism; unaffected tasks return their exact saved results.
"""
import hashlib
import json
import os
import shutil
import tempfile
import time
from pathlib import Path


def workspace_path(run_id):
    base = Path(os.environ.get('FLOW_WORKSPACES') or Path(tempfile.gettempdir()) / 'agent-flows')
    if not run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_' for c in run_id):
        raise ValueError('Invalid flow run id')
    return base / run_id


def definition_hash(flow):
    return hashlib.sha256(json.dumps(flow.to_json(), sort_keys=True).encode()).hexdigest()


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8', dir=path.parent, delete=False) as file:
        temporary = Path(file.name)
        json.dump(value, file, ensure_ascii=False, allow_nan=False)
    try:
        for attempt in range(40):
            try:
                temporary.replace(path)
                break
            except PermissionError:  # Windows: a reader (the board polling execution info) has the target open
                if attempt == 39:
                    raise
                time.sleep(0.05)
    finally:
        temporary.unlink(missing_ok=True)


def save_result(flow, run_id, step, output):
    directory = workspace_path(run_id) / '.flow-results'
    atomic_json(directory / '_definition.json', {'hash': definition_hash(flow)})
    atomic_json(directory / f'{step}.json', {'output': output})


def read_result(workspace, step):
    try:
        return json.loads((workspace / '.flow-results' / f'{step}.json').read_text(encoding='utf-8'))['output']
    except (OSError, ValueError, KeyError):
        raise ValueError(f'Exact result for step {step} is unavailable; restart the whole flow') from None


def descendants(flow, step):
    found = {step}
    while True:
        extra = {s.id for s in flow.steps.values() if found.intersection(s.needs)}
        if extra <= found: return found
        found.update(extra)


def validate_source(flow, source):
    path = workspace_path(source)
    if not path.is_dir():
        raise ValueError('The original workspace is unavailable on this worker; restart the whole flow')
    try:
        saved = json.loads((path / '.flow-results' / '_definition.json').read_text(encoding='utf-8'))
    except (OSError, ValueError):
        raise ValueError('Source definition is unavailable; restart the whole flow') from None
    if saved.get('hash') != definition_hash(flow):
        raise ValueError('The flow definition has changed; restart the whole flow')
    return path


def build_plan(flow, step, nodes, source):
    if step not in flow.steps:
        raise ValueError('Unknown step')
    if any(n['column'] not in ('done', 'stopped', 'pending') for n in nodes.values()):
        raise ValueError('A source step is still active; stop or finish every step first')
    path = validate_source(flow, source)
    rerun = descendants(flow, step)
    for sid in flow.steps:
        if sid in rerun: continue
        if nodes.get(sid, {}).get('column') != 'done':
            raise ValueError(f'Step {sid} has no successful result; restart an earlier step')
        read_result(path, sid)  # null is a valid result; absent or clipped output never is
    return {'step': step, 'source_run_id': source, 'definition_hash': definition_hash(flow)}


def prepare_workspace(flow, run_id, plan=None):
    path = workspace_path(run_id)
    if not plan:
        path.mkdir(parents=True, exist_ok=True)
        return path
    if plan.get('definition_hash') != definition_hash(flow):
        raise ValueError('The flow definition has changed since the restart was requested')
    source = validate_source(flow, plan['source_run_id'])
    if path.exists(): return path
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='restart-', dir=path.parent) as directory:
        staged = Path(directory) / 'workspace'
        shutil.copytree(source, staged)
        try:
            staged.rename(path)  # publish one complete workspace across parallel Python processes
        except OSError:
            if not path.is_dir(): raise
    return path
