"""Placement requirements and matching. Unknown capability or availability is never treated as
verified absence: only a fresh `absent` result or a selection mismatch makes a worker incompatible.

A requirement object is a worker selector (`id`, `pool`, `pools` (any of them), `tags` (all of them)) plus the step's detected
software (`agent` or `command`), or `ambiguous_command` when a program's executable cannot be
determined safely, and `engine` when the engine running the task is known: a worker serves only
the engines registered for it (legacy entries without `engines` serve every engine). Worker objects are the sanitized fleet entries (no URL or credentials) with
`online`, `active`, `capacity` and `inventory` where known.
"""
from datetime import datetime, timezone

STALE_SECONDS = 86400  # a capability result older than the daily refresh is unknown again
# Most actionable first; the first applicable one is the step's headline waiting reason.
WAITING_ORDER = ('offline', 'credentials_unavailable', 'native_engine_unavailable', 'gate_unavailable', 'full_slots', 'high_cpu', 'high_ram', 'missing_metrics',
                 'software_unknown', 'software_stale')
RESOLUTIONS = {
    'no_workers': 'Register a worker in the fleet tab.',
    'unknown_worker': 'Register that worker in the fleet tab or select another worker.',
    'pool': 'Add a worker to the selected pool in the fleet tab, or select another pool.',
    'software_absent': 'Install the required software on a matching worker, then refresh its capabilities.',
    'ambiguous_command': 'Add requires_command with the executable this program step runs.',
    'native_engine_unavailable': 'The selected PC’s native engine worker must be connected and ready before it can receive work.',
}
ENGINE_LABELS = {'hatchet': 'Hatchet', 'prefect': 'Prefect', 'trigger': 'Trigger.dev'}


def with_engine(requirements, engine):
    """The requirement object for a task that a known engine runs."""
    return {**requirements, 'engine': engine} if engine else dict(requirements)


def software_requirement(step):
    if step.kind == 'agent': return {'agent': step.agent}
    if step.requires_command: return {'command': step.requires_command}
    from .capabilities import infer_command, shell_builtin
    command = infer_command(step.run)
    if command: return {'command': command}
    if shell_builtin(step.run): return {}
    return {'ambiguous_command': True}


def requirements_for(flow, step_id, configured=None, selection=None):
    from .workers import selection_for
    wanted = selection if selection is not None else selection_for(flow, step_id, configured)
    return {**{k: v for k, v in wanted.items() if v}, **software_requirement(flow.steps[step_id])}


CARD_FIELDS = {'goal', 'agent', 'workspace', 'simulateFailures', 'resumeSessionId', 'programInput', 'attached'}
PROGRAM = 'program'  # the `agent` of a program card, whose goal is the command it runs
MAX_CARD_GOAL = 30000  # the board allows 20000; the rest is the note about attached sources


def card_session(config):
    """A standalone card's session options as a placed worker accepts them. A placed card runs in
    a new workspace on that PC, so a host path or a resumed session (bound to its original
    workspace) is refused rather than assumed to exist there."""
    from .definition import AGENTS
    if not isinstance(config, dict) or set(config) - CARD_FIELDS:
        raise ValueError('A card session contains only goal, agent, workspace, simulateFailures, resumeSessionId, programInput and attached')
    if config.get('workspace') or config.get('resumeSessionId'):
        raise ValueError('A card placed on a fleet worker runs in a new workspace on that worker; '
                         'clear the workspace (host paths and resumed sessions are not available on other PCs)')
    if not isinstance(config.get('goal'), str) or not config['goal'].strip() or len(config['goal']) > MAX_CARD_GOAL:
        raise ValueError('A card needs a goal of at most 20000 characters')
    if config.get('agent', 'codex') not in (*AGENTS, PROGRAM):
        raise ValueError(f"agent must be one of {', '.join(AGENTS)}")
    failures = config.get('simulateFailures', 0)
    if type(failures) is not int or not 0 <= failures <= 20:
        raise ValueError('simulateFailures must be a whole number from 0 to 20')
    return config


def card_requirements(config, selection):
    """The requirement object of a standalone card: its worker selection plus its agent."""
    card_session(config)
    return {**{k: v for k, v in selection.items() if v}, **card_software(config.get('agent', 'codex'), config['goal'])}


def card_software(agent, command=None):
    """What a card needs installed: its agent, or the program it runs."""
    if agent != PROGRAM: return {'agent': agent}
    from .launch_context import program_requirement
    return program_requirement(command)


def _reason(code, message, **extra):
    return {'code': code, 'message': message, **extra}


def _fresh(entry, now):
    try:
        checked = datetime.fromisoformat(entry['checked_at'])
        return checked.tzinfo is not None and 0 <= (now - checked).total_seconds() < STALE_SECONDS
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def match_worker(worker, requirements, now=None):
    now = now or datetime.now(timezone.utc)
    wid = worker.get('id')
    incompatible, unknown = [], []
    if requirements.get('id') and requirements['id'] != wid:
        incompatible.append(_reason('worker_id', f"The step is assigned to worker {requirements['id']}, not {wid}"))
    pool = requirements.get('pool')
    if pool and pool not in (worker.get('pools') or []):
        incompatible.append(_reason('pool', f'{wid} is not in pool {pool}', pool=pool))
    pools = requirements.get('pools') or []
    if pools and not set(pools) & set(worker.get('pools') or []):
        incompatible.append(_reason('pool', f"{wid} is not in {'pool' if len(pools) == 1 else 'any of the pools'} {', '.join(pools)}", pools=pools))
    missing = [t for t in requirements.get('tags') or [] if t not in (worker.get('tags') or [])]
    if missing:
        incompatible.append(_reason('tags', f"{wid} lacks tags: {', '.join(missing)}", missing=missing))
    if requirements.get('ambiguous_command'):
        incompatible.append(_reason('ambiguous_command', 'The program executable cannot be determined safely'))
    engine, engines = requirements.get('engine'), worker.get('engines')
    if engine and isinstance(engines, list) and engine not in engines:
        incompatible.append(_reason('engine', f'{wid} is not registered for {ENGINE_LABELS.get(engine, engine)}', engine=engine))
    if engine and engine in (worker.get('native_engines') or []):
        native = worker.get('_native_worker_status') if isinstance(worker.get('_native_worker_status'), dict) else {}
        if not isinstance(native.get(engine), dict) or native[engine].get('state') != 'ready':
            unknown.append(_reason('native_engine_unavailable', f'{wid} has not connected its native {ENGINE_LABELS.get(engine, engine)} worker', engine=engine))
    for kind in ('agent', 'command'):
        name = requirements.get(kind)
        if not name: continue
        inventory = worker.get('inventory') if isinstance(worker.get('inventory'), dict) else {}
        section = inventory.get(kind + 's') if isinstance(inventory.get(kind + 's'), dict) else {}
        found = section.get(name) if isinstance(section.get(name), dict) else {}  # malformed: unknown
        fresh = _fresh(found, now)
        if found.get('status') == 'absent' and fresh:
            incompatible.append(_reason('software_absent', f"{name} is not installed on {wid} (checked {found['checked_at']})",
                                        software=name, checked_at=found['checked_at']))
        elif found.get('status') == 'installed' and not fresh:
            unknown.append(_reason('software_stale', f'{name} on {wid} needs a fresh capability check', software=name))
        elif found.get('status') != 'installed':
            checked = f" (last checked {found['checked_at']})" if found.get('checked_at') else ''
            unknown.append(_reason('software_unknown', f'Whether {name} is installed on {wid} is unknown{checked}', software=name))
    if not incompatible:
        if not worker.get('online'):
            if worker.get('reason') == 'Worker credentials unavailable':
                unknown.append(_reason('credentials_unavailable', f'{wid} has no usable credentials on the coordinator'))
            elif worker.get('status') == 'updating':
                unknown.append(_reason('offline', f'{wid} is updating its worker and takes no new work until it is done'))
            else:
                unknown.append(_reason('offline', f'{wid} is offline'))
        else:
            sync = worker.get('settings_sync')
            capacity = worker.get('capacity')
            if not isinstance(sync, dict) or sync.get('state') != 'applied' or not isinstance(capacity, dict) or not capacity.get('gate_id'):
                unknown.append(_reason('gate_unavailable', f'{wid} cannot verify its admission gate and registry limits; check or update the worker service'))
            if (worker.get('active') or 0) >= (worker.get('slots') or 1):
                unknown.append(_reason('full_slots', f'{wid} has no free slot'))
            capacity = worker.get('capacity')
            if isinstance(capacity, dict):  # admission itself is enforced again by the worker's gate
                if capacity.get('state') != 'known':
                    unknown.append(_reason('missing_metrics', f'{wid} is waiting for a complete 10-second CPU/RAM window'))
                else:
                    if capacity['cpu_percent'] >= worker.get('cpu_limit', 90):
                        unknown.append(_reason('high_cpu', f"{wid} CPU {capacity['cpu_percent']:.0f}% is at or above its limit"))
                    if capacity['ram_percent'] >= worker.get('ram_limit', 90):
                        unknown.append(_reason('high_ram', f"{wid} RAM {capacity['ram_percent']:.0f}% is at or above its limit"))
    status = 'incompatible' if incompatible else 'unknown' if unknown else 'compatible'
    return {'worker': wid, 'status': status, 'reasons': incompatible or unknown, 'detected': detected(worker, requirements)}


def detected(worker, requirements):
    """The required software's last capability result on this worker, for display. An offline
    worker shows its `last_known` (cached) result; that never counts as a verified answer."""
    inventory, cached = worker.get('inventory'), False
    if not (isinstance(inventory, dict) and inventory) and isinstance(worker.get('last_known'), dict):
        inventory, cached = worker['last_known'], True
    inventory = inventory if isinstance(inventory, dict) else {}
    result = []
    for kind in ('agent', 'command'):
        name = requirements.get(kind)
        if not name: continue
        section = inventory.get(kind + 's') if isinstance(inventory.get(kind + 's'), dict) else {}
        found = section.get(name) if isinstance(section.get(name), dict) else {}
        status = found.get('status') if isinstance(found.get('status'), str) else 'unknown'
        checked = found.get('checked_at') if isinstance(found.get('checked_at'), str) else None
        result.append({'kind': kind, 'name': name, 'status': status, 'checked_at': checked, 'cached': cached and bool(found)})
    return result


def _resolution(code, requirements):
    if code == 'tags':
        chosen = [requirements['pool']] if requirements.get('pool') else requirements.get('pools') or []
        where = f" in {'pool' if len(chosen) == 1 else 'pools'} {', '.join(chosen)}" if chosen else ''
        return f'Add the missing tags to a worker{where}, or remove them from the requirement.'
    if code == 'engine':
        label = ENGINE_LABELS.get(requirements.get('engine'), requirements.get('engine'))
        return f'Add {label} to a matching worker in the fleet tab, or run this on an engine the worker serves.'
    return RESOLUTIONS.get(code)


def _blocked(reasons, matches=(), requirements=None):
    codes = list(dict.fromkeys(r['code'] for r in reasons))
    texts = [_resolution(c, requirements or {}) for c in codes]
    resolution = ' '.join(t for t in texts if t) or RESOLUTIONS['no_workers']
    message = 'No registered worker can run this step: ' + '; '.join(r['message'] for r in reasons)
    return {'status': 'blocked', 'reasons': reasons, 'message': message, 'resolution': resolution,
            'candidates': [], 'workers': list(matches)}


def validate_launch(requirements, workers, now=None):
    """ready: a compatible worker is available now; wait: a potentially compatible worker is
    offline, busy or not freshly checked; blocked: no registered worker can satisfy the step."""
    if requirements.get('ambiguous_command'):
        return _blocked([_reason('ambiguous_command', 'The program executable cannot be determined safely; declare requires_command')])
    if requirements.get('id'):
        workers = [w for w in workers if w.get('id') == requirements['id']]
        if not workers:
            return _blocked([_reason('unknown_worker', f"Worker {requirements['id']} is not registered")])
    if not workers:
        return _blocked([_reason('no_workers', 'No workers are registered')])
    matches = [match_worker(w, requirements, now) for w in workers]
    compatible = [m['worker'] for m in matches if m['status'] == 'compatible']
    if compatible:
        return {'status': 'ready', 'reasons': [], 'candidates': compatible, 'workers': matches}
    waiting = [r for m in matches if m['status'] == 'unknown' for r in m['reasons']]
    if waiting:
        present = {r['code'] for r in waiting}
        headline = next((c for c in WAITING_ORDER if c in present), waiting[0]['code'])
        return {'status': 'wait', 'reasons': waiting, 'waiting_reason': headline,
                'message': 'Waiting: ' + '; '.join(r['message'] for r in waiting), 'candidates': [], 'workers': matches}
    return _blocked([r for m in matches for r in m['reasons']], matches, requirements)
