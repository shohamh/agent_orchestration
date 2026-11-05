"""Trusted execution-worker registry; public catalog deliberately omits URLs and tokens."""
import asyncio
import json
import os
import threading
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
import yaml
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]
_CACHE_LOCK = threading.Lock()  # the flow host serves requests on threads
# A limits push that failed is not retried on every catalog call (placement polls every 2 s):
# worker id -> (wanted limits, monotonic retry time, result). A changed registry value retries now.
PUSH_RETRY_SECONDS = {'failed': 30, 'unsupported': 300}
_PUSH_BACKOFF = {}


def selector(value):
    if value is None: return {}
    if not isinstance(value, dict) or set(value) - {'id', 'pool', 'tags'}:
        raise ValueError('worker selector must contain id, pool and/or tags')
    for key in ('id', 'pool'):
        if key in value and (not isinstance(value[key], str) or not value[key] or len(value[key]) > 100):
            raise ValueError(f'worker {key} must be nonempty text (max 100)')
    if 'tags' in value and (not isinstance(value['tags'], list) or len(value['tags']) > 50 or any(not isinstance(t, str) or not t or len(t) > 100 for t in value['tags'])):
        raise ValueError('worker tags must be a list of at most 50 nonempty strings')
    return value.copy()


def routing(value, steps):
    if not isinstance(value, dict) or set(value) - {'default', 'steps'}:
        raise ValueError('execution must contain default and/or steps selectors')
    overrides = value.get('steps', {})
    if not isinstance(overrides, dict) or set(overrides) - set(steps): raise ValueError('Unknown step in worker selection')
    return {'default': selector(value.get('default')), 'steps': {sid: selector(v) for sid, v in overrides.items()}}


def selection_for(flow, step, configured):
    routes=routing(configured or {},flow.steps)
    base=routes['default'] or flow.worker
    specific=routes['steps'].get(step,flow.steps[step].worker)
    if specific.get('id'): return specific
    result={**base,**specific}
    if specific.get('pool'): result.pop('id',None)
    return result


def registry():
    from .fleet_registry import _read
    entries = {'workers': _read()}
    workers, seen = [], set()
    if not isinstance(entries, dict) or not isinstance(entries.get('workers'), list): raise ValueError('Worker registry needs a workers list')
    for item in entries['workers']:
        if not isinstance(item, dict): raise ValueError('Invalid registry worker')
        selector({k: item[k] for k in ('id', 'tags') if k in item})
        if not item.get('id') or item['id'] in seen: raise ValueError('Worker ids must be present and unique')
        seen.add(item['id'])
        url = urlsplit(item.get('url', ''))
        if not item.get('url'):
            workers.append({**item, '_headers': {}})
            continue
        if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError('Invalid execution-worker endpoint')
        token = {**dotenv_values(ROOT/'workers.local.env'), **os.environ}.get(item.get('token_env', ''), '')
        if url.hostname not in ('127.0.0.1', 'localhost', '::1') and not token:
            workers.append({**item, '_headers': {}, '_error': 'Worker credentials unavailable'})
            continue
        workers.append({**item, 'url': item['url'].rstrip('/'), '_headers': {'Authorization': f'Bearer {token}'} if token else {}})
    return workers


class _Mismatch(ValueError):
    pass


def limits(worker):
    """The registry's admission limits for a worker: the only settings pushed to its gate."""
    return {k: worker.get(k, default) for k, default in (('slots', 1), ('cpu_limit', 90), ('ram_limit', 90))}


async def push_limits(client, worker, capacity):
    """Bring a connected worker's capacity gate in line with the registry. The gate applies
    limits to future admissions only; running work is never interrupted."""
    import time
    wanted = limits(worker)
    revision = worker.get('settings_revision', 0)
    if isinstance(capacity, dict) and capacity.get('settings') == wanted and capacity.get('settings_revision') == revision:
        _PUSH_BACKOFF.pop(worker['id'], None)
        return {'state': 'applied'}
    held = _PUSH_BACKOFF.get(worker['id'])
    versioned = (wanted, revision)
    if held and held[0] == versioned and time.monotonic() < held[1]:
        return held[2]
    result = await _push(client, worker, wanted, capacity)
    if result['state'] == 'applied': _PUSH_BACKOFF.pop(worker['id'], None)
    else: _PUSH_BACKOFF[worker['id']] = (versioned, time.monotonic() + PUSH_RETRY_SECONDS[result['state']], result)
    return result


async def _push(client, worker, wanted, capacity):
    try:
        gate_id = capacity.get('gate_id') if isinstance(capacity, dict) else None
        headers = {**worker['_headers'], 'X-Flow-Gate-ID': gate_id or 'unavailable', 'X-Flow-Settings-Revision': str(worker.get('settings_revision', 0))}
        async with client.post(worker['url'] + '/capacity/settings', json=wanted, headers=headers) as response:
            if response.status == 404:
                return {'state': 'unsupported', 'message': 'This execution service is older than limit settings; update it on the PC'}
            data = await response.json(content_type=None)
            if response.status != 200:
                return {'state': 'failed', 'message': str((data or {}).get('error') or f'status {response.status}')[:300]}
        if isinstance(capacity, dict):
            capacity['settings'] = wanted
            capacity['settings_revision'] = worker.get('settings_revision', 0)
        return {'state': 'applied'}
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError):
        return {'state': 'failed', 'message': 'The worker did not accept the limits; retried on the next check'}


async def catalog(snapshot=None):
    """Sanitized registry entries with verified online state. `status` is online,
    awaiting_connection (no endpoint yet), credentials_unavailable, unauthorized,
    identity_mismatch or unreachable. Connected workers get the registry limits pushed when
    their gate reports different ones (on connect, reconnect or after a registry save)."""
    from .fleet_registry import ENGINES
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as client:
        async def health(worker):
            public = {k: worker[k] for k in ('id', 'pools', 'tags', 'slots', 'cpu_limit', 'ram_limit')}
            public.update({'pool': worker['pools'][0], 'engines': list(worker.get('engines') or ENGINES),
                           'connection': 'configured' if worker.get('url') else 'awaiting'})
            from .fleet_registry import warnings
            if warnings(worker): public['warnings'] = warnings(worker)
            pending = {'settings_sync': {'state': 'pending'}}
            try:
                if worker.get('_error'): return {**public, 'online': False, 'reason': worker['_error'], 'status': 'credentials_unavailable', **pending}
                if not worker.get('url'): return {**public, 'online': False, 'status': 'awaiting_connection', **pending}
                async with client.get(worker['url'] + '/health', headers=worker['_headers']) as response:
                    if response.status in (401, 403):
                        return {**public, 'online': False, 'status': 'unauthorized', **pending}
                    data = await response.json()
                    if response.status != 200: raise ValueError('Worker health failed')
                    if data.get('id') != worker['id']: raise _Mismatch('Worker identity mismatch')
                    # Registry settings are authoritative; legacy worker-reported pool and tags are informational.
                    result = {**public, **{k: data[k] for k in ('hostname', 'active')}, 'online': True, 'status': 'online',
                              **({'capacity': data['capacity']} if isinstance(data.get('capacity'), dict) else {})}
                result['settings_sync'] = await push_limits(client, worker, result.get('capacity'))
                return result
            except _Mismatch:
                return {**public, 'online': False, 'status': 'identity_mismatch', **pending}
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError):
                return {**public, 'online': False, 'status': 'unreachable', **pending}
        return await asyncio.gather(*(health(w) for w in (snapshot if snapshot is not None else registry())))


async def sync_limits(worker_id):
    """After a registry save: verify the worker's identity, then push its limits if it is connected."""
    entry = next((w for w in registry() if w['id'] == worker_id), None)
    if entry is None: return {'state': 'pending'}
    return (await catalog([entry]))[0]['settings_sync']


def _cache_path():
    from .fleet_registry import _path
    return Path(str(_path()) + '.inventory.json')


def _cached_inventory():
    try:
        data = json.loads(_cache_path().read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def remember_inventory(worker_id, inventory):
    """Keep the last sanitized capability results so an offline PC still shows when it was checked."""
    from .fleet_registry import _read
    from .restart import atomic_json
    try:
        with _CACHE_LOCK:
            known = {w['id'] for w in _read()}
            cache = {k: v for k, v in _cached_inventory().items() if k in known}
            cache[worker_id] = {k: v for k, v in inventory.items() if k != 'cached'}
            atomic_json(_cache_path(), cache)
    except (OSError, ValueError):
        pass  # a read-only registry folder only loses the offline history


def with_last_known(public):
    """Launch checks show when an offline PC last reported its software (display only)."""
    cache = _cached_inventory()
    for worker in public:
        if not worker.get('inventory') and isinstance(cache.get(worker['id']), dict):
            worker['last_known'] = _sanitized_inventory(cache[worker['id']])
    return public


def engine_status(worker, engine, coordinators):
    """Every engine reaches the PC through its execution service (native placement is not
    enabled). state: connected when the PC is online and that engine's coordinator worker runs;
    unconfirmed when the PC is online but the coordinator worker cannot be checked (Trigger.dev's
    `trigger dev` runs outside the flow host); not_connected otherwise."""
    coordinator = (coordinators or {}).get(engine, 'unknown')
    if not worker.get('online') or coordinator in ('stopped', 'not_enabled'): state = 'not_connected'
    elif coordinator == 'running': state = 'connected'
    else: state = 'unconfirmed'
    return {'route': 'shared_service', 'coordinator': coordinator, 'state': state, 'connected': state == 'connected'}


async def fleet_view(coordinators=None, inventory=True):
    """The board's fleet listing: sanitized workers with online state, live capacity, limits
    sync, per-engine status and (unless only metrics are wanted) capability results, the last
    known ones when a PC is offline. Never endpoints, token names or tokens."""
    from .fleet_registry import ENGINES, tag_catalog
    if inventory:
        public, _ = await fleet_status()
        cache = _cached_inventory()
    else:
        public = await catalog()
    for worker in public:
        worker.pop('reason', None)  # status carries it as a code
        worker['engine_status'] = {e: engine_status(worker, e, coordinators) for e in worker['engines']}
        if not inventory: continue
        if 'inventory' in worker:
            worker['inventory'] = {**worker['inventory'], 'cached': False}
            remember_inventory(worker['id'], worker['inventory'])
        elif isinstance(cache.get(worker['id']), dict):
            worker['inventory'] = {**_sanitized_inventory(cache[worker['id']]), 'cached': True}
    result = {'workers': public, 'engines': list(ENGINES)}
    if inventory:
        result.update(tags=tag_catalog(), pools=sorted({p for w in public for p in w['pools']}))
    return result


async def refresh_capabilities(worker_id):
    """Run a capability check on one connected worker now; returns the sanitized inventory."""
    entry = next((w for w in registry() if w['id'] == worker_id), None)
    if entry is None: raise LookupError(f'Unknown worker {worker_id}')
    status = (await catalog([entry]))[0]
    if not status['online']:
        raise ConnectionError(f'{worker_id} is not connected ({status["status"].replace("_", " ")}); '
                              'its capabilities are checked when it connects')
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60)) as client:
        async with client.post(entry['url'] + '/capabilities/refresh', json={'commands': []}, headers=entry['_headers']) as response:
            if response.status != 200: raise ConnectionError(f'{worker_id} could not refresh its capabilities (status {response.status})')
            inventory = _sanitized_inventory(await response.json())
    remember_inventory(worker_id, inventory)
    return {**inventory, 'cached': False}


async def choose(wanted):
    wanted = selector(wanted)
    snapshot = registry()
    statuses = {w['id']: w for w in await catalog(snapshot)}
    candidates = [w for w in snapshot if statuses[w['id']]['online'] and
                  (not wanted.get('id') or wanted['id'] == w['id']) and
                  (not wanted.get('pool') or wanted['pool'] in w['pools']) and
                  set(wanted.get('tags', [])) <= set(w.get('tags', []))]
    if not candidates: raise ValueError(f'No online execution worker matches {wanted or "the default selection"}')
    return min(candidates, key=lambda w: (statuses[w['id']]['active'] / statuses[w['id']]['slots'], w['id']))


def _inventory_entry(data, kind, name):
    """A malformed report is unknown for that entry, never an error for the fleet."""
    section = data.get(kind + 's') if isinstance(data, dict) else None
    value = section.get(name) if isinstance(section, dict) else None
    return value if isinstance(value, dict) else {}


def _sanitized_inventory(data):
    if not isinstance(data, dict): return {}
    keep = lambda section: {name: {k: v[k] for k in ('status', 'checked_at', 'auth_status') if k in v}
                            for name, v in (data.get(section) if isinstance(data.get(section), dict) else {}).items()
                            if isinstance(name, str) and isinstance(v, dict)}
    return {'agents': keep('agents'), 'commands': keep('commands'), 'refreshed_at': data.get('refreshed_at')}


async def fleet_status(requirements=(), probed=None, probe_interval=0):
    """Sanitized workers with online state, capacity and software inventory for matching, plus
    the private registry entries (endpoint and headers) by id for dispatch. A required command
    or agent that a worker has not freshly reported installed is probed on demand, at most once
    per `probe_interval` seconds per worker when `probed` (worker id -> monotonic time) is given."""
    import time
    from .fleet_matching import _fresh
    from datetime import datetime, timezone
    snapshot = registry()
    public = await catalog(snapshot)
    private = {w['id']: w for w in snapshot}
    for entry in public:
        private[entry['id']]['_gate_id'] = (entry.get('capacity') or {}).get('gate_id')
    commands = sorted({r['command'] for r in requirements if r.get('command')})
    agents = sorted({r['agent'] for r in requirements if r.get('agent')})
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as client:
        async def inventory(entry):
            worker = private[entry['id']]
            if not entry.get('online'): return
            base, headers = worker['url'], worker['_headers']
            try:
                async with client.get(base + '/capabilities', headers=headers) as response:
                    data = await response.json() if response.status == 200 else {}
                now = datetime.now(timezone.utc)
                found = lambda kind, name: _inventory_entry(data, kind, name)
                missing = [c for c in commands if found('command', c).get('status') != 'installed' or not _fresh(found('command', c), now)]
                stale_agent = any(found('agent', a).get('status') != 'installed' or not _fresh(found('agent', a), now) for a in agents)
                due = probed is None or time.monotonic() - probed.get(entry['id'], float('-inf')) >= probe_interval
                if (missing or stale_agent) and due:
                    if probed is not None: probed[entry['id']] = time.monotonic()
                    async with client.post(base + '/capabilities/refresh', json={'commands': missing}, headers=headers) as response:
                        if response.status == 200: data = await response.json()
                entry['inventory'] = _sanitized_inventory(data)
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError, AttributeError):
                entry.pop('inventory', None)  # unknown, never absent
        await asyncio.gather(*(inventory(e) for e in public))
    return public, private
