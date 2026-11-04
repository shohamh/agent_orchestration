"""Trusted execution-worker registry; public catalog deliberately omits URLs and tokens."""
import asyncio
import os
from pathlib import Path
from urllib.parse import urlsplit

import aiohttp
import yaml
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[2]


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


async def catalog(snapshot=None):
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as client:
        async def health(worker):
            public = {k: worker[k] for k in ('id', 'pools', 'tags', 'slots', 'cpu_limit', 'ram_limit')}
            public.update({'pool': worker['pools'][0], 'engines': {}})
            try:
                if worker.get('_error'): return {**public, 'online': False, 'reason': worker['_error']}
                if not worker.get('url'): return {**public, 'online': False}
                async with client.get(worker['url'] + '/health', headers=worker['_headers']) as response:
                    data = await response.json()
                    if response.status != 200 or data.get('id') != worker['id']: raise ValueError('Worker identity mismatch')
                    # Registry settings are authoritative; legacy worker-reported pool and tags are informational.
                    return {**public, **{k: data[k] for k in ('hostname', 'active')}, 'online': True,
                            **({'capacity': data['capacity']} if isinstance(data.get('capacity'), dict) else {})}
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError):
                return {**public, 'online': False}
        return await asyncio.gather(*(health(w) for w in (snapshot if snapshot is not None else registry())))


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
