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
    path = Path(os.environ.get('FLOW_WORKERS_FILE', ROOT / 'workers.local.yaml'))
    entries = yaml.safe_load(path.read_text(encoding='utf-8')) if path.exists() else {
        'workers': [{'id': os.environ.get('FLOW_WORKER_ID', 'local'), 'pool': 'default', 'tags': [], 'url': 'http://127.0.0.1:8096', 'token_env': 'FLOW_WORKER_TOKEN'}]}
    workers, seen = [], set()
    if not isinstance(entries, dict) or not isinstance(entries.get('workers'), list): raise ValueError('Worker registry needs a workers list')
    for item in entries['workers']:
        if not isinstance(item, dict): raise ValueError('Invalid registry worker')
        selector({k: item[k] for k in ('id', 'pool', 'tags') if k in item})
        if not item.get('id') or item['id'] in seen: raise ValueError('Worker ids must be present and unique')
        seen.add(item['id'])
        url = urlsplit(item.get('url', ''))
        if url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password or url.query or url.fragment:
            raise ValueError('Invalid execution-worker endpoint')
        token = {**dotenv_values(ROOT/'workers.local.env'), **os.environ}.get(item.get('token_env', ''), '')
        if url.hostname not in ('127.0.0.1', 'localhost', '::1') and not token:
            raise ValueError(f'Network worker {item["id"]} needs token_env credentials')
        workers.append({**item, 'url': item['url'].rstrip('/'), '_headers': {'Authorization': f'Bearer {token}'} if token else {}})
    return workers


async def catalog():
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=3)) as client:
        async def health(worker):
            public = {k: worker.get(k, [] if k == 'tags' else 'default') for k in ('id', 'pool', 'tags')}
            try:
                async with client.get(worker['url'] + '/health', headers=worker['_headers']) as response:
                    data = await response.json()
                    if response.status != 200 or data.get('id') != worker['id']: raise ValueError('Worker identity mismatch')
                    if data.get('pool') != public['pool'] or sorted(data.get('tags', [])) != sorted(public['tags']): raise ValueError('Worker pool/tags differ from registry')
                    return {**public, **{k: data[k] for k in ('hostname', 'slots', 'active')}, 'online': True}
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, KeyError):
                return {**public, 'online': False}
        return await asyncio.gather(*(health(w) for w in registry()))


async def choose(wanted):
    wanted = selector(wanted)
    statuses = {w['id']: w for w in await catalog()}
    candidates = [w for w in registry() if statuses[w['id']]['online'] and
                  (not wanted.get('id') or wanted['id'] == w['id']) and
                  (not wanted.get('pool') or wanted['pool'] == w.get('pool', 'default')) and
                  set(wanted.get('tags', [])) <= set(w.get('tags', []))]
    if not candidates: raise ValueError(f'No online execution worker matches {wanted or "the default selection"}')
    return min(candidates, key=lambda w: (statuses[w['id']]['active'] / statuses[w['id']]['slots'], w['id']))
