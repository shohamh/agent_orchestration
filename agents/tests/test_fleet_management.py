"""Fleet management API: registration, engines, tags, capability refresh, live metrics, and
registry limits pushed to each worker's capacity gate (future admissions only)."""
import asyncio
import json
import os
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from aiohttp.test_utils import TestClient, TestServer
from flowkit.capacity import CapacityGate
from flowkit.worker_service import make_app

TOKEN = 'top-secret-worker-token'


def probe(kind, name):
    status = 'installed' if name in ('demo', 'python') else 'absent'
    return {'status': status, 'checked_at': datetime.now(timezone.utc).isoformat(), 'auth_status': 'unknown'}


class Clock:
    def __init__(self): self.now = time.monotonic()
    def __call__(self): return self.now


def feed(gate, clock, cpu, ram, seconds=11):
    """A complete, contiguous 10-second window ending now (one sample per second)."""
    for i in range(seconds):
        gate.observe(cpu, ram, clock.now - seconds + 1 + i)


class Fixture(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.registry = self.root / 'workers.yaml'
        self.env = patch.dict(os.environ, {'FLOW_WORKERS_FILE': str(self.registry), 'FLOW_HOST_TOKEN': 'host-secret',
                                           'PC_A_TOKEN': TOKEN})
        self.env.start(); self.addCleanup(self.env.stop)
        # Machine samples come only from the test, never from this PC's real load.
        sampler = patch('flowkit.capacity.MachineSampler.sample', return_value=None)
        sampler.start(); self.addCleanup(sampler.stop)
        from flowkit import workers
        workers._PUSH_BACKOFF.clear(); workers._METADATA_PUSH_BACKOFF.clear()
        self.addCleanup(workers._PUSH_BACKOFF.clear); self.addCleanup(workers._METADATA_PUSH_BACKOFF.clear)
        import flow_host
        self.host = flow_host.serve(None, '127.0.0.1', 0)
        threading.Thread(target=self.host.serve_forever, daemon=True).start()
        self.addCleanup(self.host.server_close); self.addCleanup(self.host.shutdown)

    async def service(self, worker_id='pc-a', clock=None, slots=4):
        clock = clock or Clock()
        gate = CapacityGate(self.root / f'{worker_id}-capacity.db', slots=slots, clock=clock)
        app = make_app(worker_id=worker_id, root=self.root / worker_id, token=TOKEN, capacity=gate, probe=probe)
        server = TestServer(app); await server.start_server()
        self.addAsyncCleanup(server.close)
        return app, server, gate, clock

    def write_registry(self, workers, tags=None):
        data = {'version': 2, 'workers': workers}
        if tags is not None: data['tags'] = tags
        self.registry.write_text(yaml.safe_dump(data), encoding='utf-8')

    async def call(self, method, path, body=None, auth=True):
        def send():
            headers = {'Content-Type': 'application/json'}
            if auth: headers['Authorization'] = 'Bearer host-secret'
            request = urllib.request.Request(f'http://127.0.0.1:{self.host.server_port}{path}', method=method, headers=headers,
                                             data=None if body is None else json.dumps(body).encode())
            try:
                with urllib.request.urlopen(request, timeout=30) as response: return response.status, json.load(response)
            except urllib.error.HTTPError as error: return error.code, json.load(error)
        return await asyncio.to_thread(send)  # the test loop keeps serving the worker meanwhile


class ConfigureRouteTests(Fixture):
    async def test_configure_rejects_another_gate_identity(self):
        app, server, gate, _ = await self.service()
        before = gate.metrics()['settings']
        async with TestClient(server) as client:
            response = await client.post('/capacity/settings', json={'slots': 2, 'cpu_limit': 50, 'ram_limit': 60}, headers={'Authorization': f'Bearer {TOKEN}', 'X-Flow-Gate-ID': 'another-gate'})
            self.assertNotEqual(response.status, 200)
        self.assertEqual(gate.metrics()['settings'], before)

    async def test_configure_route_is_authenticated_validated_and_limits_only(self):
        app, server, gate, _ = await self.service()
        async with TestClient(server) as client:
            denied = await client.post('/capacity/settings', json={'slots': 2, 'cpu_limit': 50, 'ram_limit': 60})
            self.assertEqual(denied.status, 401)
            auth = {'Authorization': f'Bearer {TOKEN}'}
            for bad in ({'slots': 2, 'cpu_limit': 50, 'ram_limit': 60, 'url': 'http://x'}, {'slots': True, 'cpu_limit': 50, 'ram_limit': 60},
                        {'slots': 0, 'cpu_limit': 50, 'ram_limit': 60}, {'slots': 2, 'cpu_limit': 101, 'ram_limit': 60},
                        {'slots': 2, 'cpu_limit': 50}, [], {'slots': 101, 'cpu_limit': 50, 'ram_limit': 60}):
                response = await client.post('/capacity/settings', json=bad, headers=auth)
                self.assertEqual(response.status, 400, bad)
            response = await client.post('/capacity/settings', json={'slots': 2, 'cpu_limit': 50, 'ram_limit': 60.5}, headers=auth)
            self.assertEqual(response.status, 200)
            self.assertEqual(await response.json(), {'settings': {'slots': 2, 'cpu_limit': 50, 'ram_limit': 60.5}})
            health = await (await client.get('/health', headers=auth)).json()
        self.assertEqual(health['capacity']['settings'], {'slots': 2, 'cpu_limit': 50, 'ram_limit': 60.5})
        self.assertEqual(health['capacity']['leases'], 0)


class SettingsPushTests(Fixture):
    async def test_registry_save_pushes_limits_to_future_admissions_only(self):
        app, server, gate, clock = await self.service()
        self.write_registry([{'id': 'pc-a', 'pools': ['build'], 'slots': 4, 'url': str(server.make_url('')), 'token_env': 'PC_A_TOKEN'}])
        feed(gate, clock, cpu=50, ram=10)
        running = gate.try_acquire('running-job')
        self.assertTrue(running['acquired'])
        gate.confirm_started('running-job', running['fence'], os.getpid())
        clock.now += 11; feed(gate, clock, cpu=50, ram=10)  # past the post-start cooldown

        status, body = await self.call('PUT', '/fleet/workers/pc-a', {'cpu_limit': 40})
        self.assertEqual(status, 200, body)
        self.assertEqual(body['settings_sync']['state'], 'applied')
        self.assertEqual(gate.metrics()['settings'], {'slots': 4, 'cpu_limit': 40, 'ram_limit': 90})
        self.assertEqual(gate.try_acquire('new-job')['reason'], 'high_cpu')  # new work: refused
        self.assertTrue(gate.renew('running-job', running['fence']))  # running work: untouched

        status, body = await self.call('PUT', '/fleet/workers/pc-a', {'cpu_limit': 90, 'slots': 2})
        self.assertEqual((status, body['settings_sync']['state']), (200, 'applied'))
        self.assertTrue(gate.try_acquire('new-job')['acquired'])
        self.assertNotIn(TOKEN, json.dumps(body))
        self.assertNotIn('127.0.0.1', json.dumps(body))


    async def test_native_fleet_metadata_is_pushed_and_revisions_replace_removed_values(self):
        app, server, _, _ = await self.service()
        worker = {'id': 'pc-a', 'pools': ['build', 'gpu'], 'tags': ['linux'], 'engines': ['prefect'],
                  'native_engines': ['prefect'], 'native_config': {'prefect_worker_api_url': 'http://127.0.0.1:4200/api'},
                  'url': str(server.make_url('')), 'token_env': 'PC_A_TOKEN', 'settings_revision': 10}
        self.write_registry([worker])
        from flowkit.workers import catalog
        listed = (await catalog())[0]
        self.assertEqual(listed['metadata_sync']['state'], 'pending',
                         'the local service accepted metadata, but no Prefect process reported applying it')
        path = self.root / 'pc-a' / 'fleet-metadata.json'
        self.assertEqual(json.loads(path.read_text(encoding='utf-8')),
                         {'revision': 10, 'pools': ['build', 'gpu'], 'tags': ['linux']})

        worker.update(pools=['gpu'], tags=[], settings_revision=11)
        self.write_registry([worker])
        listed = (await catalog())[0]
        self.assertEqual(listed['metadata_sync']['state'], 'pending')
        self.assertEqual(json.loads(path.read_text(encoding='utf-8')),
                         {'revision': 11, 'pools': ['gpu'], 'tags': []})

    async def test_offline_save_is_pending_then_pushed_when_the_worker_connects(self):
        self.write_registry([{'id': 'pc-a', 'pools': ['build']}])  # awaiting connection
        status, body = await self.call('PUT', '/fleet/workers/pc-a', {'ram_limit': 70, 'slots': 3})
        self.assertEqual((status, body['settings_sync']['state']), (200, 'pending'))
        app, server, gate, _ = await self.service()
        self.assertEqual(gate.metrics()['settings']['ram_limit'], 90)
        data = yaml.safe_load(self.registry.read_text(encoding='utf-8'))
        data['workers'][0].update(url=str(server.make_url('')), token_env='PC_A_TOKEN')  # it connects
        self.registry.write_text(yaml.safe_dump(data), encoding='utf-8')
        from flowkit.workers import catalog
        listed = (await catalog())[0]
        self.assertTrue(listed['online'])
        self.assertEqual(listed['settings_sync']['state'], 'applied')
        self.assertEqual(gate.metrics()['settings'], {'slots': 3, 'cpu_limit': 90, 'ram_limit': 70})
        # Reconnect after a worker-side change (for example a reset store): pushed again.
        gate.configure(slots=1)
        await catalog()
        self.assertEqual(gate.metrics()['settings'], {'slots': 3, 'cpu_limit': 90, 'ram_limit': 70})


class FleetViewTests(Fixture):
    async def test_fleet_view_is_sanitized_with_inventory_metrics_engines_and_states(self):
        app, server, gate, clock = await self.service()
        self.write_registry([{'id': 'pc-a', 'pools': ['build', 'gpu'], 'tags': ['linux'], 'url': str(server.make_url('')),
                              'token_env': 'PC_A_TOKEN', 'engines': ['hatchet', 'prefect']},
                             {'id': 'pc-b', 'pools': ['build']}], tags=['spare'])
        feed(gate, clock, cpu=42, ram=63)
        status, body = await self.call('GET', '/fleet')
        self.assertEqual(status, 200, body)
        text = json.dumps(body)
        for secret in (TOKEN, 'PC_A_TOKEN', 'token_env', '127.0.0.1', 'localhost', '"url"', 'host-secret'):
            self.assertNotIn(secret, text)
        a, b = (next(w for w in body['workers'] if w['id'] == i) for i in ('pc-a', 'pc-b'))
        self.assertEqual((a['online'], a['status'], a['connection']), (True, 'online', 'configured'))
        self.assertEqual(a['engines'], ['hatchet', 'prefect'])
        self.assertEqual(set(a['engine_status']), {'hatchet', 'prefect'})
        self.assertEqual(a['engine_status']['hatchet']['route'], 'shared_service')
        self.assertEqual(a['capacity']['state'], 'known')
        self.assertAlmostEqual(a['capacity']['cpu_percent'], 42)
        self.assertAlmostEqual(a['capacity']['ram_percent'], 63)
        self.assertEqual(a['capacity']['ram_basis'], 'physical total minus available')
        self.assertLessEqual(a['capacity']['sample_age_seconds'], 3)
        self.assertEqual(a['inventory']['agents']['demo']['status'], 'installed')
        self.assertEqual(a['inventory']['agents']['codex']['status'], 'absent')
        self.assertTrue(a['inventory']['agents']['demo']['checked_at'])
        self.assertFalse(a['inventory']['cached'])
        self.assertEqual((b['online'], b['status'], b['connection']), (False, 'awaiting_connection', 'awaiting'))
        self.assertEqual(b['engines'], ['hatchet', 'prefect', 'trigger'])  # legacy entries serve every engine
        self.assertEqual((b['cpu_limit'], b['ram_limit'], b['slots']), (90, 90, 1))
        self.assertEqual(body['tags'], ['linux', 'spare'])
        self.assertEqual(body['pools'], ['build', 'gpu'])
        self.assertEqual(body['engines'], ['hatchet', 'prefect', 'trigger'])

        status, metrics = await self.call('GET', '/fleet/metrics')
        self.assertEqual(status, 200)
        self.assertNotIn('inventory', json.dumps(metrics))
        self.assertEqual(next(w for w in metrics['workers'] if w['id'] == 'pc-a')['capacity']['state'], 'known')

        clock.now += 5  # no fresh sample: stale, therefore unknown
        stale = next(w for w in (await self.call('GET', '/fleet/metrics'))[1]['workers'] if w['id'] == 'pc-a')['capacity']
        self.assertEqual((stale['state'], stale['reason'], stale['cpu_percent']), ('unknown', 'stale', None))
        self.assertGreater(stale['sample_age_seconds'], 3)

    async def test_register_offline_then_add_a_second_engine_without_duplicate(self):
        self.write_registry([{'id': 'pc-a', 'pools': ['build']}])
        new = {'id': 'lab-pc', 'pools': ['build', 'gpu'], 'tags': ['windows', 'cuda'], 'engines': ['hatchet']}
        status, body = await self.call('POST', '/fleet/workers', new)
        self.assertEqual(status, 200, body)
        self.assertEqual((body['worker']['connection'], body['worker']['cpu_limit'], body['worker']['ram_limit']), ('awaiting', 90, 90))
        self.assertEqual(body['settings_sync']['state'], 'pending')
        status, body = await self.call('POST', '/fleet/workers', {**new, 'engines': ['prefect']})
        self.assertEqual(status, 409, body)
        self.assertIn('already registered', body['error'])
        status, body = await self.call('PUT', '/fleet/workers/lab-pc', {'engines': ['hatchet', 'prefect']})
        self.assertEqual(status, 200, body)
        saved = [w for w in yaml.safe_load(self.registry.read_text(encoding='utf-8'))['workers'] if w['id'] == 'lab-pc']
        self.assertEqual(len(saved), 1)
        self.assertEqual((saved[0]['engines'], saved[0]['pools'], saved[0]['tags']), (['hatchet', 'prefect'], ['build', 'gpu'], ['windows', 'cuda']))
        for bad in ({'engines': []}, {'engines': ['hatchet', 'hatchet']}, {'engines': ['native']}, {'token_env': 'bad name'},
                    {'password': 'x'}, {'url': 'http://user:pw@pc:8096'}):
            self.assertEqual((await self.call('PUT', '/fleet/workers/lab-pc', bad))[0], 400, bad)
        # A network endpoint needs its token on the coordinator; the error never echoes values.
        status, body = await self.call('PUT', '/fleet/workers/lab-pc', {'url': 'https://lab-pc.example:8096', 'token_env': 'UNSET_LAB_TOKEN'})
        self.assertEqual(status, 400)
        self.assertIn('workers.local.env', body['error'])
        self.assertNotIn('lab-pc.example', body['error'])

    async def test_manual_refresh_and_offline_last_known_inventory(self):
        app, server, gate, clock = await self.service()
        self.write_registry([{'id': 'pc-a', 'pools': ['build'], 'url': str(server.make_url('')), 'token_env': 'PC_A_TOKEN'},
                             {'id': 'pc-b', 'pools': ['build']}])
        before = app['inventory']['refreshed_at']
        await asyncio.sleep(0.01)
        status, body = await self.call('POST', '/fleet/workers/pc-a/capabilities/refresh', {})
        self.assertEqual(status, 200, body)
        self.assertGreater(body['inventory']['refreshed_at'], before)
        self.assertEqual(body['inventory']['agents']['demo']['status'], 'installed')
        self.assertNotIn(TOKEN, json.dumps(body))
        status, body = await self.call('POST', '/fleet/workers/pc-b/capabilities/refresh', {})
        self.assertEqual(status, 409)
        self.assertIn('pc-b', body['error'])
        self.assertEqual((await self.call('POST', '/fleet/workers/nobody/capabilities/refresh', {}))[0], 404)
        await server.close()  # pc-a goes offline: the last known results stay, marked as such
        status, body = await self.call('GET', '/fleet')
        a = next(w for w in body['workers'] if w['id'] == 'pc-a')
        self.assertEqual((a['online'], a['status']), (False, 'unreachable'))
        self.assertTrue(a['inventory']['cached'])
        self.assertEqual(a['inventory']['agents']['demo']['status'], 'installed')
        self.assertTrue(a['inventory']['refreshed_at'])

    async def test_tag_catalog_create_and_validate(self):
        self.write_registry([{'id': 'pc-a', 'pools': ['build'], 'tags': ['linux']}])
        self.assertEqual((await self.call('GET', '/fleet/tags'))[1], {'tags': ['linux']})
        status, body = await self.call('POST', '/fleet/tags', {'name': 'gpu'})
        self.assertEqual((status, body), (200, {'tags': ['gpu', 'linux']}))
        self.assertEqual((await self.call('POST', '/fleet/tags', {'name': 'gpu'}))[0], 200)  # idempotent
        for bad in ('', ' gpu', 'a,b', 'x' * 101, 'line\nbreak', 7):
            self.assertEqual((await self.call('POST', '/fleet/tags', {'name': bad}))[0], 400, bad)
        await self.call('PUT', '/fleet/workers/pc-a', {'pools': ['build', 'other']})  # a worker save keeps the catalog
        self.assertEqual((await self.call('GET', '/fleet/tags'))[1], {'tags': ['gpu', 'linux']})
        self.assertEqual((await self.call('GET', '/fleet/tags', auth=False))[0], 401)


class EngineMatchTests(unittest.TestCase):
    def test_a_worker_only_serves_its_registered_engines(self):
        from flowkit.fleet_matching import match_worker, validate_launch, with_engine
        worker = {'id': 'pc-a', 'pools': ['build'], 'tags': [], 'online': True, 'active': 0, 'slots': 1,
                  'capacity': {'state': 'known', 'cpu_percent': 1, 'ram_percent': 1, 'gate_id': 'test-gate'}, 'settings_sync': {'state': 'applied'}, 'engines': ['hatchet']}
        self.assertEqual(with_engine({'pool': 'build'}, 'prefect'), {'pool': 'build', 'engine': 'prefect'})
        self.assertEqual(with_engine({'pool': 'build'}, None), {'pool': 'build'})
        self.assertEqual(match_worker(worker, {'pool': 'build', 'engine': 'hatchet'})['status'], 'compatible')
        refused = validate_launch({'pool': 'build', 'engine': 'prefect'}, [worker])
        self.assertEqual(refused['status'], 'blocked')
        self.assertEqual(refused['reasons'][0]['code'], 'engine')
        self.assertIn('Prefect', refused['resolution'])
        legacy = {k: v for k, v in worker.items() if k != 'engines'}
        self.assertEqual(match_worker(legacy, {'pool': 'build', 'engine': 'prefect'})['status'], 'compatible')


class ReviewRoundOneTests(Fixture):
    async def test_one_slot_maximum_shared_by_registry_gate_route_and_ui(self):
        from flowkit import capacity, fleet_registry
        self.assertIs(fleet_registry.MAX_SLOTS, capacity.MAX_SLOTS)
        self.assertEqual(capacity.MAX_SLOTS, 100)
        ui = (Path(__file__).resolve().parents[2] / 'board/static/fleet.js').read_text(encoding='utf-8')
        self.assertIn('const MAX_SLOTS = 100;', ui)
        with self.assertRaises(ValueError): CapacityGate(self.root / 'g.db', slots=101)
        # A stored entry above the maximum (old YAML) keeps working: clamped, with a visible warning.
        app, server, gate, _ = await self.service()
        self.write_registry([{'id': 'pc-a', 'pools': ['build'], 'slots': 500, 'url': str(server.make_url('')), 'token_env': 'PC_A_TOKEN'},
                             {'id': 'pc-b', 'pools': ['build'], 'slots': 100}])
        listed = {w['id']: w for w in fleet_registry.load_fleet()}
        self.assertEqual(listed['pc-a']['slots'], 100)
        self.assertIn('slots 500 in the registry is above the maximum of 100', listed['pc-a']['warnings'][0])
        self.assertNotIn('warnings', listed['pc-b'])
        status, body = await self.call('GET', '/fleet')
        a = next(w for w in body['workers'] if w['id'] == 'pc-a')
        self.assertEqual((a['slots'], a['settings_sync']['state']), (100, 'applied'))  # matched and enforced as 100
        self.assertIn('above the maximum', a['warnings'][0])
        self.assertEqual(gate.metrics()['settings']['slots'], 100)
        # Saves above the maximum are refused on every path, with the maximum in the message.
        for method, path in (('PUT', '/workers/pc-b'), ('PUT', '/fleet/workers/pc-b'), ('POST', '/workers')):
            status, body = await self.call(method, path, {'id': 'pc-b', 'slots': 101} if method == 'POST' else {'slots': 101})
            self.assertEqual(status, 400, path)
            self.assertIn('at most 100', body['error'])
        with self.assertRaises(ValueError): fleet_registry.save_worker({'id': 'pc-c', 'slots': 10000})
        # The next save of that entry stores the enforced value; the warning goes away.
        self.assertEqual((await self.call('PUT', '/fleet/workers/pc-a', {'tags': ['x']}))[0], 200)
        stored = next(w for w in yaml.safe_load(self.registry.read_text(encoding='utf-8'))['workers'] if w['id'] == 'pc-a')
        self.assertEqual(stored['slots'], 100)
        self.assertFalse(any(k.startswith('_') for k in stored))
        self.assertNotIn('warnings', next(w for w in fleet_registry.load_fleet() if w['id'] == 'pc-a'))

    async def test_failed_or_unsupported_push_is_not_retried_on_every_catalog_call(self):
        from aiohttp import web
        from flowkit import workers
        posts = []
        async def health(request):
            return web.json_response({'id': 'old-pc', 'hostname': 'OLD', 'active': 0,
                                      'capacity': {'state': 'unknown', 'settings': {'slots': 4, 'cpu_limit': 90, 'ram_limit': 90}}})
        async def settings(request):
            posts.append(await request.json())
            raise web.HTTPNotFound()  # an execution service older than the configure route
        old = web.Application(); old.router.add_get('/health', health); old.router.add_post('/capacity/settings', settings)
        server = TestServer(old); await server.start_server(); self.addAsyncCleanup(server.close)
        self.write_registry([{'id': 'old-pc', 'pools': ['build'], 'url': str(server.make_url(''))}])
        for _ in range(3):
            listed = (await workers.catalog())[0]
            self.assertEqual(listed['settings_sync']['state'], 'unsupported')
        self.assertEqual(len(posts), 1)
        self.write_registry([{'id': 'old-pc', 'pools': ['build'], 'cpu_limit': 70, 'url': str(server.make_url(''))}])
        await workers.catalog(); await workers.catalog()
        self.assertEqual(len(posts), 2)  # a changed registry value is tried at once, then held again
        wanted, _, held = workers._PUSH_BACKOFF['old-pc']
        self.assertEqual(held['state'], 'unsupported')
        workers._PUSH_BACKOFF['old-pc'] = (wanted, 0, held)  # the backoff has elapsed
        await workers.catalog()
        self.assertEqual(len(posts), 3)  # retried after the backoff
        # A failing push (worker refuses the limits) is held the same way.
        async def refuse(request):
            posts.append(await request.json())
            return web.json_response({'error': 'slots must be a whole number from 1 to 100'}, status=400)
        refusing = web.Application(); refusing.router.add_get('/health', health); refusing.router.add_post('/capacity/settings', refuse)
        server2 = TestServer(refusing); await server2.start_server(); self.addAsyncCleanup(server2.close)
        workers._PUSH_BACKOFF.clear(); posts.clear()
        self.write_registry([{'id': 'old-pc', 'pools': ['build'], 'url': str(server2.make_url(''))}])
        for _ in range(3):
            self.assertEqual((await workers.catalog())[0]['settings_sync']['state'], 'failed')
        self.assertEqual(len(posts), 1)

    def test_trigger_dev_unconfirmed_is_not_connected(self):
        from flowkit.workers import engine_status
        online, offline = {'online': True}, {'online': False}
        states = {'hatchet': 'running', 'prefect': 'stopped', 'trigger': 'unconfirmed'}
        self.assertEqual(engine_status(online, 'trigger', states), {'route': 'shared_service', 'coordinator': 'unconfirmed', 'state': 'unconfirmed', 'connected': False})
        self.assertEqual(engine_status(online, 'hatchet', states)['state'], 'connected')
        self.assertEqual(engine_status(online, 'prefect', states)['state'], 'not_connected')
        self.assertEqual(engine_status(offline, 'trigger', states)['state'], 'not_connected')


if __name__ == '__main__':
    unittest.main()
