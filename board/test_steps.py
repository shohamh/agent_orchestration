"""Step-view contracts without running orchestration engines."""
import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch, AsyncMock

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp.test_utils import TestClient, TestServer
from cards import Card, Dag, DagNode
import server
from live import Hub


FLOW = {'name': 'demo-flow', 'input': {'topic': 'hello'}, 'steps': {
    'first': {'kind': 'program', 'needs': []},
    'talk': {'kind': 'agent', 'needs': ['first'], 'agent': 'demo'},
    'last': {'kind': 'program', 'needs': ['talk']}}}


class Adapter:
    def __init__(self):
        self.column = 'done'
        self.calls = []
        self.rows = [{'type': 'agent', 'message_id': str(i), 'text': str(i)} for i in range(250)]

    async def list_cards(self, board, light=False):
        return [Card('run', 'fake', board, 'Flow', 'demo-flow', self.column, 'RUNNING')]

    async def run_info(self, run_id):
        return {'task': 'demo-flow', 'column': self.column, 'params': {'topic': 'original'},
                'board': 'demo', 'title': 'Original flow'}

    async def dag(self, run_id):
        return Dag('declared', [DagNode(s, s, 'done', 'COMPLETED', ref=s) for s in FLOW['steps']])

    async def run_lines(self, run_id):
        return []

    async def node_lines(self, ref):
        events = self.rows if ref == 'talk' else []
        return ['EVT ' + json.dumps(e) for e in [*events,
            {'type': 'step', 'state': 'done', 'output': None if ref == 'first' else 'result'}]]

    async def node_input(self, run_id, ref, body):
        self.calls.append((run_id, ref, body))

    async def create_flow(self, *args):
        self.calls.append(args)
        return 'new-run'

    async def restart_node(self, run_id, ref):
        self.calls.append(('native-replay', run_id, ref))

    async def cancel(self, run_id):
        self.calls.append(('cancel', run_id))


class StepAPI(unittest.IsolatedAsyncioTestCase):
    async def test_launch_forwards_validated_routing_to_runtime(self):
        self.app['flow_host'].validate_routing = AsyncMock(return_value={'default': {'pool': 'gpu'}, 'steps': {'talk': {'id': 'pc-b'}}})
        self.app['flow_host'].validate_launch = AsyncMock(return_value={'status': 'wait', 'fleet': True, 'steps': {}})
        response = await self.client.post('/api/boards/fake/demo/cards', json={
            'kind': 'flow', 'flow': 'demo-flow', 'execution': {'default': {'pool': 'gpu'}, 'steps': {'talk': {'id': 'pc-b'}}}})
        self.assertEqual(response.status, 201, await response.text())
        self.assertEqual(self.adapter.calls[0][2]['flowExecution']['steps']['talk'], {'id': 'pc-b'})
        self.assertEqual((await response.json())['launch']['status'], 'wait')

    async def test_launch_blocked_by_confirmed_mismatch_is_explained(self):
        blocked = {'status': 'blocked', 'message': 'No registered worker can run this step: pc-a lacks tags: gpu',
                   'resolution': 'Add the missing tags to a worker in the selected pool.'}
        self.app['flow_host'].validate_launch = AsyncMock(return_value={'status': 'blocked', 'fleet': True, 'steps': {'talk': blocked}})
        response = await self.client.post('/api/boards/fake/demo/cards', json={'kind': 'flow', 'flow': 'demo-flow'})
        self.assertEqual(response.status, 409)
        data = await response.json()
        self.assertIn('lacks tags: gpu', data['error'])
        self.assertIn('Add the missing tags', data['error'])
        self.assertEqual(self.adapter.calls, [])
        response = await self.client.post('/api/launch/validate', json={'flow': 'demo-flow'})
        self.assertEqual((await response.json())['steps']['talk']['status'], 'blocked')

    async def test_unsaved_draft_is_checked_for_the_flow_builder(self):
        draft = {'name': 'draft-flow', 'steps': {'go': {'kind': 'program', 'run': 'echo $X | tee out', 'worker': {'pool': 'build'}}}}
        self.app['flow_host'].validate_routing = AsyncMock(return_value={'default': {}, 'steps': {}})
        self.app['flow_host'].validate_launch = AsyncMock(return_value={'status': 'blocked', 'fleet': True, 'steps': {'go': {'status': 'blocked'}}})
        response = await self.client.post('/api/launch/validate', json={'document': draft, 'execution': {}})
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual((await response.json())['steps']['go']['status'], 'blocked')
        self.app['flow_host'].validate_routing.assert_awaited_once_with({}, ['go'])
        self.app['flow_host'].validate_launch.assert_awaited_once_with(draft, {'default': {}, 'steps': {}}, engine=None)
        for body in ({'document': 'name: x'}, {'document': {'name': 'x', 'steps': []}}, {'document': {'name': 'x', 'steps': {}, 'big': 'x' * 300000}},
                     {'flow': 'demo-flow', 'document': draft}):
            with self.subTest(body=str(body)[:60]):
                self.assertEqual((await self.client.post('/api/launch/validate', json=body)).status, 400)

    async def test_user_cancel_is_marked_on_the_flow_host_first(self):
        order = []
        async def mark(run_id): order.append(('mark', run_id))
        self.app['flow_host'].mark_cancelled = mark
        original = self.adapter.cancel
        async def cancel(run_id):
            order.append(('cancel', run_id)); await original(run_id)
        self.adapter.cancel = cancel
        response = await self.client.post('/api/cards/fake/run/cancel')
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual(order, [('mark', 'run'), ('cancel', 'run')])
        async def down(run_id): raise RuntimeError('flow host not reachable')
        self.app['flow_host'].mark_cancelled = down
        response = await self.client.post('/api/cards/fake/run/cancel')
        self.assertEqual(response.status, 200, await response.text())  # cancel still happens
        self.assertEqual(order[-1], ('cancel', 'run'))

    async def test_restart_override_forwarded_without_descendant_logic(self):
        self.app['flow_host'].set_selection = AsyncMock()
        response = await self.client.post('/api/cards/fake/run/nodes/talk/restart', json={'worker': {'id': 'pc-b'}})
        self.assertEqual(response.status, 200, await response.text())
        self.app['flow_host'].set_selection.assert_awaited_once_with('run', 'talk', {'id': 'pc-b'}, FLOW)
        self.assertEqual(self.adapter.calls, [('native-replay', 'run', 'talk')])

    async def test_flow_cards_carry_their_execution_workers_read_once_when_finished(self):
        for _ in range(2):
            cards = await self.adapter.list_cards('demo')
            await server.label_flows(self.app, cards)
        self.assertEqual(cards[0].workers, [{'step': 'talk', 'id': 'pc-b', 'hostname': 'pc-b.lan', 'pool': 'gpu', 'tags': ['cuda']}])
        self.app['flow_host'].execution.assert_awaited_once_with('run')

    async def test_logical_step_identity_survives_pending_to_started(self):
        pending = server.fit_flow_dag(Dag('declared'), 'demo-flow', FLOW)
        started = server.fit_flow_dag(await Adapter().dag('run'), 'demo-flow', FLOW)
        self.assertEqual([n.id for n in pending.nodes], [n.id for n in started.nodes])
        self.assertEqual(started.nodes[1].ref, 'talk')
    async def asyncSetUp(self):
        self.adapter = Adapter()
        with patch.object(server, 'load_adapters', return_value={'fake': self.adapter}):
            self.app = server.make_app()
        async def flows(): return {'demo-flow': FLOW}
        self.app['flow_host'].flows = flows
        self.app['flow_host'].execution = AsyncMock(return_value={
            'talk': {'state': 'done', 'worker': {'id': 'pc-b', 'hostname': 'pc-b.lan', 'pool': 'gpu', 'tags': ['cuda']}}})
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_node_transcript_pages_by_coalesced_rows(self):
        url = '/api/cards/fake/run/nodes/talk/transcript'
        response = await self.client.get(url)
        self.assertEqual(response.status, 200)
        data = await response.json()
        self.assertEqual((len(data['rows']), data['rows'][0]['text'], data['more']), (100, '150', True))
        data = await (await self.client.get(url + '?before=m:150')).json()
        self.assertEqual((len(data['rows']), data['rows'][0]['text']), (100, '50'))

    async def test_input_rejects_unrelated_and_program_nodes(self):
        for ref in ['other-run', 'first']:
            response = await self.client.post(f'/api/cards/fake/run/nodes/{ref}/input', json={'type': 'done'})
            self.assertIn(response.status, (400, 404))
        response = await self.client.post('/api/cards/fake/run/nodes/talk/input', json={'type': 'message', 'text': 'hello'})
        self.assertEqual(response.status, 200)
        self.assertEqual(self.adapter.calls, [('run', 'talk', {'type': 'message', 'text': 'hello'})])

    async def test_restart_delegates_selected_node_without_calculating_descendants(self):
        response = await self.client.post('/api/cards/fake/run/nodes/talk/restart', json={})
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual(await response.json(), {'id': 'run', 'replayed': True})
        self.assertEqual(self.adapter.calls, [('native-replay', 'run', 'talk')])

    async def test_unsupported_engine_never_emulates_dependency_restart(self):
        from prefect_adapter import PrefectAdapter
        from trigger_adapter import TriggerAdapter
        self.assertFalse(hasattr(PrefectAdapter, 'restart_node'))
        self.assertFalse(hasattr(TriggerAdapter, 'restart_node'))
        original = Adapter.restart_node
        del Adapter.restart_node
        try:
            response = await self.client.post('/api/cards/fake/run/nodes/talk/restart', json={})
            self.assertEqual(response.status, 501)
            self.assertEqual(self.adapter.calls, [])
        finally:
            Adapter.restart_node = original

    async def test_restart_rejects_active_run_without_starting_work(self):
        self.adapter.column = 'running'
        response = await self.client.post('/api/cards/fake/run/nodes/talk/restart', json={})
        self.assertEqual(response.status, 409)
        self.assertEqual(self.adapter.calls, [])

    async def test_prefect_restart_forwards_ids_to_runtime_without_reading_results(self):
        from types import SimpleNamespace
        self.app['adapters']['prefect'] = SimpleNamespace(run_info=self.adapter.run_info, dag=self.adapter.dag)
        self.app['flow_host'].restart_node = AsyncMock(return_value={'id': 'new-run', 'mode': 'runtime'})
        response = await self.client.post('/api/cards/prefect/run/nodes/talk/restart', json={})
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())['id'], 'new-run')
        self.app['flow_host'].restart_node.assert_awaited_once_with('prefect', 'run', 'talk')
        self.assertEqual(self.adapter.calls, [])


    async def test_step_stream_uses_shared_board_watcher_and_sends_latest_rows(self):
        hub = self.app['hub']
        watcher, a = hub.subscribe('fake', 'demo', 'run', node='talk', kind='agent')
        other, b = hub.subscribe('fake', 'demo', 'run', node='talk', kind='agent')
        self.assertIs(watcher, other)
        messages = []
        while not any(m.get('type') == 'node-rows' for m in messages):
            messages.append(await asyncio.wait_for(a.queue.get(), 3))
        data = next(m for m in messages if m['type'] == 'node-rows')
        self.assertEqual((data['node'], len(data['rows']), data['more']), ('talk', 100, True))
        watcher.unsubscribe(a); watcher.unsubscribe(b)

    async def test_step_chat_does_not_wait_for_a_slow_read_of_its_run(self):
        release = asyncio.Event()
        async def stuck_run_lines(run_id):
            await release.wait()
            return []
        self.adapter.run_lines = stuck_run_lines
        watcher, sub = self.app['hub'].subscribe('fake', 'demo', 'run', node='talk', kind='agent')
        try:
            while (await asyncio.wait_for(sub.queue.get(), 3)).get('type') != 'node-rows':
                pass
        finally:
            release.set()
            watcher.unsubscribe(sub)


class FinishedStepLogs(unittest.IsolatedAsyncioTestCase):
    """A finished step's log is read once: the board refreshes an open step every second, and an
    engine under load can take seconds (or minutes) to answer each read."""

    async def test_hatchet_step_log_is_frozen_once_the_step_settles(self):
        from datetime import datetime, timedelta, timezone
        from types import SimpleNamespace
        from hatchet_sdk.clients.rest.models.v1_task_status import V1TaskStatus
        import hatchet_adapter
        with patch.object(hatchet_adapter, 'Hatchet'):
            adapter = hatchet_adapter.HatchetAdapter()
        task = SimpleNamespace(step_id='s1', display_name='talk', task_external_id='talk-run', status=V1TaskStatus.COMPLETED,
                               started_at=None, finished_at=datetime.now(timezone.utc) - timedelta(minutes=5),
                               output=None, error_message=None)
        adapter.hatchet.runs.get.return_value = SimpleNamespace(tasks=[task], shape=None)
        reads = []
        async def log_rows(ref, since=None):
            reads.append(since)
            return [SimpleNamespace(created_at=datetime(2026, 1, 1, tzinfo=timezone.utc), message='EVT {}')]
        adapter._log_rows = log_rows

        await adapter.dag('run')
        for _ in range(3):
            self.assertEqual(await adapter.node_lines('talk-run'), ['EVT {}'])
        self.assertEqual(len(reads), 1)

        task.status, task.finished_at = V1TaskStatus.RUNNING, None  # replayed
        await adapter.dag('run')
        await adapter.node_lines('talk-run')
        self.assertEqual(len(reads), 2)


class PrefectFeeds(unittest.IsolatedAsyncioTestCase):
    async def test_failed_log_socket_cancels_event_socket_before_retry(self):
        from prefect_adapter import PrefectAdapter
        adapter = PrefectAdapter()
        opened, closed = asyncio.Event(), asyncio.Event()
        async def listen(path, *args):
            if path == '/events/out':
                opened.set()
                try: await asyncio.Event().wait()
                finally: closed.set()
            else:
                await opened.wait()
                raise RuntimeError('socket failed')
        adapter._listen = listen
        with self.assertRaises(Exception):
            await adapter.feed('board', lambda: None)
        self.assertTrue(closed.is_set())


if __name__ == '__main__':
    unittest.main()
