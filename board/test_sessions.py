"""Session API contracts; no running orchestration engines required."""
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp.test_utils import TestClient, TestServer
from cards import Card
import server
from trigger_adapter import TriggerAdapter


class APIContracts(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.calls = []
        owner = self
        class Adapter:
            async def create(self, *args, **kwargs):
                owner.calls.append((args, kwargs))
                return 'run-1'
            async def input(self, run_id, message):
                owner.calls.append((run_id, message))
        with patch.object(server, 'load_adapters', return_value={'hatchet': Adapter()}):
            self.client = TestClient(TestServer(server.make_app()))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_create_session_preserves_options(self):
        response = await self.client.post('/api/boards/hatchet/demo/cards', json={
            'kind': 'agent-session', 'goal': 'Fix test', 'agent': 'demo',
            'workspace': 'C:/work', 'simulateFailures': 2})
        self.assertEqual(response.status, 201, await response.text())
        self.assertEqual(self.calls[0][1]['session_options'], {
            'agent': 'demo', 'workspace': 'C:/work', 'simulateFailures': 2})

    async def test_rejects_malformed_or_unbounded_requests_with_json(self):
        cases = [[], {'goal': []}, {'kind': 'agent-session', 'goal': 'x', 'agent': 'unknown'},
                 {'kind': 'agent-session', 'goal': 'x', 'simulateFailures': -1},
                 {'kind': 'agent-session', 'goal': 'x', 'simulateFailures': True},
                 {'goal': 'x', 'turns': 'no'}, {'goal': 'x' * 20001}]
        for body in cases:
            with self.subTest(body=str(body)[:80]):
                response = await self.client.post('/api/boards/hatchet/demo/cards', json=body)
                self.assertEqual(response.status, 400)
                self.assertIn('error', await response.json())
        self.assertEqual(self.calls, [])

    async def test_input_preserves_typed_answers(self):
        body = {'type': 'answer', 'id': 'q1', 'answers': {'count': 4, 'enabled': True, 'names': ['a', 'b']}}
        response = await self.client.post('/api/cards/hatchet/run-1/input', json=body)
        self.assertEqual(response.status, 200, await response.text())
        self.assertEqual(self.calls, [('run-1', body)])

    async def test_input_rejects_bad_shapes(self):
        for body in [[], {}, {'type': 'message', 'text': []}, {'type': 'message', 'text': ''},
                     {'type': 'approval', 'id': 'a'}, {'type': 'answer', 'id': 'q', 'answers': []},
                     {'type': 'answer', 'id': 'q', 'answers': {'x': {'nested': 'bad'}}},
                     {'type': 'interrupt', 'text': 'unexpected'}, {'type': 'stop'}]:
            response = await self.client.post('/api/cards/hatchet/run-1/input', json=body)
            self.assertEqual(response.status, 400, str(body))
            self.assertIn('error', await response.json())
        self.assertEqual(self.calls, [])


class ProjectionTests(unittest.TestCase):
    def test_waiting_state_and_terminal_engine_precedence(self):
        for column, expected in [('running', 'waiting'), ('done', 'done'), ('stopped', 'stopped')]:
            card = Card('id', 'hatchet', 'demo', 'goal', 'agent-session', column, 'RAW')
            card.apply_session_events([{'type': 'status', 'state': 'awaiting_review'}])
            self.assertEqual(card.column, expected)
            self.assertEqual(card.session_state, 'done' if column == 'done' else 'error' if column == 'stopped' else 'awaiting_review')

    def test_status_projection_ignores_later_non_status_events(self):
        card = Card('id', 'trigger-dev', 'demo', 'goal', 'agent-session', 'waiting', 'WAITING')
        card.apply_session_events([{'type': 'status', 'state': 'working'}, {'type': 'agent', 'text': 'hello'}])
        self.assertEqual((card.column, card.session_state), ('running', 'working'))
        self.assertEqual(card.progress, ['hello'])


class TriggerContracts(unittest.IsolatedAsyncioTestCase):
    async def test_dev_cards_use_events_and_terminal_status_wins(self):
        with patch.dict('os.environ', {'TRIGGER_DEV_SECRET_KEY': 'dev-key'}):
            adapter = TriggerAdapter(development=True)
        async def request(method, path, **kwargs):
            if path.startswith('/api/v1/queues'):
                return {'data': []}
            if path.startswith('/api/v3/runs'):
                return {'error': {'message': 'worker crashed'}}
            if kwargs.get('params', {}).get('filter[status]'):
                return {'data': []}
            return {'data': [{'id': 'run-1', 'status': 'CRASHED', 'taskIdentifier': 'agent-session',
                             'createdAt': '2026-01-01', 'metadata': {'agent': 'demo', 'events': [
                                 {'type': 'status', 'state': 'approval'}, {'type': 'agent', 'text': 'hello'}]}}]}
        adapter._request = request
        card, = await adapter.list_cards('demo')
        self.assertEqual((card.column, card.session_state, card.error), ('stopped', 'error', 'worker crashed'))
        self.assertEqual(card.agent, 'demo')
        self.assertEqual(len(card.events), 2)

    async def test_production_rejects_native_session(self):
        with patch.dict('os.environ', {'TRIGGER_SECRET_KEY': 'test'}):
            adapter = TriggerAdapter()
        async def request(*args, **kwargs):
            return {'id': 'unexpected'}
        adapter._request = request
        with self.assertRaisesRegex(ValueError, 'development'):
            await adapter.create('board', 'agent-session', 'goal', 6, 5)

    async def test_dev_key_stream_and_queue_url(self):
        with patch.dict('os.environ', {'TRIGGER_DEV_SECRET_KEY': 'dev-key'}):
            adapter = TriggerAdapter(development=True)
        calls = []
        async def request(method, path, **kwargs):
            calls.append((method, path, kwargs))
            return {'id': 'run-1'}
        adapter._request = request
        await adapter.create('board', 'agent-session', 'goal', 6, 5, session_options={'agent': 'demo'})
        await adapter.input('run-1', {'type': 'done'})
        await adapter.set_queue_paused('task/agent session', True)
        self.assertEqual(adapter.system, 'trigger-dev')
        self.assertEqual(adapter.headers['Authorization'], 'Bearer dev-key')
        self.assertEqual(calls[0][1], '/api/v1/tasks/agent-session/trigger')
        self.assertEqual(calls[0][2]['json']['payload'], {'goal': 'goal', 'agent': 'demo'})
        self.assertEqual(calls[1][1], '/realtime/v1/streams/run-1/input/agent-input')
        self.assertEqual(calls[2][1], '/api/v1/queues/task%2Fagent%20session/pause')


class HatchetContracts(unittest.IsolatedAsyncioTestCase):
    async def test_reads_more_than_one_thousand_logs_in_either_server_order(self):
        from hatchet_adapter import HatchetAdapter
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        rows = [SimpleNamespace(created_at=start + timedelta(milliseconds=i), message=f'EVT {{"seq":{i}}}') for i in range(1502)]
        for descending in (False, True):
            class Logs:
                def list(self, task_run_id, limit=1000, since=None, until=None):
                    available = [r for r in rows if (since is None or r.created_at >= since) and (until is None or r.created_at <= until)]
                    return SimpleNamespace(rows=sorted(available, key=lambda r:r.created_at, reverse=descending)[:limit])
            adapter = HatchetAdapter.__new__(HatchetAdapter)
            adapter.hatchet = SimpleNamespace(logs=Logs())
            with self.subTest(descending=descending):
                progress = await adapter._progress('task-1')
                self.assertEqual(len(progress), 1502)
                self.assertEqual(progress[0], 'EVT {"seq":0}')
                self.assertEqual(progress[-1], 'EVT {"seq":1501}')

    async def test_session_logs_become_events_without_leaking_evt_json_to_progress(self):
        from hatchet_adapter import HatchetAdapter, V1TaskStatus, V1WorkflowType
        adapter = HatchetAdapter.__new__(HatchetAdapter)
        adapter._final_progress = {}
        async def progress(ref):
            return ['worker started', 'EVT invalid', 'EVT {"type":"agent","text":"hello"}',
                    'EVT {"type":"status","state":"approval"}']
        adapter._progress = progress
        summary = SimpleNamespace(workflow_run_external_id='run-1', status=V1TaskStatus.RUNNING,
                                  is_evicted=False, workflow_name='agent-session', display_name='agent-session',
                                  input={'input': {'goal': 'fix', 'agent': 'demo'}}, created_at=datetime.now(timezone.utc),
                                  started_at=None, finished_at=None, output=None, error_message=None,
                                  type=V1WorkflowType.TASK, task_external_id='task-1')
        card = await adapter._card(summary, 'demo', {})
        self.assertEqual((card.column, card.session_state, card.agent), ('waiting', 'approval', 'demo'))
        self.assertEqual(card.progress, ['hello'])
        self.assertEqual(len(card.events), 2)


if __name__ == '__main__':
    unittest.main()
