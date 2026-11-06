"""The board's side of New card: program cards, attached sources, pools, uploads and schedules."""
import base64
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp.test_utils import TestClient, TestServer
import server

READY = {'status': 'ready', 'fleet': True, 'card': {'status': 'ready', 'candidates': ['pc']}}
FLOW = {'name': 'demo', 'input': {'topic': 'x'}, 'steps': {'a': {'kind': 'program'}}}


class Host:
    """The flow host's answers; records what the board sends."""
    def __init__(self):
        self.calls, self.claimable, self.items = [], [], {}
        self.decision = READY

    async def flows(self): return {'demo': FLOW}
    async def validate_card(self, agent, worker, engine=None, command=None):
        self.calls.append(('validate_card', agent, worker, engine, command))
        return self.decision
    async def validate_routing(self, execution, steps): return execution
    async def validate_launch(self, flow, execution=None, engine=None): return {'status': 'ready', 'fleet': True, 'steps': {}}
    async def normalize_context(self, context):
        self.calls.append(('normalize', context))
        return dict(context, normalized=True)
    async def save_upload(self, name, data):
        self.calls.append(('upload', name, base64.b64decode(data)))
        return {'id': 'a' * 32, 'name': name, 'size': len(base64.b64decode(data))}
    async def delete_upload(self, upload): return {'ok': True}
    async def schedules(self, system=None, board=None): return [{'id': 's', 'system': system, 'board': board}]
    async def create_schedule(self, body):
        self.calls.append(('create_schedule', body))
        item = {'id': 's1', **body}
        self.items['s1'] = item
        return item
    async def schedule(self, schedule_id): return self.items[schedule_id]
    async def preview_schedule(self, body): return {'next': ['t']}
    async def claim_schedules(self, systems):
        self.calls.append(('claim', systems))
        return self.claimable
    async def delete_schedule(self, schedule_id):
        self.calls.append(('delete_schedule', schedule_id))
        return {'ok': True}
    async def set_schedule_enabled(self, schedule_id, enabled):
        self.calls.append(('enabled', schedule_id, enabled))
        return {'id': schedule_id, 'enabled': enabled}
    async def record_schedule(self, schedule_id, **outcome):
        self.calls.append(('record', schedule_id, outcome))
        return {'ok': True}
    async def close(self): pass


class Base(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        owner = self
        self.created, self.cards = [], []
        class Adapter:
            async def create(self, *args, **kwargs):
                owner.created.append((args, kwargs))
                return f'run-{len(owner.created)}'
            async def create_flow(self, *args):
                owner.created.append(args)
                return f'run-{len(owner.created)}'
            async def list_cards(self, board): return owner.cards
        with patch.object(server, 'load_adapters', return_value={'hatchet': Adapter(), 'trigger-dev': Adapter()}):
            self.client = TestClient(TestServer(server.make_app()))
        self.host = Host()
        self.client.app['flow_host'] = self.host
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def post(self, path, body, status=201):
        response = await self.client.post(path, json=body)
        data = await response.json()
        self.assertEqual(response.status, status, data)
        return data

    def cards_path(self, system='hatchet'):
        return f'/api/boards/{system}/demo/cards'


class Cards(Base):
    async def test_a_program_runs_as_a_session_placed_on_the_fleet(self):
        await self.post(self.cards_path(), {'kind': 'program', 'command': 'make test', 'worker': {'pools': ['a', 'b'], 'tags': ['gpu']},
                                            'context': {'text': 'hello', 'repositories': [{'url': 'https://x.io/a.git'}]}})
        args, kwargs = self.created[0]
        self.assertEqual(args[1:3], ('agent-session', 'make test'))
        options = kwargs['session_options']
        self.assertEqual(options['agent'], 'program')
        self.assertEqual(options['worker'], {'pools': ['a', 'b'], 'tags': ['gpu']})
        self.assertEqual(options['programInput'], 'hello')
        self.assertNotIn('text', options['context'])
        self.assertIn(('validate_card', 'program', {'pools': ['a', 'b'], 'tags': ['gpu']}, 'hatchet', 'make test'), self.host.calls)

    async def test_a_program_needs_a_command(self):
        await self.post(self.cards_path(), {'kind': 'program', 'command': ' '}, status=400)
        await self.post(self.cards_path(), {'kind': 'program', 'command': 'x', 'resumeSessionId': 'y'}, status=400)
        self.assertEqual(self.created, [])

    async def test_an_agent_card_cannot_pretend_to_be_a_program(self):
        await self.post(self.cards_path(), {'kind': 'agent-session', 'goal': 'x', 'agent': 'program'}, status=400)
        self.assertEqual(self.created, [])

    async def test_agent_cards_get_what_was_attached(self):
        await self.post(self.cards_path(), {'kind': 'agent-session', 'goal': 'Fix it', 'agent': 'demo', 'context': {'files': [{'id': 'f', 'name': 'n'}]}})
        self.assertEqual(self.created[0][1]['session_options']['context']['files'], [{'id': 'f', 'name': 'n'}])

    async def test_flows_get_what_was_attached_as_a_parameter(self):
        await self.post(self.cards_path(), {'kind': 'flow', 'flow': 'demo', 'input': {'topic': 'y'}, 'context': {'confluence': [{'url': 'u'}]}})
        board, name, params, title = self.created[0]
        self.assertEqual(params['flowContext'], {'confluence': [{'url': 'u'}], 'normalized': True})
        self.assertEqual(title, 'demo: y')

    async def test_pool_lists_are_validated(self):
        await self.post(self.cards_path(), {'kind': 'agent-session', 'goal': 'x', 'agent': 'demo', 'worker': {'pools': 'a'}}, status=400)
        await self.post(self.cards_path(), {'kind': 'agent-session', 'goal': 'x', 'agent': 'demo', 'worker': {'nope': 1}}, status=400)

    async def test_trigger_cards_never_go_to_the_fleet(self):
        await self.post(self.cards_path('trigger-dev'), {'kind': 'program', 'command': 'x', 'worker': {'pools': ['a']}}, status=400)
        await self.post(self.cards_path('trigger-dev'), {'kind': 'program', 'command': 'x'})
        self.assertEqual(self.created[0][1]['session_options']['agent'], 'program')
        self.assertNotIn('worker', self.created[0][1]['session_options'])

    async def test_launch_check_passes_the_program_on(self):
        await self.post('/api/launch/validate', {'card': {'agent': 'program', 'worker': {'tags': []}, 'command': 'gcc x'}, 'system': 'hatchet'}, status=200)
        self.assertIn(('validate_card', 'program', {'tags': []}, 'hatchet', 'gcc x'), self.host.calls)


class Uploads(Base):
    async def test_a_large_file_gets_past_the_json_size_limit(self):
        content = b'x' * (3 * 1024 * 1024)
        response = await self.client.post('/api/uploads?name=big.bin', data=content, headers={'Content-Type': 'application/octet-stream'})
        self.assertEqual(response.status, 201, await response.text())
        self.assertEqual((await response.json())['size'], len(content))
        self.assertEqual(self.host.calls[-1], ('upload', 'big.bin', content))

    async def test_over_the_limit_is_refused(self):
        with patch.object(server, 'MAX_UPLOAD_BYTES', 10):
            response = await self.client.post('/api/uploads?name=big.bin', data=b'x' * 11)
        self.assertEqual(response.status, 413)


class Schedules(Base):
    WHEN = {'cron': '0 9 * * *', 'timezone': 'UTC', 'summary': 'Every day at 09:00'}

    def created_schedule(self):
        return next(c[1] for c in self.host.calls if c[0] == 'create_schedule')

    async def test_later_stores_it_and_starts_nothing(self):
        result = await self.post(self.cards_path(), {'kind': 'program', 'command': 'x', 'schedule': self.WHEN})
        self.assertEqual(result['schedule']['id'], 's1')
        self.assertEqual(self.created, [])
        body = self.created_schedule()
        self.assertEqual((body['system'], body['board'], body['title']), ('hatchet', 'demo', 'x'))
        self.assertNotIn('schedule', body['request'])
        self.assertEqual(body['schedule'], self.WHEN)

    async def test_start_now_and_repeat_records_the_first_run(self):
        result = await self.post(self.cards_path(), {'kind': 'program', 'command': 'x', 'schedule': {**self.WHEN, 'runNow': True}})
        self.assertEqual(result['id'], 'run-1')
        self.assertNotIn('runNow', self.created_schedule()['schedule'])
        self.assertIn(('record', 's1', {'run': 'run-1'}), self.host.calls)

    async def test_a_card_that_cannot_start_is_not_scheduled(self):
        self.host.decision = {'status': 'blocked', 'fleet': True, 'card': {'status': 'blocked', 'message': 'no', 'resolution': 'fix'}}
        await self.post(self.cards_path(), {'kind': 'program', 'command': 'x', 'schedule': self.WHEN}, status=409)
        self.assertFalse(any(c[0] == 'create_schedule' for c in self.host.calls))

    def item(self, **extra):
        return {'id': 's1', 'system': 'hatchet', 'board': 'demo', 'cron': '0 9 * * *', 'last_run': None,
                'request': {'kind': 'program', 'command': 'x'}, **extra}

    async def test_a_due_schedule_starts_its_card(self):
        self.host.claimable = [self.item()]
        await server.run_due_schedules(self.client.app)
        self.assertEqual(len(self.created), 1)
        self.assertIn(('record', 's1', {'run': 'run-1'}), self.host.calls)
        self.assertIn(('claim', ['hatchet', 'trigger-dev']), self.host.calls)

    async def test_a_repeat_waits_for_its_previous_run(self):
        self.cards = [SimpleNamespace(id='old', column='running')]
        self.host.claimable = [self.item(last_run='old')]
        await server.run_due_schedules(self.client.app)
        self.assertEqual(self.created, [])
        self.assertIn(('record', 's1', {'skipped': 'The previous run is still active'}), self.host.calls)
        self.cards = [SimpleNamespace(id='old', column='done')]
        await server.run_due_schedules(self.client.app)
        self.assertEqual(len(self.created), 1)

    async def test_a_repeat_can_be_set_to_overlap_its_previous_run(self):
        self.cards = [SimpleNamespace(id='old', column='running')]
        self.host.claimable = [self.item(last_run='old', overlap='start')]
        await server.run_due_schedules(self.client.app)
        self.assertEqual(len(self.created), 1)
        self.assertIn(('record', 's1', {'run': 'run-1'}), self.host.calls)

    async def test_a_start_that_fails_is_recorded(self):
        self.host.decision = {'status': 'blocked', 'fleet': True, 'card': {'status': 'blocked', 'message': 'no worker', 'resolution': 'fix'}}
        self.host.claimable = [self.item()]
        await server.run_due_schedules(self.client.app)
        record = next(c for c in self.host.calls if c[0] == 'record')
        self.assertIn('no worker', record[2]['error'])

    async def test_run_now_ignores_the_overlap_rule(self):
        self.cards = [SimpleNamespace(id='old', column='running')]
        self.host.items['s1'] = self.item(last_run='old')
        data = await self.post('/api/schedules/s1/run', {}, status=200)
        self.assertEqual(data, {'run': 'run-1'})

    async def test_pause_delete_and_list(self):
        self.assertEqual(await self.post('/api/schedules/s1/disable', {}, status=200), {'id': 's1', 'enabled': False})
        self.assertEqual((await (await self.client.delete('/api/schedules/s1')).json()), {'ok': True})
        listed = await (await self.client.get('/api/schedules?system=hatchet&board=demo')).json()
        self.assertEqual(listed['schedules'][0]['board'], 'demo')


if __name__ == '__main__':
    unittest.main()
