import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit.definition import parse
from flowkit.restart import build_plan, save_result, workspace_path, definition_hash


def definition():
    return parse({'name': 'restart-test', 'steps': {
        'a': {'kind': 'program', 'run': 'echo a'},
        'b': {'kind': 'program', 'run': 'echo b', 'needs': ['a']},
        'c': {'kind': 'program', 'run': 'echo c', 'needs': ['b']},
        'independent': {'kind': 'program', 'run': 'echo other'}}})


class RestartRuntime(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.env = patch.dict(os.environ, {'FLOW_WORKSPACES': self.temp.name})
        self.env.start()
        self.flow = definition()
        self.nodes = {s: {'ref': s, 'column': 'done'} for s in self.flow.steps}
        for s in self.flow.steps:
            save_result(self.flow, 'source', s, None if s == 'a' else 'x' * 5000)

    def tearDown(self):
        self.env.stop(); self.temp.cleanup()

    def test_plan_uses_selected_subtree_and_preserves_exact_null_and_large_results(self):
        plan = build_plan(self.flow, 'b', self.nodes, 'source')
        self.assertEqual(plan, {'step': 'b', 'source_run_id': 'source', 'definition_hash': definition_hash(self.flow)})
        from flowkit.restart import prepare_workspace, read_result
        target = prepare_workspace(self.flow, 'new', plan)
        self.assertIsNone(read_result(target, 'a'))
        self.assertEqual(read_result(target, 'independent'), 'x' * 5000)

    def test_missing_output_and_nonterminal_sibling_are_rejected(self):
        self.nodes['independent']['column'] = 'running'
        with self.assertRaisesRegex(ValueError, 'still active'):
            build_plan(self.flow, 'b', self.nodes, 'source')
        self.nodes['independent']['column'] = 'done'
        (workspace_path('source') / '.flow-results' / 'a.json').unlink()
        with self.assertRaisesRegex(ValueError, 'result.*unavailable'):
            build_plan(self.flow, 'b', self.nodes, 'source')

    def test_changed_definition_and_missing_workspace_are_rejected(self):
        self.flow.steps['a'].run = 'echo changed'
        with self.assertRaisesRegex(ValueError, 'definition.*changed'):
            build_plan(self.flow, 'b', self.nodes, 'source')
        with self.assertRaisesRegex(ValueError, 'workspace.*unavailable'):
            build_plan(self.flow, 'b', self.nodes, 'missing')


class RestartService(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from aiohttp import web
        from aiohttp.test_utils import TestServer
        self.requests = []
        self.old_status = 'COMPLETED'
        self.orphan = False
        self.flow = definition()
        async def handler(request):
            path = request.path
            body = await request.json() if request.method == 'POST' else None
            self.requests.append((path, body))
            if path == '/flow_runs/source':
                result = {'state_type': 'COMPLETED', 'flow_id': 'f', 'name': 'original', 'parameters': {}, 'tags': ['board:test']}
            elif path == '/flows/f': result = {'name': self.flow.name}
            elif path == '/task_runs/filter':
                result = [{'id': 'a-new', 'name': 'a', 'state_type': 'COMPLETED', 'expected_start_time': '2026-02-02'},
                          {'id': 'a-old', 'name': 'a', 'state_type': self.old_status, 'expected_start_time': '2026-01-01'},
                          {'id': 'b', 'name': 'b', 'state_type': 'COMPLETED', 'expected_start_time': '2026-02-02'}]
            elif path.startswith('/deployments/name/'): result = {'id': 'deployment'}
            elif path.endswith('/create_flow_run'): result = {'id': 'new'}
            elif path == '/api/v3/runs/source':
                result = {'id': 'source', 'status': 'COMPLETED', 'taskIdentifier': 'flow-' + self.flow.name,
                          'metadata': {'steps': {'b': 'b'}}, 'relatedRuns': {'children': [{'id': 'orphan'}] if self.orphan else []}}
            elif path == '/api/v1/runs':
                self.assertEqual(request.query['filter[tag]'], 'flowrun:source')
                result = {'data': [{'id': 'orphan'}] if self.orphan else [], 'pagination': {}}
            elif path == '/api/v3/runs/orphan': result = {'id': 'orphan', 'status': 'EXECUTING'}
            elif path == '/api/v3/runs/b': result = {'id': 'b', 'status': 'COMPLETED'}
            elif path.startswith('/api/v1/tasks/'): result = {'id': 'new'}
            else: raise AssertionError(path)
            return web.json_response(result)
        app = web.Application(); app.router.add_route('*', '/{path:.*}', handler)
        self.server = TestServer(app); await self.server.start_server()
        url = str(self.server.make_url('')).rstrip('/')
        self.env = patch.dict(os.environ, {'PREFECT_API_URL': url, 'FLOW_TRIGGER_API_URL': url, 'TRIGGER_DEV_SECRET_KEY': 'test'})
        self.env.start()

    async def asyncTearDown(self):
        self.env.stop(); await self.server.close()

    async def invoke(self, system):
        from flowkit import restart_service as service
        with patch.object(service, 'dotenv_values', return_value={}), patch.object(service, 'load_all', return_value=({self.flow.name: self.flow}, {})), patch.object(service, 'build_plan', return_value={'step': 'b'}) as plan:
            result = await service.restart_node(system, 'source', 'b')
            return result, plan.call_args

    async def test_prefect_uses_worker_endpoint_and_newest_attempt(self):
        result, args = await self.invoke('prefect')
        self.assertEqual(result['id'], 'new')
        self.assertEqual(args.args[2]['a']['ref'], 'a-new')
        created = next(body for path, body in self.requests if path.endswith('/create_flow_run'))
        self.assertEqual(created['parameters']['flowRestart'], {'step': 'b'})

    async def test_prefect_rejects_active_older_attempt(self):
        self.old_status = 'RUNNING'
        with self.assertRaisesRegex(ValueError, 'still active'):
            await self.invoke('prefect')
        self.assertFalse(any(path.endswith('/create_flow_run') for path, _ in self.requests))

    async def test_trigger_rejects_active_orphan_not_in_parent_metadata(self):
        self.orphan = True
        with self.assertRaisesRegex(ValueError, 'still active'):
            await self.invoke('trigger-dev')
        self.assertFalse(any(path.startswith('/api/v1/tasks/') for path, _ in self.requests))


if __name__ == '__main__': unittest.main()
