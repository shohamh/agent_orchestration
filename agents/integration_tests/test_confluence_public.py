"""Live, anonymous Confluence imports. These exercise real public services and require internet."""
import sys
import tempfile
import time
import unittest
import asyncio
import os
from pathlib import Path
from unittest.mock import patch

import yaml
from aiohttp.test_utils import TestServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from confluence_smoke_test import run_public_checks
from flowkit.definition import parse
from flowkit.distributed import execute_step
from agents.tests.worker_fixture import make_app  # each simulated PC gets its own capacity gate


class PublicConfluenceIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_public_page_trees_are_exported_and_validated(self):
        with tempfile.TemporaryDirectory(prefix='confluence-public-test-') as directory:
            results = await run_public_checks(directory)
            by_target = {Path(result['target']).name: result for result in results}
            self.assertEqual(set(by_target), {'apache-tika-tree', 'apache-tika', 'atlassian', 'shibboleth'})
            self.assertTrue(all(result['validated'] for result in results))
            tree = by_target['apache-tika-tree']
            self.assertTrue({'148639291', '191334866', '240883999'} <= set(tree['page_ids']))
            root = Path(tree['target']) / '148639291.md'
            text = root.read_text(encoding='utf-8')
            self.assertIn('(191334866.md)', text)
            self.assertIn('(240883999.md)', text)
            self.assertTrue((Path(tree['target']) / '191334866.md').is_file())
            self.assertGreater(tree['files'], tree['pages'] + 2, 'Referenced attachments were not exported')
            for result in results:
                self.assertTrue((Path(result['target']) / 'confluence-export.json').is_file())
                self.assertTrue((Path(result['target']) / 'index.json').is_file())

    async def test_public_cloud_page_crosses_worker_boundary_after_review(self):
        with tempfile.TemporaryDirectory(prefix='confluence-worker-test-') as directory:
            root=Path(directory)
            services=[TestServer(make_app(worker_id=name,root=root/name,pool='test',slots=1))
                      for name in ('setup-pc','step-pc')]
            for service in services: await service.start_server()
            registry=root/'workers.yaml'
            registry.write_text(yaml.safe_dump({'workers':[{'id':name,'pool':'test','tags':[],'cpu_limit':100,'ram_limit':100,'settings_revision':time.time_ns(),  # admission must not depend on this PC's load
                'url':str(service.make_url('')).rstrip('/')} for name,service in zip(('setup-pc','step-pc'),services)]}),encoding='utf-8')
            flow=parse({'name':'public-context-test','environment':{'agent':'demo',
                'confluence':[{'base_url':'https://shibboleth.atlassian.net/wiki','page_id':'2067399657',
                               'target':'context/wiki','include_descendants':False}]},
                'steps':{'verify':{'kind':'program','run':'''python -c "from pathlib import Path; assert 'SystemRequirements' in Path('context/wiki/2067399657.md').read_text(encoding='utf-8'); print('public markdown received')"'''}}})
            execution={'steps':{'setup_environment':{'id':'setup-pc'},'verify':{'id':'step-pc'}}}
            inbox=asyncio.Queue(); events=[]
            async def sink(event):
                events.append(event)
                if event.get('type')=='status' and event.get('state')=='awaiting_review':
                    await inbox.put({'type':'done'})
            try:
                with patch.dict(os.environ,{'FLOW_WORKERS_FILE':str(registry),'FLOW_WORKSPACES':str(root/'canonical')}):
                    setup=await asyncio.wait_for(execute_step(flow,'setup_environment',{'input':{},'steps':{},
                        '_flow_execution':execution},'public-run',inbox.get,sink),180)
                    self.assertTrue(setup['validated'])
                    result=await asyncio.wait_for(execute_step(flow,'verify',{'input':{},'steps':{'setup_environment':{'output':setup}},
                        '_flow_execution':execution},'public-run',inbox.get,sink),60)
                    self.assertIn('public markdown received',result['stdout'])
                    self.assertEqual([e['worker']['id'] for e in events if e.get('type')=='worker'],['setup-pc','step-pc'])
            finally:
                for service in services: await service.close()


if __name__ == '__main__':
    unittest.main()
