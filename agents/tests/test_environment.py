import asyncio
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from aiohttp import web
from aiohttp.test_utils import TestServer
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit.definition import parse, FlowError
from flowkit.environment import prepare_sources, validate_sources
from confluence_fixture import confluence_app


class EnvironmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_setup_expands_to_native_dependency_and_roundtrips(self):
        flow = parse({'name': 'context-test', 'environment': {'agent': 'demo', 'files': [{'source': 'x', 'target': 'context/x'}]},
                      'steps': {'work': {'kind': 'program', 'run': 'echo hello'}}})
        self.assertEqual(list(flow.steps), ['setup_environment', 'work'])
        self.assertEqual(flow.steps['work'].needs, ['setup_environment'])
        self.assertEqual(parse(flow.to_json()).to_json(), flow.to_json())
        with self.assertRaises(FlowError):
            parse({'name': 'bad', 'environment': {'files': [{'source': 'x', 'target': '../escape'}]}, 'steps': {'x': {'kind': 'program', 'run': 'x'}}})

    async def test_git_files_paginated_confluence_and_incomplete_export(self):
        app = confluence_app()
        async with TestServer(app) as server:
            with tempfile.TemporaryDirectory() as tmp:
                root = Path(tmp); repo = root/'source'; repo.mkdir()
                (repo/'hello.txt').write_text('repo context')
                for args in (['init'], ['add', '.'], ['-c', 'user.name=Test', '-c', 'user.email=test@example.invalid', 'commit', '-m', 'seed']):
                    subprocess.run(['git', '-C', str(repo), *args], check=True, capture_output=True)
                source = root/'notes.txt'; source.write_text('file context')
                config = {'repositories': [{'url': str(repo), 'target': 'repos/sample'}],
                          'files': [{'source': str(source), 'target': 'context/notes.txt'}],
                          'confluence': [{'base_url': str(server.make_url('')).rstrip('/'), 'page_id': '1', 'target': 'context/wiki'}]}
                workspace = root/'workspace'; workspace.mkdir()
                report = await prepare_sources(config, workspace)
                self.assertTrue(all(item['ok'] for item in report), report)
                self.assertEqual(await validate_sources(config, workspace), [])
                self.assertEqual((workspace/'repos/sample/hello.txt').read_text(), 'repo context')
                (workspace/'repos/sample/hello.txt').unlink()
                self.assertTrue(await validate_sources(config,workspace), 'Missing tracked files must block setup completion')
                subprocess.run(['git','-C',str(workspace/'repos/sample'),'restore','.'],check=True,capture_output=True)
                self.assertEqual(len(json.loads((workspace/'context/wiki/index.json').read_text())), 3)
                index=json.loads((workspace/'context/wiki/index.json').read_text())
                page3=workspace/'context/wiki'/next(p['file'] for p in index if p['id']=='3')
                self.assertEqual(page3.suffix,'.md')
                self.assertIn('**content 3**',page3.read_text())
                page3.unlink()
                self.assertTrue(await validate_sources(config, workspace))
                await prepare_sources(config, workspace)
                self.assertEqual(await validate_sources(config, workspace), [])

    async def test_missing_source_is_reported_and_never_validated_by_directory_existence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); (root/'partial').mkdir()
            config={'repositories':[{'url': str(root/'missing'), 'target':'partial'}]}
            report=await prepare_sources(config, root)
            self.assertFalse(report[0]['ok'])
            self.assertTrue(await validate_sources(config, root))


if __name__ == '__main__': unittest.main()
