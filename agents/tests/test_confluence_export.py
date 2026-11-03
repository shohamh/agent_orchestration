import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp.test_utils import TestServer
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from flowkit.environment import prepare_sources,validate_sources,configuration
from flowkit.confluence_export import MANIFEST
from confluence_fixture import confluence_app,PNG


class MarkdownExportTests(unittest.IsolatedAsyncioTestCase):
    def test_root_relative_page_links_are_local_only_for_exported_pages(self):
        from flowkit.confluence_export import normalize_markdown_links
        source='[Child](/confluence/spaces/TEST/pages/2/Child#section)\n[Outside](/confluence/spaces/TEST/pages/4/Outside)\n`[example](/confluence/spaces/TEST/pages/2/Child)`\n```\n[x](/confluence/spaces/TEST/pages/2/Child)\n```\n'
        result=normalize_markdown_links(source,'https://example.org/confluence',{'2'})
        self.assertIn('[Child](2.md#section)',result)
        self.assertIn('[Outside](https://example.org/confluence/spaces/TEST/pages/4/Outside)',result)
        self.assertIn('`[example](/confluence/spaces/TEST/pages/2/Child)`',result)
        self.assertIn('```\n[x](/confluence/spaces/TEST/pages/2/Child)\n```',result)

    def test_link_normalization_preserves_nested_backticks_multiline_and_indented_code(self):
        from flowkit.confluence_export import normalize_markdown_links
        source=('``literal ` [x](/spaces/TEST/pages/2/Child)``\n'
                '`multiline code\n[x](/spaces/TEST/pages/2/Child)\nend`\n'
                '    [indented example](/spaces/TEST/pages/2/Child)\n'
                '[ordinary link](/spaces/TEST/pages/2/Child)\n')
        result=normalize_markdown_links(source,'https://example.org',{'2'})
        self.assertIn('``literal ` [x](/spaces/TEST/pages/2/Child)``',result)
        self.assertIn('`multiline code\n[x](/spaces/TEST/pages/2/Child)\nend`',result)
        self.assertIn('    [indented example](/spaces/TEST/pages/2/Child)',result)
        self.assertIn('[ordinary link](2.md)',result)

    async def test_real_exporter_pagination_attachments_and_partial_refresh(self):
        app=confluence_app()
        async with TestServer(app) as server:
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);item={'base_url':str(server.make_url('')).rstrip('/'),'page_id':'1','target':'wiki'}
                config={'confluence':[item]}
                report=await prepare_sources(config,root)
                self.assertTrue(report[0]['ok'],report)
                target=root/'wiki'
                index=json.loads((target/'index.json').read_text())
                self.assertEqual({p['id'] for p in index},{'1','2','3'})
                self.assertTrue(any('content/search?start=1' in r for r in app['requests']))
                text=(target/'2.md').read_text(encoding='utf-8')
                self.assertIn('| Name',text);self.assertIn('print(42)',text)
                attachment=target/'attachments/1/att10.png'
                self.assertEqual(attachment.read_bytes(),PNG)
                self.assertIn('attachments/1/att10.png',(target/'1.md').read_text())
                self.assertIn('[Relative child](2.md)',(target/'1.md').read_text())
                self.assertEqual(await validate_sources(config,root),[])
                app['attachment_versions']['att10']=2
                self.assertTrue(await validate_sources(config,root),'Changed remote attachment must invalidate the import even if the page version did not change')
                app['attachment_versions']['att10']=1
                app['attachment_versions']['att20']=2
                self.assertTrue(await validate_sources(config,root),'Changed descendant attachment must invalidate the import')
                app['attachment_versions']['att20']=1
                attachment.unlink()
                self.assertTrue(await validate_sources(config,root))
                attachment.write_bytes(PNG)
                saved=(target/MANIFEST).read_bytes()
                app['broken_page']='3'
                report=await prepare_sources(config,root)
                self.assertFalse(report[0]['ok'])
                self.assertEqual((target/MANIFEST).read_bytes(),saved)
                self.assertTrue((target/'3.md').is_file())
                self.assertFalse(list(root.glob('.confluence-stage-*')))

    async def test_single_page_isolated_credentials_and_remote_versions(self):
        app=confluence_app()
        async with TestServer(app) as server:
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);item={'base_url':str(server.make_url('')).rstrip('/'),'page_id':'1','target':'wiki',
                    'include_descendants':False,'token_env':'TEST_CONFLUENCE_SECRET'}
                config={'confluence':[item]};secret='dummy-secret-for-confluence-test'
                with patch.dict(os.environ,{'TEST_CONFLUENCE_SECRET':secret,'CME_EXPORT__OUTPUT_PATH':str(root/'wrong-location')}):
                    report=await prepare_sources(config,root)
                    self.assertTrue(report[0]['ok'],report)
                    self.assertIn('Bearer',app['auth_schemes'])
                    self.assertFalse((root/'wrong-location').exists())
                    self.assertEqual(len(json.loads((root/'wiki/index.json').read_text())),1)
                    for path in (root/'wiki').rglob('*'):
                        if path.is_file(): self.assertNotIn(secret.encode(),path.read_bytes())
                    self.assertEqual(await validate_sources(config,root),[])
                    app['versions']['1']=2
                    self.assertTrue(await validate_sources(config,root))
                    app['auth_schemes'].clear()
                    basic={**item,'target':'wiki-basic','username_env':'TEST_CONFLUENCE_USER'}
                    with patch.dict(os.environ,{'TEST_CONFLUENCE_USER':'test@example.invalid'}):
                        basic_report=await prepare_sources({'confluence':[basic]},root)
                    self.assertTrue(basic_report[0]['ok'],basic_report)
                    self.assertIn('Basic',app['auth_schemes'])
                with patch.dict(os.environ,{'TEST_CONFLUENCE_SECRET':''}):
                    report=await prepare_sources(config,root)
                    self.assertFalse(report[0]['ok'])
                    self.assertIn('Missing worker credential',report[0]['error'])

    def test_configuration_rejects_invalid_options(self):
        source={'base_url':'https://wiki.example','page_id':'1','target':'wiki'}
        for extra in ({'include_descendants':'false'},{'attachments':'anything'}):
            with self.assertRaises(ValueError): configuration({'confluence':[{**source,**extra}]})
