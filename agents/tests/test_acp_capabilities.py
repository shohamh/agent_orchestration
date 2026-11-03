import asyncio
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agentdriver.acp_engine import AcpEngine


class AcpCapabilityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.events = []
        async def emit(kind, **fields): self.events.append({'type': kind, **fields})
        self.engine = AcpEngine(SimpleNamespace(emit=emit, permission=AsyncMock(return_value='allow'), config={'agent': 'codex', 'workspace': '.'}))
        self.engine.connection = SimpleNamespace(set_config_option=AsyncMock(return_value=SimpleNamespace(config_options=[])),
                                                 set_session_mode=AsyncMock())
        self.engine.session_id = 's'

    async def test_usage_and_config_updates_are_forwarded(self):
        await self.engine.session_update('s', {'sessionUpdate': 'usage_update', 'used': 20, 'size': 100, 'cost': {'amount': .2, 'currency': 'USD'}})
        await self.engine.session_update('s', {'sessionUpdate': 'config_option_update', 'configOptions': [{'id': 'mode', 'type': 'select'}]})
        self.assertEqual(self.events[-2]['usage']['used'], 20)
        self.assertEqual(self.events[-1]['configOptions'][0]['id'], 'mode')

    async def test_config_change_uses_agent_protocol(self):
        await self.engine.set_option('permission', 'ask')
        self.engine.connection.set_config_option.assert_awaited_once_with(session_id='s', config_id='permission', value='ask')

    async def test_legacy_mode_change_uses_agent_protocol(self):
        await self.engine.set_mode('review')
        self.engine.connection.set_session_mode.assert_awaited_once_with(session_id='s', mode_id='review')

    async def test_structured_tool_content_survives_transport(self):
        await self.engine.session_update('s', {'sessionUpdate': 'tool_call', 'toolCallId': 't', 'title': 'Edit',
                                                 'content': [{'type': 'diff', 'path': '/a', 'oldText': 'old', 'newText': 'new'}]})
        self.assertEqual(self.events[-1]['content'][0]['newText'], 'new')

    async def test_image_attachment_is_sent_as_acp_image(self):
        self.engine.acp_state['capabilities'] = {'promptCapabilities': {'image': True}}
        self.engine.connection.prompt = AsyncMock(return_value=SimpleNamespace(stop_reason='end_turn'))
        await self.engine.prompt({'text': 'Look', 'attachments': [{'name': 'a.png', 'mimeType': 'image/png', 'data': 'aGk='}]})
        blocks = self.engine.connection.prompt.await_args.kwargs['prompt']
        self.assertEqual([block.type for block in blocks], ['text', 'image'])
        self.assertEqual(blocks[1].data, 'aGk=')

    async def test_client_terminal_streams_output_and_reports_exit(self):
        created = await self.engine.create_terminal('s', sys.executable, ['-c', "import time; print('hello terminal', flush=True); time.sleep(2)"])
        await self.engine.wait_for_terminal_exit('s', created.terminal_id)
        output = await self.engine.terminal_output('s', created.terminal_id)
        self.assertIn('hello terminal', output.output)
        self.assertEqual(output.exit_status.exit_code, 0)
        self.assertTrue(any(e.get('name') == 'terminal' and 'hello terminal' in e.get('output', '') for e in self.events))
        await self.engine.release_terminal('s', created.terminal_id)


if __name__ == '__main__': unittest.main()
