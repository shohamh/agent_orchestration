import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agentdriver.session import Session
from agentdriver.acp_engine import AcpEngine


class AcpTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_can_be_loaded_after_reconnect(self):
        events = []
        async def sink(event): events.append(event)
        async def receive(): await asyncio.Event().wait()
        with tempfile.TemporaryDirectory() as workspace:
            session = Session({'goal': 'fix', 'agent': 'codex', 'workspace': workspace}, receive, sink)
            command = [sys.executable, str(Path(__file__).with_name('fake_acp_agent.py'))]
            first = AcpEngine(session, command=command)
            await first.start()
            await first.close()
            second = AcpEngine(session, command=command)
            try:
                await second.start()
                self.assertTrue(second.resumed)
                self.assertEqual(second.session_id, 'test-session')
                self.assertFalse(any(e.get('text') == 'Past response' for e in events))
            finally:
                await second.close()

    async def test_explicit_resume_replays_previous_messages(self):
        events = []
        async def sink(event): events.append(event)
        async def receive(): await asyncio.Event().wait()
        with tempfile.TemporaryDirectory() as workspace:
            session = Session({'goal': 'continue', 'agent': 'codex', 'workspace': workspace,
                               'resumeSessionId': 'test-session'}, receive, sink)
            engine = AcpEngine(session, command=[sys.executable, str(Path(__file__).with_name('fake_acp_agent.py'))])
            try:
                await engine.start()
                self.assertTrue(engine.resumed)
                self.assertTrue(any(e.get('text') == 'Past response' for e in events))
            finally:
                await engine.close()

    async def test_real_stdio_transport_round_trip_with_permission_and_form(self):
        events = []
        inputs = asyncio.Queue()

        async def sink(event):
            events.append(event)
            if event['type'] == 'approval':
                await inputs.put({'type': 'approval', 'id': event['id'], 'optionId': 'once'})
            elif event['type'] == 'question':
                await inputs.put({'type': 'answer', 'id': event['id'], 'answers': {'style': 'short'}})
            elif event['type'] == 'status' and event['state'] == 'awaiting_review':
                await inputs.put({'type': 'done'})

        with tempfile.TemporaryDirectory() as workspace:
            session = Session({'goal': 'fix', 'agent': 'codex', 'workspace': workspace, 'retryDelay': 0}, inputs.get, sink)
            session.engine_factory = lambda: AcpEngine(session, command=[sys.executable, str(Path(__file__).with_name('fake_acp_agent.py'))])
            result = await asyncio.wait_for(session.run(), 20)
        self.assertEqual(result['state'], 'done')
        self.assertEqual([e['commands'] for e in events if e['type'] == 'commands'][-1],
                         [{'name': 'test', 'description': 'Run tests', 'input': {'hint': 'path'}}])
        self.assertEqual(''.join(e['text'] for e in events if e['type'] == 'agent'), 'It works.')
        self.assertFalse(any(e['type'] == 'error' for e in events), events)


if __name__ == '__main__':
    unittest.main()
