import asyncio
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from agentdriver.session import Session


class FakeEngine:
    def __init__(self, session):
        self.session = session
        self.calls = []
        self.failures = 0
        self.block = False
        self.cancelled = asyncio.Event()

    async def start(self):
        pass

    async def prompt(self, text):
        self.calls.append(text)
        if self.failures:
            self.failures -= 1
            raise RuntimeError('temporary failure')
        if self.block:
            await self.cancelled.wait()
            return 'cancelled'
        await self.session.emit('agent', text='Finished this turn', message_id=str(len(self.calls)))
        return 'end_turn'

    async def cancel(self):
        self.cancelled.set()

    async def close(self):
        pass


class SessionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.events = []
        self.inputs = asyncio.Queue()

        async def sink(event):
            self.events.append(event)

        self.session = Session({'goal': 'fix it', 'agent': 'demo', 'retryDelay': 0}, self.inputs.get, sink)
        self.engine = FakeEngine(self.session)
        self.session.engine_factory = lambda: self.engine
        self.task = asyncio.create_task(self.session.run())

    async def asyncTearDown(self):
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)

    async def state(self, state):
        async with asyncio.timeout(3):
            while self.session.state != state:
                await asyncio.sleep(.005)

    async def test_turn_needs_human_review_and_continue_does_not_end_session(self):
        await self.state('awaiting_review')
        self.assertFalse(self.task.done())
        await self.inputs.put({'type': 'message', 'text': 'continue'})
        while len(self.engine.calls) < 2:
            await asyncio.sleep(.005)
        await self.state('awaiting_review')
        await self.inputs.put({'type': 'done'})
        result = await asyncio.wait_for(self.task, 3)
        self.assertEqual(result['state'], 'done')
        self.assertEqual(self.engine.calls, ['fix it', 'continue'])

    async def test_retry_exhaustion_waits_for_continue(self):
        self.engine.failures = 3
        await self.state('stuck')
        self.assertEqual(len(self.engine.calls), 3)
        self.assertFalse(self.task.done())
        await self.inputs.put({'type': 'message', 'text': 'continue'})
        await self.state('awaiting_review')

    async def test_interrupt_keeps_session_open_and_queued_steering_runs_next(self):
        self.engine.block = True
        await self.state('working')
        await self.inputs.put({'type': 'interrupt'})
        await self.state('awaiting_input')
        self.assertFalse(self.task.done())
        self.engine.block = False
        await self.inputs.put({'type': 'message', 'text': 'try another approach'})
        await self.state('awaiting_review')
        self.assertEqual(self.engine.calls[-1], 'try another approach')

    async def test_permission_only_accepts_matching_id_and_option(self):
        await self.state('awaiting_review')
        request = asyncio.create_task(self.session.permission('shell', 'Run tests', '', [
            {'id': 'yes', 'label': 'Allow', 'kind': 'allow_once'},
            {'id': 'no', 'label': 'Deny', 'kind': 'reject_once'}]))
        await self.state('approval')
        event = next(e for e in reversed(self.events) if e['type'] == 'approval')
        await self.inputs.put({'type': 'approval', 'id': 'stale', 'optionId': 'yes'})
        await self.inputs.put({'type': 'approval', 'id': event['id'], 'optionId': 'bogus'})
        await asyncio.sleep(.03)
        self.assertFalse(request.done())
        await self.inputs.put({'type': 'approval', 'id': event['id'], 'optionId': 'no'})
        self.assertEqual(await request, 'no')

    async def test_interrupt_resolves_pending_permission_as_cancelled(self):
        await self.state('awaiting_review')
        request = asyncio.create_task(self.session.permission('shell', 'Run tests', '', [
            {'id': 'yes', 'label': 'Allow', 'kind': 'allow_once'}]))
        await self.state('approval')
        await self.inputs.put({'type': 'interrupt'})
        self.assertIsNone(await asyncio.wait_for(request, 3))
        self.assertTrue(any(e['type'] == 'approval_resolved' and e['decision'] == 'cancelled' for e in self.events))

    async def test_question_preserves_typed_values_and_rejects_invalid_answer(self):
        await self.state('awaiting_review')
        schema = {'type': 'object', 'properties': {
            'choice': {'type': 'string', 'enum': ['a', 'b']},
            'many': {'type': 'array', 'items': {'type': 'string', 'enum': ['x', 'y']}}},
            'required': ['choice']}
        question = asyncio.create_task(self.session.question('Choose', schema))
        await self.state('question')
        event = next(e for e in reversed(self.events) if e['type'] == 'question')
        await self.inputs.put({'type': 'answer', 'id': event['id'], 'answers': {'choice': 'invalid'}})
        await asyncio.sleep(.03)
        self.assertFalse(question.done())
        answer = {'choice': 'b', 'many': ['x', 'y']}
        await self.inputs.put({'type': 'answer', 'id': event['id'], 'answers': answer})
        self.assertEqual(await question, answer)

    async def test_done_while_working_cannot_end_a_running_turn(self):
        self.engine.block = True
        await self.state('working')
        await self.inputs.put({'type': 'done'})
        await asyncio.sleep(.03)
        self.assertFalse(self.task.done())
        self.assertFalse(self.session.finishing)
        await self.inputs.put({'type': 'interrupt'})
        await self.state('awaiting_input')

    async def test_busy_messages_are_queued_and_run_after_current_turn(self):
        self.engine.block = True
        await self.state('working')
        await self.inputs.put({'type': 'message', 'text': 'next instruction'})
        await asyncio.sleep(.03)
        self.assertEqual(self.engine.calls, ['fix it'])
        self.engine.block = False
        self.engine.cancelled.set()
        async with asyncio.timeout(3):
            while len(self.engine.calls) < 2:
                await asyncio.sleep(.005)
        self.assertEqual(self.engine.calls, ['fix it', 'next instruction'])

    async def test_interrupt_during_retry_delay_prevents_another_attempt(self):
        self.engine.failures = 1
        self.session.config['retryDelay'] = .2
        await self.state('retrying')
        await self.inputs.put({'type': 'interrupt'})
        await self.state('awaiting_input')
        await self.inputs.put({'type': 'done'})
        await asyncio.wait_for(self.task, 3)
        self.assertEqual(self.engine.calls, ['fix it'])

    async def test_slow_cancel_does_not_crash_when_active_turn_has_ended(self):
        self.engine.block = True

        async def delayed_cancel():
            self.engine.cancelled.set()
            await asyncio.sleep(.05)

        self.engine.cancel = delayed_cancel
        await self.state('working')
        await self.inputs.put({'type': 'interrupt'})
        await self.state('awaiting_input')
        await asyncio.sleep(.1)
        self.assertFalse(self.task.done())
        await self.inputs.put({'type': 'done'})
        await asyncio.wait_for(self.task, 3)

    async def test_transport_eof_while_working_closes_active_engine(self):
        self.engine.block = True
        closed = []

        async def close():
            closed.append(True)

        async def receive():
            await self.inputs.get()
            raise EOFError('test transport closed')

        self.engine.close = close
        self.session.receive = receive
        await self.state('working')
        await self.inputs.put({})
        # The first receive may already be blocked on the queue from asyncSetUp.
        await self.inputs.put({})
        with self.assertRaises(EOFError):
            await asyncio.wait_for(asyncio.shield(self.task), 1)
        self.assertEqual(closed, [True])

    async def test_restarted_connection_receives_original_goal_and_followup(self):
        await self.state('awaiting_review')
        self.engine.failures = 1
        replacement = FakeEngine(self.session)
        self.session.engine_factory = lambda: replacement
        await self.inputs.put({'type': 'message', 'text': 'keep the API compatible'})
        async with asyncio.timeout(3):
            while not replacement.calls:
                await asyncio.sleep(.005)
        self.assertIn('fix it', replacement.calls[0])
        self.assertIn('keep the API compatible', replacement.calls[0])

    async def test_cancellation_during_permission_logging_completes_future_once(self):
        await self.state('awaiting_review')
        entered, release = asyncio.Event(), asyncio.Event()
        original_sink = self.session.emit._sink

        async def slow_sink(event):
            await original_sink(event)
            if event['type'] == 'approval_resolved' and event.get('decision') == 'allow':
                entered.set()
                await release.wait()

        self.session.emit._sink = slow_sink
        request = asyncio.create_task(self.session.permission('shell', 'test', '', [
            {'id': 'yes', 'label': 'Allow', 'kind': 'allow_once'}]))
        await self.state('approval')
        event = next(e for e in reversed(self.events) if e['type'] == 'approval')
        resolve = asyncio.create_task(self.session._resolve({'type': 'approval', 'id': event['id'], 'optionId': 'yes'}))
        await entered.wait()
        cancel = asyncio.create_task(self.session._cancel_pending())
        await asyncio.sleep(.02)
        release.set()
        await asyncio.gather(resolve, cancel)
        self.assertIn(await request, [None, 'yes'])

    async def test_new_permission_after_interrupt_is_cancelled_immediately(self):
        await self.state('awaiting_review')
        await self.inputs.put({'type': 'interrupt'})
        await self.state('awaiting_input')
        result = await asyncio.wait_for(self.session.permission('shell', 'late', '', []), .1)
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
