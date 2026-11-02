import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).parent))


class InboxTests(unittest.IsolatedAsyncioTestCase):
    async def test_orders_and_deduplicates_scoped_inputs(self):
        from hatchet_worker import InputInbox
        early = SimpleNamespace(metadata=SimpleNamespace(id='a', created_at=datetime(2026, 1, 1, tzinfo=timezone.utc)), payload={'type': 'message', 'text': 'first'})
        late = SimpleNamespace(metadata=SimpleNamespace(id='b', created_at=datetime(2026, 1, 2, tzinfo=timezone.utc)), payload={'type': 'done'})
        calls = []
        class Events:
            async def aio_list(self, **kwargs):
                calls.append(kwargs)
                return SimpleNamespace(rows=[late, early, late])
        inbox = InputInbox(Events(), 'run-1')
        await inbox.poll()
        await inbox.poll()
        self.assertEqual(await inbox.receive(), {'type': 'message', 'text': 'first'})
        self.assertEqual(await inbox.receive(), {'type': 'done'})
        self.assertTrue(inbox.queue.empty())
        self.assertEqual(calls[0]['keys'], ['agent:input'])
        self.assertEqual(calls[0]['scopes'], ['run-1'])
        self.assertIsNotNone(calls[1]['since'])


if __name__ == '__main__':
    unittest.main()
