"""Live updates and paging: transcript rows, cached log tails, the board hub and its HTTP routes."""
import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))
from aiohttp.test_utils import TestClient, TestServer

import live
import server
from cards import Card
from logtail import TailCache
from transcript import Session, coalesce, page


class TranscriptRows(unittest.TestCase):
    def test_merges_chunks_tools_and_resolutions_with_stable_keys(self):
        rows = coalesce([
            {'type': 'user', 'text': 'go'},
            {'type': 'agent', 'message_id': 'm1', 'text': 'Hel'},
            {'type': 'tool', 'id': 't1', 'name': 'read', 'status': 'running', 'detail': 'a.py'},
            {'type': 'agent', 'message_id': 'm1', 'text': 'lo'},
            {'type': 'tool', 'id': 't1', 'name': 'tool', 'status': 'ok'},
            {'type': 'approval', 'id': 'p1', 'options': []},
            {'type': 'status', 'state': 'working'},
            {'type': 'approval_resolved', 'id': 'p1', 'optionId': 'allow'},
        ], dropped=10)
        self.assertEqual([r['key'] for r in rows], ['e:10', 'm:m1', 't:t1', 'a:p1'])
        self.assertEqual(rows[1]['text'], 'Hello')
        self.assertEqual((rows[2]['name'], rows[2]['status'], rows[2]['detail']), ('read', 'ok', 'a.py'))
        self.assertEqual(rows[3]['resolved']['optionId'], 'allow')

    def test_pages_back_from_a_row_key(self):
        rows = [{'key': f'e:{i}'} for i in range(250)]
        newest, more = page(rows, None, 100)
        self.assertEqual((newest[0]['key'], newest[-1]['key'], more), ('e:150', 'e:249', True))
        older, more = page(rows, 'e:150', 100)
        self.assertEqual((older[0]['key'], older[-1]['key'], more), ('e:50', 'e:149', True))
        oldest, more = page(rows, 'e:50', 100)
        self.assertEqual((len(oldest), more), (50, False))

    def test_session_projection(self):
        session = Session([{'type': 'status', 'state': 'approval'}, {'type': 'agent', 'text': 'x'}], dropped=3)
        self.assertEqual((session.state, session.truncated, len(session.rows)), ('approval', True, 1))


class Tails(unittest.IsolatedAsyncioTestCase):
    async def test_reads_only_newer_lines_and_freezes_finished_runs(self):
        log = [(1, 'a', 'one'), (2, 'b', 'two')]
        calls = []

        async def fetch(since):
            calls.append(since)
            return [row for row in log if since is None or row[0] >= since - 1]  # overlapping reads
        cache = TailCache()
        self.assertEqual(await cache.read('run', fetch), ['one', 'two'])
        log.append((3, 'c', 'three'))
        self.assertEqual(await cache.read('run', fetch), ['one', 'two', 'three'])
        self.assertEqual(calls, [None, 2])
        await cache.read('run', fetch, final=True)
        log.append((4, 'd', 'late'))
        self.assertEqual(await cache.read('run', fetch, final=True), ['one', 'two', 'three'])
        self.assertEqual(len(calls), 3)  # frozen: no more engine reads
        self.assertEqual(await cache.read('run', fetch), ['one', 'two', 'three', 'late'])  # replayed


class FakeAdapter:
    system = 'fake'

    def __init__(self):
        self.events_list = [{'type': 'agent', 'message_id': f'm{i}', 'text': f'msg {i}'} for i in range(150)]
        self.lines = [f'line {i}' for i in range(1500)]
        self.column = 'running'
        self.feed_wake = None

    def cards(self):
        session = Session(self.events_list)
        a = Card('s1', 'fake', 'b', 'session', 'agent-session', self.column, 'RUNNING')
        a.apply_session(session)
        b = Card('p1', 'fake', 'b', 'plain', 'research', 'running', 'RUNNING')
        b.apply_lines(self.lines)
        return [a, b]

    async def list_cards(self, board):
        return self.cards()

    async def session(self, run_id):
        return Session(self.events_list)

    async def run_lines(self, run_id):
        return self.lines

    async def events(self, run_id):
        return 0, self.events_list

    async def feed(self, board, wake):
        self.feed_wake = wake
        await asyncio.Event().wait()


async def next_message(sub, kind):
    while True:
        message = await asyncio.wait_for(sub.queue.get(), 5)
        if message['type'] == kind:
            return message


class Hub(unittest.IsolatedAsyncioTestCase):
    async def test_paged_older_rows_receive_patches_without_appending_unloaded_history(self):
        self.adapter.events_list.insert(0, {'type': 'tool', 'id': 'old-tool', 'status': 'running'})
        _, sub = self.hub.subscribe('fake', 'b', 's1')
        await next_message(sub, 'rows')
        self.adapter.events_list.append({'type': 'tool', 'id': 'old-tool', 'status': 'ok', 'output': 'done'})
        self.hub.poke('fake')
        message = await next_message(sub, 'rows')
        self.assertEqual(message['rows'], [])
        self.assertEqual([(r['key'], r['status']) for r in message['patches']], [('t:old-tool', 'ok')])
    async def asyncSetUp(self):
        self.adapter = FakeAdapter()
        self.hub = live.Hub({'fake': self.adapter})

    async def asyncTearDown(self):
        await self.hub.close()

    async def test_pushes_changed_cards_and_new_rows_only(self):
        watcher, sub = self.hub.subscribe('fake', 'b', 's1')
        snapshot = await next_message(sub, 'cards')
        self.assertTrue(snapshot['reset'])
        self.assertEqual({c['id'] for c in snapshot['cards']}, {'s1', 'p1'})
        self.assertNotIn('events', snapshot['cards'][0])
        rows = await next_message(sub, 'rows')
        self.assertEqual((rows['reset'], len(rows['rows']), rows['more']), (True, 100, True))
        self.assertEqual(rows['rows'][0]['key'], 'm:m50')

        # A new chunk of the last message and a new message: only those two rows go out.
        self.adapter.events_list += [{'type': 'agent', 'message_id': 'm149', 'text': '!'},
                                     {'type': 'user', 'text': 'next'}]
        self.adapter.feed_wake()
        update = await next_message(sub, 'cards')
        self.assertEqual([c['id'] for c in update['upsert']], ['s1'])
        self.assertEqual(update['upsert'][0]['last'], 'next')
        rows = await next_message(sub, 'rows')
        self.assertEqual([(r['key'], r['text']) for r in rows['rows']], [('m:m149', 'msg 149!'), ('e:151', 'next')])

    async def test_lines_start_with_the_newest_page_and_then_append(self):
        _, sub = self.hub.subscribe('fake', 'b', 'p1')
        first = await next_message(sub, 'lines')
        self.assertEqual((first['start'], len(first['lines']), first['more']), (500, 1000, True))
        self.adapter.lines.append('line 1500')
        self.hub.poke('fake')
        appended = await next_message(sub, 'lines')
        self.assertEqual((appended['start'], appended['lines']), (1500, ['line 1500']))

    async def test_one_watcher_per_board_and_it_stops_when_idle(self):
        w1, s1 = self.hub.subscribe('fake', 'b', None)
        w2, s2 = self.hub.subscribe('fake', 'b', None)
        self.assertIs(w1, w2)
        w1.unsubscribe(s1)
        w2.unsubscribe(s2)
        with patch.object(live, 'IDLE_STOP', 0):
            w1.wake()
            await asyncio.wait_for(w1.task, 5)
        self.assertNotIn(('fake', 'b'), self.hub.watchers)


class Routes(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.adapter = FakeAdapter()
        with patch.object(server, 'load_adapters', return_value={'fake': self.adapter}):
            self.client = TestClient(TestServer(server.make_app()))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_transcript_and_log_pages(self):
        data = await (await self.client.get('/api/cards/fake/s1/transcript?before=m:m50&limit=20')).json()
        self.assertEqual((data['rows'][0]['key'], data['rows'][-1]['key'], data['more']), ('m:m30', 'm:m49', True))
        data = await (await self.client.get('/api/cards/fake/p1/logs?before=500')).json()
        self.assertEqual((data['start'], len(data['lines']), data['more']), (0, 500, False))
        data = await (await self.client.get('/api/cards/fake/p1/logs?after=1498')).json()
        self.assertEqual(data['lines'], ['line 1498', 'line 1499'])
        response = await self.client.get('/api/cards/fake/p1/logs?limit=0')
        self.assertEqual(response.status, 400)

    async def test_live_stream_sends_server_sent_events(self):
        response = await self.client.get('/api/live?system=fake&board=b')
        self.assertEqual(response.headers['Content-Type'], 'text/event-stream')
        messages = []
        while not any(m['type'] == 'cards' for m in messages):
            line = (await asyncio.wait_for(response.content.readline(), 5)).decode()
            if line.startswith('data: '):
                messages.append(json.loads(line[6:]))
        response.close()
        self.assertEqual(len(next(m for m in messages if m['type'] == 'cards')['cards']), 2)


if __name__ == '__main__':
    unittest.main()
