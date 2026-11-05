"""Live board updates: one shared watcher per (system, board), pushed to browsers over SSE.

A watcher wakes when its engine's live feed reports a change (Trigger.dev Realtime, Prefect's
event and log websockets, Hatchet's run event stream), refreshes the board once, and pushes only
the cards that changed to every browser watching that board. A browser with a card open also
gets that card's new transcript rows (agent sessions) or log lines. A quiet or broken feed
falls back to polling: the board every LIST_EVERY seconds, an open card every DETAIL_EVERY.
"""

import asyncio
import hashlib
import json
import logging
import time
from typing import Any

from transcript import digest, page
from steps import node_session

log = logging.getLogger("board.live")

TRANSCRIPT_PAGE = 100  # rows of an agent session sent when it opens
LOG_PAGE = 1000  # lines of an ordinary log sent when it opens
LIST_EVERY = 5.0
DETAIL_EVERY = 1.0
IDLE_STOP = 30.0  # keep a watcher (and its feed) this long after the last viewer leaves
DEBOUNCE = 0.15  # let a burst of feed events settle into one refresh
QUEUE_SIZE = 500


class Subscriber:
    def __init__(self, card: str | None, node: str | None = None, kind: str | None = None) -> None:
        self.card = card
        self.node, self.kind = node, kind
        self.node_sub = Subscriber(card) if node else None
        self.graph_version = None
        self.graph_finished = None
        self.queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(QUEUE_SIZE)
        self.closed = False
        # What this browser has of the open card: row digests from `anchor` on, or the log length.
        self.rows: dict[str, str] | None = None
        self.anchor: str | None = None
        self.version: str | None = None
        self.lines_end: int | None = None
        self.lines_tail: str | None = None
        self.refreshing = False  # a refresh of the open card is in flight

    def send(self, message: dict[str, Any]) -> None:
        if self.closed:
            return
        try:
            self.queue.put_nowait(message)
        except asyncio.QueueFull:
            # A browser that stopped reading: drop it. EventSource reconnects and starts afresh.
            self.closed = True
            while not self.queue.empty():
                self.queue.get_nowait()
            self.queue.put_nowait(None)


class BoardWatcher:
    def __init__(self, hub: "Hub", system: str, adapter: Any, board: str) -> None:
        self.hub, self.system, self.adapter, self.board = hub, system, adapter, board
        self.subscribers: set[Subscriber] = set()
        self.cards: dict[str, dict[str, Any]] | None = None
        self.hashes: dict[str, str] = {}
        self.event = asyncio.Event()
        self.left_at = time.monotonic()
        self.feed_live = False
        self.details = {}
        self.refreshes: set[asyncio.Task] = set()
        self.task = asyncio.create_task(self.run())

    def wake(self) -> None:
        self.event.set()

    def subscribe(self, sub: Subscriber) -> None:
        self.subscribers.add(sub)
        sub.send({"type": "feed", "live": self.feed_live})
        if self.cards is not None:
            sub.send({"type": "cards", "reset": True, "cards": list(self.cards.values())})
            self.start_refresh(sub)  # its open card's transcript goes out now, not after the current pass
        self.wake()

    def start_refresh(self, sub: Subscriber) -> None:
        """Refresh one browser's open card in the background. Engine reads can stall for minutes,
        so a slow card never holds back other browsers; a browser still refreshing is skipped."""
        if not sub.card or sub.refreshing:
            return
        sub.refreshing = True

        async def refresh() -> None:
            try:
                await self.refresh_card(sub)
            except Exception as exc:
                sub.send({"type": "error", "message": str(exc)})
            finally:
                sub.refreshing = False

        task = asyncio.create_task(refresh())
        self.refreshes.add(task)
        task.add_done_callback(self.refreshes.discard)

    def unsubscribe(self, sub: Subscriber) -> None:
        self.subscribers.discard(sub)
        if not self.subscribers:
            self.left_at = time.monotonic()

    def broadcast(self, message: dict[str, Any]) -> None:
        for sub in list(self.subscribers):
            sub.send(message)

    def set_feed_live(self, live: bool) -> None:
        if live != self.feed_live:
            self.feed_live = live
            self.broadcast({"type": "feed", "live": live})

    async def run_feed(self) -> None:
        if not hasattr(self.adapter, "feed"):
            return
        delay = 1.0
        while True:
            started = time.monotonic()
            self.set_feed_live(True)
            try:
                await self.adapter.feed(self.board, self.wake)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # the feed is an optimisation: polling carries on without it
                log.warning("%s/%s live feed stopped: %s", self.system, self.board, exc)
            self.set_feed_live(False)
            delay = 1.0 if time.monotonic() - started > 60 else min(delay * 2, 30.0)
            await asyncio.sleep(delay)
            self.wake()  # catch up on anything missed while the feed was down

    async def run(self) -> None:
        feed = asyncio.create_task(self.run_feed())
        last_list = float("-inf")
        list_task: asyncio.Task | None = None
        list_pending = True
        try:
            while True:
                now = time.monotonic()
                timeout = LIST_EVERY - (now - last_list)
                watching_card = any(sub.card for sub in self.subscribers)
                if list_task is not None and not list_task.done():
                    # The next list refresh cannot start yet; avoid a 50 ms polling loop
                    # once the normal listing interval has elapsed.
                    timeout = DETAIL_EVERY if watching_card else LIST_EVERY
                if watching_card:
                    timeout = min(timeout, DETAIL_EVERY)
                try:
                    await asyncio.wait_for(self.event.wait(), max(0.05, timeout))
                    woke = True
                    await asyncio.sleep(DEBOUNCE)
                except asyncio.TimeoutError:
                    woke = False
                self.event.clear()
                if not self.subscribers:
                    if time.monotonic() - self.left_at > IDLE_STOP:
                        return
                    continue
                try:
                    list_pending |= woke or time.monotonic() - last_list >= LIST_EVERY or self.cards is None
                    if list_task is not None and list_task.done():
                        try:
                            list_task.result()
                        except Exception as exc:
                            log.exception("%s/%s refresh failed", self.system, self.board)
                            self.broadcast({"type": "error", "message": str(exc)})
                        list_task = None
                    if list_pending and list_task is None:
                        last_list = time.monotonic()
                        list_pending = False
                        if self.cards is None:
                            # Publish the cheap snapshot first, so open-card details can be sent
                            # while the full list fetches other runs' logs in the background.
                            try:
                                await self.publish(await self.adapter.list_cards(self.board, light=True))
                            except TypeError:  # an adapter without a light listing
                                pass
                        list_task = asyncio.create_task(self.refresh_cards())
                    self.details = {}  # each open run/step is read once per shared watcher pass
                    for sub in list(self.subscribers):
                        self.start_refresh(sub)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception("%s/%s refresh failed", self.system, self.board)
                    self.broadcast({"type": "error", "message": str(exc)})
                    await asyncio.sleep(2)
        finally:
            feed.cancel()
            for task in [list_task, *self.refreshes]:
                if task is not None:
                    task.cancel()
            await asyncio.gather(*(t for t in [list_task, *self.refreshes] if t is not None), return_exceptions=True)
            if self.hub.watchers.get((self.system, self.board)) is self:
                del self.hub.watchers[(self.system, self.board)]
            for sub in list(self.subscribers):
                sub.send({"type": "error", "message": "live updates stopped"})

    async def refresh_cards(self) -> None:
        await self.publish(await self.adapter.list_cards(self.board))

    async def publish(self, listed: list[Any]) -> None:
        if self.hub.decorate:
            try:  # labels are extras: never hold a live update back for them
                await asyncio.wait_for(self.hub.decorate(listed), 2)
            except asyncio.TimeoutError:
                log.warning("%s/%s: labelling cards timed out", self.system, self.board)
        cards = {card.id: card.to_json() for card in listed}
        # Field order is fixed by the Card dataclass, so unsorted JSON is a stable (and much cheaper) key.
        hashes = {card_id: hashlib.blake2b(json.dumps(card, default=str).encode(), digest_size=8).digest()
                  for card_id, card in cards.items()}
        first = self.cards is None
        upsert = [cards[i] for i in cards if self.hashes.get(i) != hashes[i]]
        remove = [i for i in self.hashes if i not in cards]
        self.cards, self.hashes = cards, hashes
        if first:
            self.broadcast({"type": "cards", "reset": True, "cards": list(cards.values())})
        elif upsert or remove:
            self.broadcast({"type": "cards", "upsert": upsert, "remove": remove})

    async def refresh_card(self, sub: Subscriber) -> None:
        card = (self.cards or {}).get(sub.card)
        if card is None:
            return
        if card["task"] == "agent-session":
            await self.push_rows(sub, card)
        if sub.node:
            # Step activity may not change the parent run's status. Always refresh the selected
            # step on feed wakes and fallback ticks; it shares the board watcher and SSE socket.
            # It goes first: it is what the browser is showing, and the run's reads can be slow.
            if sub.kind == "agent":
                session = await self.detail(("node-session", sub.node), lambda: node_session(self.adapter, sub.node))
                await self.push_rows(sub.node_sub, card, session, sub)
            else:
                lines = await self.detail(("node-lines", sub.node), lambda: self.adapter.node_lines(sub.node))
                await self.push_lines(sub.node_sub, card, lines, sub)
        if card["task"] != "agent-session":
            await self.push_lines(sub, card)
        if card["task"] != "agent-session" and hasattr(self.adapter, "dag"):
            final = card["column"] in ("done", "stopped")
            if not final or sub.graph_version is None or sub.graph_finished != card.get("finished_at"):
                graph = await self.detail(("dag", card["id"]), lambda: self.adapter.dag(card["id"]))
                version = digest(graph.to_json())
                if sub.graph_version != version:
                    sub.send({"type": "graph", "card": card["id"]})
                    sub.graph_version = version
                sub.graph_finished = card.get("finished_at") if final else None

    async def detail(self, key, fetch):
        # Browsers refresh concurrently: share one in-flight read, and don't let one browser
        # leaving cancel it for the others.
        if key not in self.details:
            self.details[key] = asyncio.ensure_future(fetch())
        return await asyncio.shield(self.details[key])

    async def push_rows(self, sub: Subscriber, card: dict[str, Any], session=None, owner=None) -> None:
        session = session or await self.detail(("session", card["id"]), lambda: self.adapter.session(card["id"]))
        def send(message):
            if owner:
                message.update(type="node-rows", node=owner.node, state=session.state)
            (owner or sub).send(message)
        rows = session.rows
        if sub.rows is None:
            shown, more = page(rows, None, TRANSCRIPT_PAGE)
            sub.rows = {row["key"]: digest(row) for row in rows}
            sub.anchor = shown[0]["key"] if shown else None
            sub.version = session.version
            send({"type": "rows", "card": card["id"], "reset": True, "rows": shown, "more": more,
                      "truncated": session.truncated})
            return
        if session.version == sub.version:
            return
        sub.version = session.version
        start = 0
        if sub.anchor is not None:
            start = next((i for i, row in enumerate(rows) if row["key"] == sub.anchor), 0)
        changed, patches = [], []
        for index, row in enumerate(rows):
            hashed = digest(row)
            if sub.rows.get(row["key"]) != hashed:
                sub.rows[row["key"]] = hashed
                (patches if index < start else changed).append(row)
        if sub.anchor is None and changed:
            sub.anchor = changed[0]["key"]
        # A state-only change (approval -> working) must update the controls too.
        send({"type": "rows", "card": card["id"], "rows": changed, "patches": patches})

    async def push_lines(self, sub: Subscriber, card: dict[str, Any], lines=None, owner=None) -> None:
        lines = lines if lines is not None else await self.detail(("lines", card["id"]), lambda: self.adapter.run_lines(card["id"]))
        def send(message):
            if owner:
                message.update(type="node-lines", node=owner.node)
            (owner or sub).send(message)
        tail = digest(lines[-50:])
        grew = sub.lines_end is not None and len(lines) > sub.lines_end
        if sub.lines_end is None or len(lines) < sub.lines_end or (len(lines) == sub.lines_end and tail != sub.lines_tail):
            # First send, or the log was rewritten (a DAG's task statuses, a rolling window).
            start = max(0, len(lines) - LOG_PAGE)
            send({"type": "lines", "card": card["id"], "reset": True, "start": start, "lines": lines[start:],
                      "more": start > 0})
        elif grew:
            send({"type": "lines", "card": card["id"], "start": sub.lines_end, "lines": lines[sub.lines_end:]})
        sub.lines_end, sub.lines_tail = len(lines), tail


class Hub:
    def __init__(self, adapters: dict[str, Any], decorate: Any = None) -> None:
        self.adapters = adapters
        self.decorate = decorate  # async (cards) -> None: labels cards (e.g. their portable flow)
        self.watchers: dict[tuple[str, str], BoardWatcher] = {}

    def subscribe(self, system: str, board: str, card: str | None, node=None, kind=None) -> tuple[BoardWatcher, Subscriber]:
        watcher = self.watchers.get((system, board))
        if watcher is None or watcher.task.done():
            watcher = self.watchers[(system, board)] = BoardWatcher(self, system, self.adapters[system], board)
        sub = Subscriber(card, node, kind)
        watcher.subscribe(sub)
        return watcher, sub

    def poke(self, system: str) -> None:
        """Something changed through the board itself (a new card, an input): refresh now."""
        for (watched, _), watcher in self.watchers.items():
            if watched == system:
                watcher.wake()

    async def close(self) -> None:
        for watcher in list(self.watchers.values()):
            watcher.task.cancel()
        await asyncio.gather(*(w.task for w in self.watchers.values()), return_exceptions=True)
