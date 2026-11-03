"""Incremental, cached reads of a run's log lines.

The first read of a run fetches its whole log once; later reads ask the engine only for lines
newer than the last one seen. A finished run's log is kept as is and never fetched again.
"""

import asyncio
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

# fetch(since) -> rows newer than `since` (None = everything), each (cursor, dedupe key, message),
# oldest first. A fetch may return rows it already returned (an overlap window); the key drops them.
Fetch = Callable[[Any], Awaitable[list[tuple[Any, Any, str]]]]


@dataclass
class Tail:
    lines: list[str] = field(default_factory=list)
    cursor: Any = None
    keys: set = field(default_factory=set)
    messages: set = field(default_factory=set)
    final: bool = False
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)


class TailCache:
    def __init__(self, size: int = 400, unique_messages: bool = False) -> None:
        self.size = size
        # Durable Hatchet tasks can replay after eviction and log a step again: keep the first.
        self.unique_messages = unique_messages
        self.tails: OrderedDict[str, Tail] = OrderedDict()

    async def read(self, key: str, fetch: Fetch, final: bool = False) -> list[str]:
        tail = self.tails.get(key)
        if tail is None:
            tail = self.tails[key] = Tail()
            while len(self.tails) > self.size:
                self.tails.popitem(last=False)
        self.tails.move_to_end(key)
        if tail.final and not final:  # replayed: read the log again from the start
            tail = self.tails[key] = Tail()
        async with tail.lock:
            if tail.final:
                return tail.lines
            for cursor, dedupe, message in await fetch(tail.cursor):
                if dedupe in tail.keys:
                    continue
                tail.keys.add(dedupe)
                if self.unique_messages:
                    if message in tail.messages:
                        continue
                    tail.messages.add(message)
                tail.lines.append(message)
                if tail.cursor is None or cursor > tail.cursor:
                    tail.cursor = cursor
            if final:
                # Read once more after the run ended, then freeze: nothing else will be logged.
                tail.final, tail.keys, tail.messages = True, set(), set()
            return tail.lines

    def forget(self, key: str) -> None:
        self.tails.pop(key, None)
