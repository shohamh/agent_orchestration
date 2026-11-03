"""Coalesce raw session events into transcript rows (mirrors AgentChat.transcript in static/chat.js).

A row is what the chat shows as one item: agent chunks merged by `message_id`, a tool's updates
merged by `id`, and an approval/question with its resolution attached. Every row carries a stable
`key`, so the board can page the transcript and push only rows that changed.
"""

import hashlib
import json
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Callable

SHOWN = ("user", "agent", "error", "plan")


def coalesce(events: list[dict[str, Any]], dropped: int = 0) -> list[dict[str, Any]]:
    """`dropped` = events discarded before `events[0]` (Trigger.dev keeps a rolling window), so
    keys of rows without an id of their own stay the same as the window moves."""
    rows: list[dict[str, Any]] = []
    messages: dict[str, dict] = {}
    tools: dict[str, dict] = {}
    requests: dict[str, dict] = {}
    commands = None
    acp_state = None
    for index, event in enumerate(events):
        kind = event.get("type")
        if kind == "commands":
            commands = event
        elif kind == "acp_state":
            acp_state = event
        elif kind == "agent" and event.get("message_id"):
            row = messages.get(event["message_id"])
            if row is not None:
                row["text"] += event.get("text") or ""
            else:
                row = {**event, "text": event.get("text") or "", "key": f"m:{event['message_id']}"}
                rows.append(row)
                messages[event["message_id"]] = row
        elif kind == "tool":
            row = tools.get(event.get("id"))
            if row is not None:
                # Agents send a tool's name once; later updates may say only "tool".
                for field, value in event.items():
                    generic = field == "name" and (not value or value == "tool") and row.get("name")
                    if value is not None and value != "" and not generic:
                        row[field] = value
            else:
                row = {**event, "key": f"t:{event.get('id')}"}
                rows.append(row)
                tools[event.get("id")] = row
        elif kind in ("approval", "question"):
            row = {**event, "key": f"{kind[0]}:{event.get('id')}"}
            rows.append(row)
            requests[f"{kind}:{event.get('id')}"] = row
        elif kind in ("approval_resolved", "answer"):
            row = requests.get(f"{'question' if kind == 'answer' else 'approval'}:{event.get('id')}")
            if row is not None:
                row["resolved"] = event
        elif kind in SHOWN:
            rows.append({**event, "key": f"e:{dropped + index}"})
    # Keep the latest session metadata in the newest page, even in long chats.
    if commands is not None:
        rows.insert(0, {**commands, "key": "commands"})
    if acp_state is not None:
        rows.insert(0, {**acp_state, "key": "acp_state"})
    return rows


def session_state(events: list[dict[str, Any]]) -> str | None:
    return next((e.get("state") for e in reversed(events) if e.get("type") == "status"), None)


class Session:
    """What the board needs from a session's events: its rows and its latest state."""

    def __init__(self, events: list[dict[str, Any]], dropped: int = 0) -> None:
        self.rows = coalesce(events, dropped)
        self.state = session_state(events)
        self.truncated = dropped > 0
        self.version = digest([dropped, self.rows, self.state])


def last_text(rows: list[dict[str, Any]]) -> str | None:
    for row in reversed(rows):
        if row.get("type") in ("agent", "user", "error"):
            text = row.get("text") or row.get("message")
            if text:
                return str(text)[:200]
    return None


def digest(value: Any) -> str:
    return hashlib.blake2b(json.dumps(value, sort_keys=True, default=str).encode(), digest_size=8).hexdigest()


def page(rows: list[dict[str, Any]], before: str | None, limit: int) -> tuple[list[dict[str, Any]], bool]:
    """The `limit` rows just before the row keyed `before` (the newest rows when it is None)."""
    metadata = []
    while rows and rows[0].get('type') in ('commands', 'acp_state'):
        metadata.append(rows[0])
        rows = rows[1:]
    end = len(rows)
    if before is not None:
        end = next((i for i, row in enumerate(rows) if row["key"] == before), 0)
    start = max(0, end - limit)
    return (metadata if before is None else []) + rows[start:end], start > 0


class Memo:
    """A value per run (e.g. its coalesced rows), rebuilt only when `version` changes."""

    def __init__(self, size: int = 400) -> None:
        self.size = size
        self.items: OrderedDict[str, tuple[Any, Any]] = OrderedDict()

    def get(self, key: str, version: Any, build: Callable[[], Any]) -> Any:
        cached = self.items.get(key)
        if cached is None or cached[0] != version:
            cached = self.items[key] = (version, build())
            while len(self.items) > self.size:
                self.items.popitem(last=False)
        self.items.move_to_end(key)
        return cached[1]


def settled(finished_at: str | None, seconds: float = 15) -> bool:
    """A run that finished a while ago: late log batches have landed, so its log can be frozen."""
    if not finished_at:
        return False
    try:
        finished = datetime.fromisoformat(finished_at.replace("Z", "+00:00"))
    except ValueError:
        return False
    if finished.tzinfo is None:
        finished = finished.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - finished).total_seconds() > seconds


def parse_evt_lines(lines: list[str]) -> list[dict[str, Any]]:
    events = []
    for line in lines:
        if line.startswith("EVT "):
            try:
                event = json.loads(line[4:])
            except (TypeError, ValueError):
                continue
            if isinstance(event, dict):
                events.append(event)
    return events
