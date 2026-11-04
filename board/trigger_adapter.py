"""Trigger.dev adapter: boards are run tags (`board:<name>`), cards are runs.

Talks to the REST API with an environment secret key (env: TRIGGER_API_URL, TRIGGER_SECRET_KEY).
Intermediate results are the run's live `metadata.transcript` (set by the agent), which the
runs list already returns, so one request per board refresh covers every card.
"""

import json
import asyncio
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable
from urllib.parse import quote

import aiohttp

from cards import Card, Dag, DagNode, is_final
from transcript import Memo, Session, digest
from backend_links import trigger_queue_url

MAX_DAG_NODES = 60
MAX_DAG_DEPTH = 4

AGENT_TASK = "steerable-agent"
STEER_STREAM = "steer"

_COLUMN = {
    "DELAYED": "scheduled",
    "PENDING_VERSION": "queued",
    "QUEUED": "queued",
    "DEQUEUED": "running",
    "EXECUTING": "running",
    "WAITING": "waiting",
    "COMPLETED": "done",
}  # everything else (CANCELED, FAILED, CRASHED, SYSTEM_FAILURE, TIMED_OUT, EXPIRED) -> stopped
ACTIVE_STATUSES = ("PENDING_VERSION", "DELAYED", "QUEUED", "DEQUEUED", "EXECUTING", "WAITING")


def _timestamp(text: str | None) -> float:
    try:
        return datetime.fromisoformat((text or "").replace("Z", "+00:00")).timestamp()
    except ValueError:
        return time.time()


def _sequence_edges(children: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Group siblings (sorted by creation) into waves of runs that overlapped; a new wave starts when
    every run of the current wave had finished before the next sibling was created. Every run in a
    wave then precedes every run in the next wave."""
    waves: list[list[dict[str, Any]]] = []
    for child in children:
        current = waves[-1] if waves else None
        if current and not all(c.get("finishedAt") and c["finishedAt"] <= child["createdAt"] for c in current):
            current.append(child)
        else:
            waves.append([child])
    return [
        {"from": before["id"], "to": after["id"], "kind": "then"}
        for earlier, later in zip(waves, waves[1:])
        for before in earlier
        for after in later
    ]


class TriggerAdapter:
    system = "trigger"

    def __init__(self, development: bool = False) -> None:
        self.development = development
        self.system = "trigger-dev" if development else "trigger"
        self.base_url = os.environ.get("TRIGGER_API_URL", "http://localhost:8030").rstrip("/")
        key = "TRIGGER_DEV_SECRET_KEY" if development else "TRIGGER_SECRET_KEY"
        self.headers = {"Authorization": f"Bearer {os.environ[key]}"}
        self._session: aiohttp.ClientSession | None = None
        self._outputs: dict[str, Any] = {}  # finished runs' output, fetched once
        self._finished_runs: dict[str, dict[str, Any]] = {}  # finished runs never change
        self._metadata: dict[str, dict[str, Any]] = {}  # run id -> metadata from the latest list
        self._runs_seen: dict[str, dict[str, Any]] = {}  # run id -> tags + metadata, for replays with priority
        self.sessions = Memo()

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self.headers)
        async with self._session.request(method, self.base_url + path, **kwargs) as response:
            body = await response.json(content_type=None)
            if response.status >= 400:
                raise RuntimeError(f"Trigger.dev {method} {path} -> {response.status}: {body}")
            return body

    async def list_cards(self, board: str, light: bool = False) -> list[Card]:
        """light: skip fetching finished runs' outputs (one request each) for a fast first paint."""
        runs = await self._list_runs({"filter[tag]": f"board:{board}", "filter[createdAt][period]": "7d"})
        queues = {q["name"]: q for q in (await self._request("GET", "/api/v1/queues?perPage=100"))["data"]}
        queued_by_task: dict[str, list[str]] = {}
        # Dequeue order: created time minus the priority offset (recorded in metadata by the board).
        queue_time = lambda r: _timestamp(r["createdAt"]) - float((r.get("metadata") or {}).get("priority") or 0)
        for run in sorted(await self._list_runs({"filter[status]": "QUEUED"}), key=queue_time):
            queued_by_task.setdefault(run["taskIdentifier"], []).append(run["id"])

        cards = []
        for run in runs:
            column = _COLUMN.get(run["status"], "stopped")
            metadata = run.get("metadata") or {}
            task = run["taskIdentifier"]
            card = Card(
                id=run["id"],
                system=self.system,
                board=board,
                title=metadata.get("title") or task,
                task=task,
                column=column,
                status=run["status"],
                created_at=run.get("createdAt"),
                started_at=run.get("startedAt"),
                finished_at=run.get("finishedAt"),
                steerable=task in (AGENT_TASK, "agent-session") and not is_final(column),
                agent=metadata.get("agent"),
            )
            self._metadata[run["id"]] = metadata
            # Trigger.dev dequeues by queue time = created time minus `priority` seconds. The board
            # records the priority it chose in the run's metadata; the runs list doesn't report it.
            card.priority = float(metadata.get("priority") or 0)
            card.order = _timestamp(run.get("createdAt")) - card.priority
            self._runs_seen[run["id"]] = {"tags": run.get("tags") or [], "metadata": metadata}
            if task == "agent-session":
                card.apply_session(self._session_view(run["id"], metadata))
            else:
                card.apply_lines(list(metadata.get("transcript") or []))
            queue = queues.get(task, {})
            card.queue = {
                "name": task,
                "running": queue.get("running"),
                "queued": queue.get("queued"),
                "limit": queue.get("concurrencyLimit"),
                "paused": queue.get("paused", False),
                "id": queue.get("id"),
                "backend_url": trigger_queue_url(self.system, queue.get('id')),
            }
            if column == "queued":
                ahead = queued_by_task.get(task, [])
                card.queue["ahead"] = ahead.index(run["id"]) if run["id"] in ahead else None
            if is_final(column) and (not light or run["id"] in self._outputs):
                card.output, card.error = await self._result(run["id"])
            cards.append(card)
        return sorted(cards, key=lambda c: c.created_at or "")

    async def active_runs(self, flows: set[str]) -> list[dict[str, Any]]:
        """Unfinished runs of any task on any board: the local dev worker reloads every task when
        the generated flow tasks change, so agent sessions are interrupted too."""
        if not self.development:
            return []  # portable flows run on the local development worker only
        params = {"filter[status]": ",".join(ACTIVE_STATUSES), "page[size]": "100"}
        runs: list[dict[str, Any]] = []
        for _ in range(20):
            page = await self._request("GET", "/api/v1/runs", params=params)
            runs += page.get("data") or []
            cursor = (page.get("pagination") or {}).get("next")
            if not cursor or cursor == params.get("page[after]"):
                break
            params["page[after]"] = cursor
        else:
            raise RuntimeError("too many active Trigger.dev runs to list")
        return [{"system": self.system, "board": next((t[6:] for t in run.get("tags", []) if t.startswith("board:")), None),
                 "id": run["id"], "title": (run.get("metadata") or {}).get("title") or run["taskIdentifier"], "status": run["status"]}
                for run in runs]

    async def _list_runs(self, filters: dict[str, str]) -> list[dict[str, Any]]:
        params = {**filters, "page[size]": "100"}
        return (await self._request("GET", "/api/v1/runs", params=params))["data"]

    async def _result(self, run_id: str) -> tuple[Any, str | None]:
        if run_id not in self._outputs:
            run = await self._request("GET", f"/api/v3/runs/{run_id}")
            error = (run.get("error") or {}).get("message")
            self._outputs[run_id] = (run.get("output"), error)
        return self._outputs[run_id]

    async def _retrieve(self, run_id: str) -> dict[str, Any]:
        if run_id in self._finished_runs:
            return self._finished_runs[run_id]
        run = await self._request("GET", f"/api/v3/runs/{run_id}")
        if is_final(_COLUMN.get(run["status"], "stopped")):
            self._finished_runs[run_id] = run
        return run

    async def dag(self, run_id: str) -> Dag:
        """Trigger.dev has no declared DAG: walk the parent -> child run tree, then infer ordering
        between siblings (a child created only after its earlier siblings finished ran after them)."""
        dag = Dag(model="calls")
        frontier, depth = [run_id], 0
        while frontier and depth <= MAX_DAG_DEPTH and len(dag.nodes) < MAX_DAG_NODES:
            next_frontier = []
            for parent_id in frontier:
                run = await self._retrieve(parent_id)
                dag.nodes.append(self._node(run))
                children = sorted((run.get("relatedRuns") or {}).get("children") or [], key=lambda c: c["createdAt"])
                for child in children:
                    dag.edges.append({"from": parent_id, "to": child["id"], "kind": "calls"})
                dag.edges += _sequence_edges(children)
                next_frontier += [c["id"] for c in children]
            frontier, depth = next_frontier[: MAX_DAG_NODES - len(dag.nodes)], depth + 1
        return dag

    @staticmethod
    def _node(run: dict[str, Any]) -> DagNode:
        column = _COLUMN.get(run["status"], "stopped")
        return DagNode(
            id=run["id"],
            label=run["taskIdentifier"],
            column=column,
            status=run["status"],
            ref=run["id"],
            started_at=run.get("startedAt"),
            finished_at=run.get("finishedAt"),
            output=run.get("output"),
            error=(run.get("error") or {}).get("message"),
        )

    async def node_lines(self, ref: str) -> list[str]:
        run = await self._retrieve(ref)
        events = (run.get("metadata") or {}).get("events")
        if events is not None:  # agent sessions and flow steps: same `EVT <json>` lines as the other backends
            return ["EVT " + json.dumps(event, ensure_ascii=False) for event in events]
        return list((run.get("metadata") or {}).get("transcript") or [])

    async def node_events(self, ref: str) -> tuple[int, list[dict]]:
        metadata = (await self._retrieve(ref)).get("metadata") or {}
        return int(metadata.get("eventsDropped") or 0), self._events_with_commands(metadata)

    async def run_info(self, run_id: str) -> dict:
        run = await self._retrieve(run_id)
        return {"task": run["taskIdentifier"], "params": run.get("payload") or {},
                "board": next((t[6:] for t in run.get("tags", []) if t.startswith("board:")), "research"),
                "title": (run.get("metadata") or {}).get("title") or run["taskIdentifier"],
                "column": _COLUMN.get(run["status"], "stopped")}

    async def _run_metadata(self, run_id: str) -> dict[str, Any]:
        if run_id not in self._metadata:
            self._metadata[run_id] = (await self._retrieve(run_id)).get("metadata") or {}
        return self._metadata[run_id]

    def _session_view(self, run_id: str, metadata: dict[str, Any]) -> Session:
        events, dropped = self._events_with_commands(metadata), int(metadata.get("eventsDropped") or 0)
        return self.sessions.get(run_id, digest([dropped, events]), lambda: Session(events, dropped))

    @staticmethod
    def _events_with_commands(metadata: dict[str, Any]) -> list[dict]:
        events = list(metadata.get("events") or [])
        if isinstance(metadata.get("commands"), list):
            events.append({"type": "commands", "commands": metadata["commands"]})
        if isinstance(metadata.get("acpState"), dict):
            events.append(metadata["acpState"])
        return events

    async def run_lines(self, run_id: str) -> list[str]:
        return list((await self._run_metadata(run_id)).get("transcript") or [])

    async def events(self, run_id: str) -> tuple[int, list[dict[str, Any]]]:
        metadata = await self._run_metadata(run_id)
        return int(metadata.get("eventsDropped") or 0), self._events_with_commands(metadata)

    async def session(self, run_id: str) -> Session:
        return self._session_view(run_id, await self._run_metadata(run_id))

    async def feed(self, board: str, wake: Callable[[], None]) -> None:
        """Trigger.dev Realtime (an Electric shape of this board's runs): a long poll that returns
        as soon as a run's status or metadata (the session transcript) changes."""
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self.headers)
        # Without this header the webapp speaks an older protocol that keeps answering must-refetch.
        headers = {"x-trigger-electric-version": "1.0.0-beta.1"}
        url = f"{self.base_url}/realtime/v1/runs"
        params: dict[str, str] = {"tags": f"board:{board}", "offset": "-1"}
        while True:
            timeout = aiohttp.ClientTimeout(total=90)
            async with self._session.get(url, params=params, headers=headers, timeout=timeout) as response:
                if response.status == 409:  # the shape was rotated: start again from a snapshot
                    params = {"tags": f"board:{board}", "offset": "-1"}
                    continue
                if response.status >= 400:
                    raise RuntimeError(f"Trigger.dev realtime -> {response.status}: {await response.text()}")
                messages = await response.json(content_type=None) or []
                live = params.get("live") == "true"
                params = {"tags": f"board:{board}", "live": "true",
                          "handle": response.headers.get("electric-handle", params.get("handle", "")),
                          "offset": response.headers.get("electric-offset", params.get("offset", "-1"))}
                if response.headers.get("electric-cursor"):
                    params["cursor"] = response.headers["electric-cursor"]
            if live and any("value" in m or (m.get("headers") or {}).get("operation") for m in messages):
                wake()

    async def create(self, board: str, kind: str, goal: str, turns: int, step_seconds: int,
                     session_options: dict[str, Any] | None = None) -> str:
        if kind == "agent-session":
            if not self.development:
                raise ValueError("agent-session requires the Trigger.dev development backend")
            task, payload = "agent-session", {"goal": goal, "agent": "codex", **(session_options or {})}
        elif kind == "research-report":
            task, payload = "research-report", {"topic": goal}
        else:
            task, payload = AGENT_TASK, {"goal": goal, "turns": turns, "stepSeconds": step_seconds}
        initial_metadata = {"title": goal}
        if kind == "agent-session":
            initial_metadata = {"title": goal[:512], "agent": payload["agent"]}
        body = {"payload": payload, "options": {"tags": [f"board:{board}"], "metadata": initial_metadata}}
        return (await self._request("POST", f"/api/v1/tasks/{task}/trigger", json=body))["id"]

    async def create_flow(self, board: str, flow: str, params: dict[str, Any], title: str) -> str:
        # A portable flow is the generated parent task flow-<flow> (src/trigger/flows.generated.ts).
        if not self.development:
            raise ValueError("Portable flows run on the Trigger.dev local (development) worker; use that tab")
        body = {"payload": params, "options": {"tags": [f"board:{board}"], "metadata": {"title": title[:512]}}}
        return (await self._request("POST", f"/api/v1/tasks/flow-{flow}/trigger", json=body))["id"]

    async def node_input(self, run_id: str, ref: str, message: dict[str, Any]) -> None:
        # Each flow step is its own run with the agent-input stream.
        await self.input(ref, message)

    async def input(self, run_id: str, message: dict[str, Any]) -> None:
        await self._request("POST", f"/realtime/v1/streams/{quote(run_id, safe='')}/input/agent-input", json={"data": message})

    async def steer(self, run_id: str, message: dict[str, Any]) -> None:
        await self._request("POST", f"/realtime/v1/streams/{run_id}/input/{STEER_STREAM}", json={"data": message})

    async def place(self, card: Card, above: Card | None, below: Card | None) -> str:
        """Drag and drop into Queued (moves.py). Trigger.dev sets a run's priority only when it is
        triggered, so the run is cancelled (unless finished) and replayed with a priority offset that
        puts its queue time between its new neighbours. moves.py only allows this in the middle of
        the column for runs that haven't started; started runs go to the top or bottom."""
        if above is not None and below is not None:
            target = (above.order + below.order) / 2
        elif below is not None:
            target = below.order - 10
        elif above is not None:
            target = above.order + 1
        else:
            target = time.time()
        priority = max(0, round(time.time() - target + 1))
        seen = self._runs_seen.get(card.id) or {}
        if not is_final(card.column):
            await self.cancel(card.id)
        body = {"priority": priority, "tags": seen.get("tags") or [f"board:{card.board}"],
                "metadata": {**(seen.get("metadata") or {"title": card.title}), "priority": priority}}
        return (await self._request("POST", f"/api/v1/runs/{card.id}/replay", json=body))["id"]

    async def start_now(self, card: Card) -> None:
        # A delayed run: reschedule it to now.
        await self._request("POST", f"/api/v1/runs/{card.id}/reschedule",
                            json={"delay": datetime.now(timezone.utc).isoformat()})

    async def cancel(self, run_id: str) -> None:
        await self._request("POST", f"/api/v2/runs/{run_id}/cancel")

    async def replay(self, run_id: str) -> None:
        await self._request("POST", f"/api/v1/runs/{run_id}/replay")

    async def set_queue_paused(self, queue: str, paused: bool) -> None:
        await self._request(
            "POST", f"/api/v1/queues/{quote(queue, safe='')}/pause", json={"action": "pause" if paused else "resume"}
        )

    async def close(self) -> None:
        if self._session:
            await self._session.close()
