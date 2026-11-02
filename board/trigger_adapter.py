"""Trigger.dev adapter: boards are run tags (`board:<name>`), cards are runs.

Talks to the REST API with an environment secret key (env: TRIGGER_API_URL, TRIGGER_SECRET_KEY).
Intermediate results are the run's live `metadata.transcript` (set by the agent), which the
runs list already returns, so one request per board refresh covers every card.
"""

import os
from typing import Any
from urllib.parse import quote

import aiohttp

from cards import Card, Dag, DagNode, is_final

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

    async def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self.headers)
        async with self._session.request(method, self.base_url + path, **kwargs) as response:
            body = await response.json(content_type=None)
            if response.status >= 400:
                raise RuntimeError(f"Trigger.dev {method} {path} -> {response.status}: {body}")
            return body

    async def list_cards(self, board: str) -> list[Card]:
        runs = await self._list_runs({"filter[tag]": f"board:{board}", "filter[createdAt][period]": "7d"})
        queues = {q["name"]: q for q in (await self._request("GET", "/api/v1/queues?perPage=100"))["data"]}
        queued_by_task: dict[str, list[str]] = {}
        for run in sorted(await self._list_runs({"filter[status]": "QUEUED"}), key=lambda r: r["createdAt"]):
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
                progress=list(metadata.get("transcript") or []),
                steerable=task in (AGENT_TASK, "agent-session") and not is_final(column),
                agent=metadata.get("agent"),
            )
            if task == "agent-session":
                card.apply_session_events(metadata.get("events") or [])
            queue = queues.get(task, {})
            card.queue = {
                "name": task,
                "running": queue.get("running"),
                "queued": queue.get("queued"),
                "limit": queue.get("concurrencyLimit"),
                "paused": queue.get("paused", False),
                "id": queue.get("id"),
            }
            if column == "queued":
                ahead = queued_by_task.get(task, [])
                card.queue["ahead"] = ahead.index(run["id"]) if run["id"] in ahead else None
            if is_final(column):
                card.output, card.error = await self._result(run["id"])
            cards.append(card)
        return sorted(cards, key=lambda c: c.created_at or "")

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

    async def node_logs(self, ref: str) -> list[str]:
        run = await self._retrieve(ref)
        if run.get("taskIdentifier") == "agent-session":
            return [str(event) for event in (run.get("metadata") or {}).get("events", [])]
        return list((run.get("metadata") or {}).get("transcript") or [])

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

    async def input(self, run_id: str, message: dict[str, Any]) -> None:
        await self._request("POST", f"/realtime/v1/streams/{quote(run_id, safe='')}/input/agent-input", json={"data": message})

    async def steer(self, run_id: str, message: dict[str, Any]) -> None:
        await self._request("POST", f"/realtime/v1/streams/{run_id}/input/{STEER_STREAM}", json={"data": message})

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
