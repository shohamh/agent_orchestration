"""Prefect adapter: boards are flow run tags (`board:<name>`), cards are flow runs.

Talks to the Prefect REST API (env: PREFECT_API_URL, e.g. http://host.docker.internal:4200/api).
- Intermediate results are the flow run's logs; agent sessions log `EVT <json>` transcript events.
- The DAG is the run's task runs, with edges from `task_inputs`: Prefect records which task run's
  result each task received, so the graph shows real data dependencies as they happened.
- Steering and agent-session input use flow run inputs, the same channel as Prefect's
  `send_input` / `receive_input` (keys `<model name>-auto-<uuid>`, JSON values).
"""

import json
import os
import uuid
from typing import Any

import aiohttp

from cards import Card, Dag, DagNode, is_final

# Deployments the board can start, served by the example worker / the native agent worker.
DEPLOYMENTS = {
    "steerable-agent": "steerable-agent/example",
    "research-report": "research-report/example",
    "agent-session": "agent-session/local",
}

_COLUMN = {
    "SCHEDULED": "scheduled",
    "PENDING": "queued",
    "RUNNING": "running",
    "CANCELLING": "running",
    "PAUSED": "waiting",
    "COMPLETED": "done",
}  # FAILED, CRASHED, CANCELLED -> stopped


def _column(run: dict[str, Any]) -> str:
    column = _COLUMN.get(run.get("state_type") or "", "stopped")
    # A scheduled run that is due ("Late") is effectively waiting in the queue.
    if column == "scheduled" and run.get("state_name") == "Late":
        return "queued"
    return column


class PrefectAdapter:
    system = "prefect"

    def __init__(self) -> None:
        self.api = os.environ.get("PREFECT_API_URL", "http://localhost:4200/api").rstrip("/")
        self._session: aiohttp.ClientSession | None = None
        self._flow_names: dict[str, str] = {}
        self._deployment_ids: dict[str, str] = {}
        self._final_logs: dict[tuple[str, str | None], list[str]] = {}

    async def _request(self, method: str, path: str, body: Any = None) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        async with self._session.request(method, self.api + path, json=body) as response:
            data = await response.json(content_type=None) if response.status != 204 else None
            if response.status >= 400:
                raise RuntimeError(f"Prefect {method} {path} -> {response.status}: {data}")
            return data

    async def _flow_name(self, flow_id: str) -> str:
        if flow_id not in self._flow_names:
            flow = await self._request("GET", f"/flows/{flow_id}")
            self._flow_names[flow_id] = flow["name"]
        return self._flow_names[flow_id]

    async def _logs(self, *, flow_run_id: str | None = None, task_run_id: str | None = None) -> list[str]:
        logs_filter = {"flow_run_id": {"any_": [flow_run_id]}} if flow_run_id else {"task_run_id": {"any_": [task_run_id]}}
        lines, offset = [], 0
        while True:
            page = await self._request("POST", "/logs/filter",
                                       {"logs": logs_filter, "sort": "TIMESTAMP_ASC", "limit": 200, "offset": offset})
            lines += [row["message"] for row in page]
            if len(page) < 200:
                return lines
            offset += len(page)

    async def list_cards(self, board: str) -> list[Card]:
        runs = await self._request("POST", "/flow_runs/filter",
                                   {"flow_runs": {"tags": {"all_": [f"board:{board}"]}}, "sort": "EXPECTED_START_TIME_DESC", "limit": 200})
        waiting = await self._request("POST", "/flow_runs/filter",
                                      {"flow_runs": {"state": {"type": {"any_": ["SCHEDULED", "PENDING"]}}}, "sort": "EXPECTED_START_TIME_ASC", "limit": 200})
        ahead_by_deployment: dict[str, list[str]] = {}
        for run in waiting:
            ahead_by_deployment.setdefault(run.get("deployment_id") or "", []).append(run["id"])
        cards = [await self._card(run, board, ahead_by_deployment) for run in runs]
        return sorted(cards, key=lambda c: c.created_at or "")

    async def _card(self, run: dict[str, Any], board: str, ahead_by_deployment: dict[str, list[str]]) -> Card:
        params = run.get("parameters") or {}
        task = await self._flow_name(run["flow_id"])
        column = _column(run)
        state = run.get("state") or {}
        card = Card(
            id=run["id"],
            system=self.system,
            board=board,
            title=str(params.get("goal") or params.get("topic") or params.get("message") or params.get("action") or run["name"]),
            task=task,
            column=column,
            status=run.get("state_name") or run.get("state_type") or "",
            created_at=run.get("created"),
            started_at=run.get("start_time"),
            finished_at=run.get("end_time"),
            error=state.get("message") if column == "stopped" else None,
            steerable=task in ("steerable-agent", "agent-session") and not is_final(column),
            agent=params.get("agent"),
        )
        if column in ("queued", "scheduled"):
            ahead = ahead_by_deployment.get(run.get("deployment_id") or "", [])
            card.queue = {"name": run.get("work_queue_name") or task, "queued": len(ahead),
                          "ahead": ahead.index(run["id"]) if run["id"] in ahead else None}
            return card
        if is_final(column):
            key = (run["id"], card.finished_at)
            if key not in self._final_logs:
                self._final_logs[key] = await self._logs(flow_run_id=run["id"])
            lines = self._final_logs[key]
        else:
            lines = await self._logs(flow_run_id=run["id"])
        if task == "agent-session":
            events = []
            for line in lines:
                if line.startswith("EVT "):
                    try:
                        event = json.loads(line[4:])
                    except ValueError:
                        continue
                    if isinstance(event, dict):
                        events.append(event)
            card.apply_session_events(events)
        else:
            card.progress = lines[-50:]
        return card

    async def dag(self, run_id: str) -> Dag:
        tasks = await self._request("POST", "/task_runs/filter",
                                    {"flow_runs": {"id": {"any_": [run_id]}}, "sort": "EXPECTED_START_TIME_ASC", "limit": 200})
        ids = {t["id"] for t in tasks}
        dag = Dag(model="dataflow")
        for task in tasks:
            dag.nodes.append(DagNode(
                id=task["id"],
                label=task["name"].rsplit("-", 1)[0],  # Prefect names task runs "<task>-<suffix>"
                column=_column(task),
                status=task.get("state_name") or task.get("state_type") or "",
                ref=task["id"],
                started_at=task.get("start_time"),
                finished_at=task.get("end_time"),
                error=(task.get("state") or {}).get("message") if _column(task) == "stopped" else None,
            ))
            upstream = {ref["id"] for refs in (task.get("task_inputs") or {}).values() for ref in refs
                        if ref.get("input_type") == "task_run" and ref.get("id") in ids}
            dag.edges += [{"from": parent, "to": task["id"], "kind": "depends"} for parent in sorted(upstream)]
        return dag

    async def node_logs(self, ref: str) -> list[str]:
        return await self._logs(task_run_id=ref)

    async def _deployment_id(self, kind: str) -> str:
        name = DEPLOYMENTS[kind]
        if name not in self._deployment_ids:
            flow, deployment = name.split("/")
            self._deployment_ids[name] = (await self._request("GET", f"/deployments/name/{flow}/{deployment}"))["id"]
        return self._deployment_ids[name]

    async def create(self, board: str, kind: str, goal: str, turns: int, step_seconds: int,
                     session_options: dict[str, Any] | None = None) -> str:
        if kind == "agent-session":
            parameters = {"goal": goal, **(session_options or {})}
        elif kind == "research-report":
            parameters = {"topic": goal}
        else:
            parameters = {"goal": goal, "turns": turns, "step_seconds": step_seconds}
        run = await self._request("POST", f"/deployments/{await self._deployment_id(kind)}/create_flow_run",
                                  {"parameters": parameters, "tags": [f"board:{board}"]})
        return run["id"]

    async def _send_input(self, run_id: str, model_name: str, value: dict[str, Any]) -> None:
        # Same key layout as prefect.input.send_input(), so receive_input(<Model>) sees it.
        await self._request("POST", f"/flow_runs/{run_id}/input",
                            {"key": f"{model_name}-auto-{uuid.uuid4()}", "value": json.dumps(value), "sender": "agent-board"})

    async def input(self, run_id: str, message: dict[str, Any]) -> None:
        await self._send_input(run_id, "agentinput", message)

    async def steer(self, run_id: str, message: dict[str, Any]) -> None:
        await self._send_input(run_id, "steermessage", message)

    async def cancel(self, run_id: str) -> None:
        await self._request("POST", f"/flow_runs/{run_id}/set_state", {"state": {"type": "CANCELLING"}, "force": True})

    async def replay(self, run_id: str) -> None:
        # Prefect has no in-place replay for a finished run: start a new run with the same inputs.
        run = await self._request("GET", f"/flow_runs/{run_id}")
        await self._request("POST", f"/deployments/{run['deployment_id']}/create_flow_run",
                            {"parameters": run.get("parameters") or {}, "tags": run.get("tags") or []})

    async def set_queue_paused(self, queue: str, paused: bool) -> None:
        raise NotImplementedError("Served Prefect deployments have no work queue to pause")

    async def close(self) -> None:
        if self._session:
            await self._session.close()
