"""Prefect adapter: boards are flow run tags (`board:<name>`), cards are flow runs.

Talks to the Prefect REST API (env: PREFECT_API_URL, e.g. http://host.docker.internal:4200/api).
- Intermediate results are the flow run's logs; agent sessions log `EVT <json>` transcript events.
- The DAG is the run's task runs, with edges from `task_inputs`: Prefect records which task run's
  result each task received, so the graph shows real data dependencies as they happened.
- Steering and agent-session input use flow run inputs, the same channel as Prefect's
  `send_input` / `receive_input` (keys `<model name>-auto-<uuid>`, JSON values).
"""

import asyncio
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import aiohttp

from cards import Card, Dag, DagNode, is_final
from logtail import TailCache
from transcript import Memo, Session, parse_evt_lines, settled

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
        self._flow_names: dict[str, asyncio.Future] = {}
        self._deployment_ids: dict[str, str] = {}
        self.tails = TailCache()
        self.sessions = Memo()
        self._settled: dict[str, bool] = {}  # flow run id (or "task:" + task run id) -> finished a while ago
        self._run_boards: dict[str, str] = {}  # flow run id -> board, for routing live logs

    async def _request(self, method: str, path: str, body: Any = None) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        async with self._session.request(method, self.api + path, json=body) as response:
            data = await response.json(content_type=None) if response.status != 204 else None
            if response.status >= 400:
                raise RuntimeError(f"Prefect {method} {path} -> {response.status}: {data}")
            return data

    async def _flow_name(self, flow_id: str) -> str:
        # Cache the request, not just its result: a board's cards are built concurrently.
        if flow_id not in self._flow_names:
            self._flow_names[flow_id] = asyncio.ensure_future(self._request("GET", f"/flows/{flow_id}"))
        try:
            return (await self._flow_names[flow_id])["name"]
        except Exception:
            self._flow_names.pop(flow_id, None)
            raise

    async def _log_rows(self, logs_filter: dict[str, Any], since: str | None) -> list[dict[str, Any]]:
        if since:
            # Overlap the previous read: a log batch can land with timestamps just before the
            # newest line already read. Repeats are dropped by log id.
            start = datetime.fromisoformat(since.replace("Z", "+00:00")) - timedelta(seconds=5)
            logs_filter = {**logs_filter, "timestamp": {"after_": start.isoformat()}}
        rows, offset = [], 0
        while True:
            page = await self._request("POST", "/logs/filter",
                                       {"logs": logs_filter, "sort": "TIMESTAMP_ASC", "limit": 200, "offset": offset})
            rows += page
            if len(page) < 200:
                return rows
            offset += len(page)

    async def _lines(self, key: str, logs_filter: dict[str, Any], final: bool) -> list[str]:
        async def fetch(since: str | None) -> list[tuple[Any, Any, str]]:
            return [(row["timestamp"], row["id"], row["message"]) for row in await self._log_rows(logs_filter, since)]
        return await self.tails.read(key, fetch, final)

    async def run_lines(self, run_id: str) -> list[str]:
        if run_id not in self._settled:
            run = await self._request("GET", f"/flow_runs/{run_id}")
            self._settled[run_id] = is_final(_column(run)) and settled(run.get("end_time"))
        return await self._lines(run_id, {"flow_run_id": {"any_": [run_id]}}, self._settled[run_id])

    async def node_lines(self, ref: str) -> list[str]:
        # A settled step's log (as last seen by dag()) is read once more, then served from the cache.
        return await self._lines(f"task:{ref}", {"task_run_id": {"any_": [ref]}}, self._settled.get(f"task:{ref}", False))

    async def run_info(self, run_id: str) -> dict:
        run = await self._request("GET", f"/flow_runs/{run_id}")
        return {"task": await self._flow_name(run["flow_id"]), "params": run.get("parameters") or {},
                "board": next((t[6:] for t in run.get("tags", []) if t.startswith("board:")), "research"),
                "title": run["name"], "column": _column(run)}

    async def events(self, run_id: str) -> tuple[int, list[dict[str, Any]]]:
        return 0, parse_evt_lines(await self.run_lines(run_id))

    async def session(self, run_id: str) -> Session:
        lines = await self.run_lines(run_id)
        return self.sessions.get(run_id, (id(lines), len(lines)), lambda: Session(parse_evt_lines(lines)))

    async def list_cards(self, board: str, light: bool = False) -> list[Card]:
        """light: skip reading logs (no last line or session state yet) for a fast first paint."""
        runs = await self._request("POST", "/flow_runs/filter",
                                   {"flow_runs": {"tags": {"all_": [f"board:{board}"]}}, "sort": "EXPECTED_START_TIME_DESC", "limit": 200})
        waiting = await self._request("POST", "/flow_runs/filter",
                                      {"flow_runs": {"state": {"type": {"any_": ["SCHEDULED", "PENDING"]}}}, "sort": "EXPECTED_START_TIME_ASC", "limit": 200})
        ahead_by_deployment: dict[str, list[str]] = {}
        for run in waiting:
            ahead_by_deployment.setdefault(run.get("deployment_id") or "", []).append(run["id"])
        limit = asyncio.Semaphore(8)

        async def card(run: dict[str, Any]) -> Card:
            async with limit:
                return await self._card(run, board, ahead_by_deployment, light)
        cards = await asyncio.gather(*map(card, runs))
        return sorted(cards, key=lambda c: c.created_at or "")

    async def _card(self, run: dict[str, Any], board: str, ahead_by_deployment: dict[str, list[str]],
                    light: bool = False) -> Card:
        params = run.get("parameters") or {}
        task = await self._flow_name(run["flow_id"])
        column = _column(run)
        state = run.get("state") or {}
        card = Card(
            id=run["id"],
            system=self.system,
            board=board,
            title=str(params.get("goal") or params.get("message") or params.get("action")
                      or (run["name"] if "portable-flow" in (run.get("tags") or []) else None)
                      or params.get("topic") or run["name"]),
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
        self._settled[run["id"]] = is_final(column) and settled(card.finished_at)
        self._run_boards[run["id"]] = board
        if light:
            pass
        elif task == "agent-session":
            card.apply_session(await self.session(run["id"]))
        else:
            card.apply_lines(await self.run_lines(run["id"]))
        return card

    async def feed(self, board: str, wake: Callable[[], None]) -> None:
        """Prefect's websockets: flow run state events (tagged with the board) and new log lines
        (routed by flow run). Log streaming needs PREFECT_SERVER_LOGS_STREAM_OUT_ENABLED on the server."""
        async with asyncio.TaskGroup() as group:
            group.create_task(self._listen("/events/out", {"event": {"prefix": ["prefect.flow-run."]}}, board, wake))
            group.create_task(self._listen("/logs/out", {}, board, wake))

    async def _listen(self, path: str, extra: dict[str, Any], board: str, wake: Callable[[], None]) -> None:
        if self._session is None:
            self._session = aiohttp.ClientSession()
        now = datetime.now(timezone.utc)
        window = (now.isoformat(), (now + timedelta(days=365)).isoformat())
        flt = ({"occurred": {"since": window[0], "until": window[1]}, **extra} if path == "/events/out"
               else {"timestamp": {"after_": window[0], "before_": window[1]}})
        tag = f"prefect.tag.board:{board}"
        url = "ws" + self.api[len("http"):] + path
        async with self._session.ws_connect(url, protocols=("prefect",), heartbeat=30) as ws:
            await ws.send_json({"type": "auth", "token": None})
            reply = await ws.receive_json(timeout=10)
            if reply.get("type") != "auth_success":
                raise RuntimeError(f"Prefect {path} refused the connection: {reply}")
            await ws.send_json({"type": "filter", "filter": flt})
            async for message in ws:
                if message.type != aiohttp.WSMsgType.TEXT:
                    break
                data = json.loads(message.data)
                if data.get("type") == "event":
                    related = (data.get("event") or {}).get("related") or []
                    if any(r.get("prefect.resource.id") == tag for r in related):
                        wake()
                elif data.get("type") == "log" or "log" in data:
                    if self._run_boards.get((data.get("log") or {}).get("flow_run_id")) == board:
                        wake()
        raise ConnectionError(f"Prefect {path} closed")

    async def active_runs(self, flows: set[str]) -> list[dict[str, Any]]:
        """Unfinished runs of the served portable-flow deployments on any board: restarting the
        flow worker (activating a flow) interrupts them or changes what they will run."""
        states = ["SCHEDULED", "PENDING", "RUNNING", "PAUSED", "CANCELLING"]
        runs, offset = [], 0
        while True:
            page = await self._request("POST", "/flow_runs/filter", {
                "flow_runs": {"tags": {"all_": ["portable-flow"]}, "state": {"type": {"any_": states}}},
                "sort": "EXPECTED_START_TIME_ASC", "limit": 200, "offset": offset})
            runs += page
            if len(page) < 200:
                break
            offset += len(page)
        return [{"system": self.system, "board": next((t[6:] for t in run.get("tags", []) if t.startswith("board:")), None),
                 "id": run["id"], "title": run["name"], "status": run.get("state_name") or run.get("state_type") or ""}
                for run in runs]

    async def dag(self, run_id: str) -> Dag:
        tasks = await self._request("POST", "/task_runs/filter",
                                    {"flow_runs": {"id": {"any_": [run_id]}}, "sort": "EXPECTED_START_TIME_ASC", "limit": 200})
        ids = {t["id"] for t in tasks}
        dag = Dag(model="dataflow")
        for task in tasks:
            self._settled[f"task:{task['id']}"] = is_final(_column(task)) and settled(task.get("end_time"))
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

    async def create_flow(self, board: str, flow: str, params: dict[str, Any], title: str) -> str:
        # A portable flow is served as deployment <flow>/flows (agents/flow_worker_prefect.py).
        name = f"{flow}/flows"
        if name not in self._deployment_ids:
            self._deployment_ids[name] = (await self._request("GET", f"/deployments/name/{flow}/flows"))["id"]
        run = await self._request("POST", f"/deployments/{self._deployment_ids[name]}/create_flow_run",
                                  {"parameters": params, "tags": [f"board:{board}"], "name": title[:100]})
        return run["id"]

    async def node_input(self, run_id: str, ref: str, message: dict[str, Any]) -> None:
        # A flow's agent step reads flow run inputs keyed with its task run id (flow_worker_prefect.py).
        await self._send_input(run_id, f"agentinput-{ref}", message)

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
