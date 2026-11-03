"""Hatchet adapter: boards are `additional_metadata.board`, cards are workflow runs.

Uses the hatchet_sdk client (env: HATCHET_CLIENT_TOKEN, HATCHET_CLIENT_HOST_PORT,
HATCHET_CLIENT_SERVER_URL, HATCHET_CLIENT_TLS_STRATEGY). Intermediate results come from the
run's persisted logs (ctx.log in the agent), so history survives board restarts.
"""

import asyncio
import warnings
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

from cards import Card, Dag, DagNode, is_final
from logtail import TailCache
from transcript import Memo, Session, parse_evt_lines, settled

from hatchet_sdk import Hatchet
from hatchet_sdk.clients.rest.exceptions import NotFoundException
from hatchet_sdk.clients.rest.models.v1_task_status import V1TaskStatus
from hatchet_sdk.clients.rest.models.v1_workflow_type import V1WorkflowType

AGENT_TASK = "steerable-agent"
STEER_EVENT = "agent:steer"

_COLUMN = {
    V1TaskStatus.QUEUED: "queued",
    V1TaskStatus.RUNNING: "running",
    V1TaskStatus.COMPLETED: "done",
    V1TaskStatus.CANCELLED: "stopped",
    V1TaskStatus.FAILED: "stopped",
}


class _ShapeItem(NamedTuple):
    step_id: str
    task_name: str
    children_step_ids: list[str]


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


class HatchetAdapter:
    system = "hatchet"

    def __init__(self) -> None:
        self.hatchet = Hatchet()
        self.tails = TailCache(unique_messages=True)
        self.sessions = Memo()
        # A finished DAG's task statuses don't change (until it is replayed, which changes finished_at).
        self._final_dag: dict[tuple[str, str | None], list[str]] = {}
        self._runs: dict[str, tuple[str, bool, bool]] = {}  # run id -> (task run id, is DAG, settled)

    async def list_cards(self, board: str, light: bool = False) -> list[Card]:
        """light: skip reading logs (no last line or session state yet) for a fast first paint."""
        since = datetime.now(timezone.utc) - timedelta(days=7)
        summaries = await asyncio.to_thread(
            lambda: self.hatchet.runs.list(
                since=since, limit=200, additional_metadata={"board": board}, include_payloads=True
            ).rows
            or []
        )
        queued = await self._queued_ahead()
        cards = await asyncio.gather(*(self._card(s, board, queued, light) for s in summaries))
        return sorted(cards, key=lambda c: c.created_at or "")

    async def _card(self, s: Any, board: str, queued: dict[str, list[str]], light: bool = False) -> Card:
        run_id = s.workflow_run_external_id
        column = _COLUMN.get(s.status, "running")
        if column == "running" and s.is_evicted:
            column = "waiting"  # durable task parked in a wait, holding no worker slot
        task = s.workflow_name or s.display_name
        # Standalone tasks report {"input": {...}, "parents": ...}; DAG workflow runs the input itself.
        run_input = (s.input or {}).get("input", s.input or {})
        card = Card(
            id=run_id,
            system=self.system,
            board=board,
            title=str((s.additional_metadata or {}).get("title") or run_input.get("goal") or run_input.get("topic")
                      or s.display_name),
            task=task,
            column=column,
            status=s.status.value,
            created_at=_iso(s.created_at),
            started_at=_iso(s.started_at),
            finished_at=_iso(s.finished_at),
            output=s.output or None,
            error=s.error_message,
            steerable=task in (AGENT_TASK, "agent-session") and not is_final(column),
            agent=run_input.get("agent"),
        )
        if column == "queued":
            ahead = queued.get(task, [])
            card.queue = {
                "name": task,
                "ahead": ahead.index(run_id) if run_id in ahead else None,
                "queued": len(ahead),
            }
        else:
            is_dag = s.type == V1WorkflowType.DAG
            self._runs[run_id] = (s.task_external_id, is_dag, is_final(column) and settled(card.finished_at))
            if light:
                pass
            elif task == "agent-session":
                card.apply_session(await self.session(run_id))
            else:
                card.apply_lines(await self.run_lines(run_id))
        return card

    async def _run(self, run_id: str) -> tuple[str, bool, bool]:
        if run_id not in self._runs:
            details = await asyncio.to_thread(self.hatchet.runs.get, run_id)
            is_dag = len(details.tasks) > 1 or bool(details.shape and len(details.shape) > 1)
            final = is_final(_COLUMN.get(details.run.status, "running"))
            self._runs[run_id] = (details.tasks[0].task_external_id if details.tasks else run_id, is_dag,
                                  final and settled(_iso(details.run.finished_at)))
        return self._runs[run_id]

    async def _lines(self, task_run_id: str, final: bool) -> list[str]:
        async def fetch(since: datetime | None) -> list[tuple[Any, Any, str]]:
            # Overlap the previous read a little: a line can land just after a read with an
            # earlier timestamp. Repeats are dropped by their (time, message) key.
            rows = await self._log_rows(task_run_id, since - timedelta(seconds=2) if since else None)
            return [(row.created_at, (row.created_at, row.message), row.message) for row in rows]
        return await self.tails.read(task_run_id, fetch, final)

    async def run_lines(self, run_id: str) -> list[str]:
        """A run's log, or for a DAG workflow run (no logs of its own) its tasks' statuses."""
        task_run_id, is_dag, final = await self._run(run_id)
        if not is_dag:
            return await self._lines(task_run_id, final)
        key = (run_id, final)
        if final and key in self._final_dag:
            return self._final_dag[key]
        lines = await self._dag_progress(run_id)
        if final:
            self._final_dag[key] = lines
        return lines

    async def events(self, run_id: str) -> tuple[int, list[dict[str, Any]]]:
        return 0, parse_evt_lines(await self.run_lines(run_id))

    async def session(self, run_id: str) -> Session:
        lines = await self.run_lines(run_id)
        return self.sessions.get(run_id, (id(lines), len(lines)), lambda: Session(parse_evt_lines(lines)))

    async def feed(self, board: str, wake: Any) -> None:
        """Hatchet pushes run events (started, stream data, finished) for runs with this board's
        metadata. The agent worker streams a marker per transcript event, so new messages wake
        the board too."""
        with warnings.catch_warnings():
            # Deprecated in the SDK, but the engine serves it (verified on this setup).
            warnings.simplefilter("ignore", DeprecationWarning)
            listener = self.hatchet.listener.stream_by_additional_metadata("board", board)
        async for _ in listener:
            wake()

    async def _dag_progress(self, run_id: str) -> list[str]:
        dag = (await self.dag(run_id)).topologically_sorted()
        return [f"{node.label}: {node.status.lower()}" for node in dag.nodes]

    async def _progress(self, task_run_id: str) -> list[str]:
        # Durable tasks can replay after eviction and re-log a step; keep the first occurrence.
        return list(dict.fromkeys(line.message for line in await self._log_rows(task_run_id)))

    async def _log_rows(self, task_run_id: str, since: datetime | None = None) -> list[Any]:
        # The SDK has no offset/cursor and defaults to 1000 rows. Partition time
        # windows when a page fills; this works with either server sort order.
        windows = [(since, datetime.now(timezone.utc) + timedelta(minutes=1), 1000)]
        logs = {}
        try:
            while windows:
                since, until, limit = windows.pop()
                page = await asyncio.to_thread(
                    lambda: self.hatchet.logs.list(task_run_id=task_run_id, since=since, until=until, limit=limit).rows or []
                )
                for line in page:
                    logs[(line.created_at, line.message)] = line
                if len(page) < limit:
                    continue
                first = min(line.created_at for line in page)
                last = max(line.created_at for line in page)
                tick = timedelta(microseconds=1)
                if since is None or since < first:
                    windows.append((since, first - tick, 1000))
                if until > last:
                    windows.append((last + tick, until, 1000))
                if first < last:
                    middle = first + (last - first) // 2
                    windows.extend([(first, middle, 1000), (middle + tick, last, 1000)])
                elif limit < 1000000:
                    windows.append((first, last, limit * 2))
                else:
                    raise RuntimeError("Too many Hatchet logs sharing one timestamp; cannot safely paginate")
        except NotFoundException:
            return []
        return sorted(logs.values(), key=lambda line: line.created_at)

    async def _queued_ahead(self) -> dict[str, list[str]]:
        """Queued runs per task (any board), oldest first: a card's index = how many are ahead."""
        since = datetime.now(timezone.utc) - timedelta(days=7)
        rows = await asyncio.to_thread(
            lambda: self.hatchet.runs.list(
                since=since, limit=500, statuses=[V1TaskStatus.QUEUED], include_payloads=False
            ).rows
            or []
        )
        per_task: dict[str, list[str]] = {}
        for s in sorted(rows, key=lambda s: s.created_at):
            per_task.setdefault(s.workflow_name or s.display_name, []).append(s.workflow_run_external_id)
        return per_task

    async def dag(self, run_id: str) -> Dag:
        """The workflow's declared DAG (from the run's shape) with each task's live status."""
        details = await asyncio.to_thread(self.hatchet.runs.get, run_id)
        tasks = {t.step_id: t for t in details.tasks}
        dag = Dag(model="declared")
        # A standalone task run may have no shape: show its task(s) as single nodes.
        shape = details.shape or [_ShapeItem(t.step_id, t.display_name, []) for t in details.tasks]
        for item in shape:
            task = tasks.get(item.step_id)
            dag.nodes.append(
                DagNode(
                    id=item.step_id,
                    label=item.task_name,
                    column=_COLUMN.get(task.status, "running") if task else "pending",
                    status=task.status.value if task else "PENDING",
                    ref=task.task_external_id if task else None,
                    started_at=_iso(task.started_at) if task else None,
                    finished_at=_iso(task.finished_at) if task else None,
                    output=(task.output or None) if task else None,
                    error=(task.error_message or None) if task else None,
                )
            )
            dag.edges += [{"from": item.step_id, "to": child, "kind": "depends"} for child in item.children_step_ids]
        return dag

    async def node_lines(self, ref: str) -> list[str]:
        return await self._lines(ref, False)

    async def run_info(self, run_id: str) -> dict:
        details = await asyncio.to_thread(self.hatchet.runs.get, run_id)
        run = details.run
        workflow = await asyncio.to_thread(self.hatchet.workflows.get, run.workflow_id)
        meta = run.additional_metadata or {}
        return {"task": workflow.name, "params": run.input.get("input", run.input),
                "board": meta.get("board", "research"), "title": meta.get("title") or workflow.name,
                "column": _COLUMN.get(run.status, "stopped")}

    async def create(self, board: str, kind: str, goal: str, turns: int, step_seconds: int,
                     session_options: dict[str, Any] | None = None) -> str:
        if kind == "agent-session":
            workflow, run_input = "agent-session", {"goal": goal, **(session_options or {})}
        elif kind == "research-report":
            workflow, run_input = "research-report", {"topic": goal}
        else:
            workflow, run_input = AGENT_TASK, {"goal": goal, "turns": turns, "step_seconds": step_seconds}
        details = await self.hatchet.runs.aio_create(workflow, run_input, additional_metadata={"board": board})
        return details.run.metadata.id

    async def create_flow(self, board: str, flow: str, params: dict[str, Any], title: str) -> str:
        # A portable flow is a registered DAG workflow named after the flow (agents/flow_worker_hatchet.py).
        details = await self.hatchet.runs.aio_create(flow, params, additional_metadata={"board": board, "title": title})
        return details.run.metadata.id

    async def node_input(self, run_id: str, ref: str, message: dict[str, Any]) -> None:
        # A flow's agent step listens for input scoped to its own task run.
        await asyncio.to_thread(lambda: self.hatchet.event.push("agent:input", message, scope=ref))

    async def restart_node(self, run_id: str, ref: str) -> None:
        # Pass exactly the selected task to Hatchet. Its ListTasksForReplay recursively walks
        # the engine's persisted _StepOrder edges and replays the dependent subtree.
        await self.hatchet.runs.aio_replay(ref)
        self._runs.pop(run_id, None)
        self._final_dag = {k: v for k, v in self._final_dag.items() if k[0] != run_id}
        # Replayed nodes reuse task ids. Forget cached history; the engine owns which tasks ran.
        self.tails = TailCache(unique_messages=True)
        self.sessions = Memo()

    async def input(self, run_id: str, message: dict[str, Any]) -> None:
        await asyncio.to_thread(lambda: self.hatchet.event.push("agent:input", message, scope=run_id))

    async def steer(self, run_id: str, message: dict[str, Any]) -> None:
        await asyncio.to_thread(lambda: self.hatchet.event.push(STEER_EVENT, message, scope=run_id))

    async def cancel(self, run_id: str) -> None:
        await asyncio.to_thread(self.hatchet.runs.cancel, run_id)

    async def replay(self, run_id: str) -> None:
        await asyncio.to_thread(self.hatchet.runs.replay, run_id)

    async def set_queue_paused(self, queue: str, paused: bool) -> None:
        raise NotImplementedError("Hatchet has no queue pause; hold work with concurrency limits instead")
