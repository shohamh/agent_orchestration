"""Hatchet adapter: boards are `additional_metadata.board`, cards are workflow runs.

Uses the hatchet_sdk client (env: HATCHET_CLIENT_TOKEN, HATCHET_CLIENT_HOST_PORT,
HATCHET_CLIENT_SERVER_URL, HATCHET_CLIENT_TLS_STRATEGY). Intermediate results come from the
run's persisted logs (ctx.log in the agent), so history survives board restarts.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, NamedTuple

from cards import Card, Dag, DagNode, is_final

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
        # A finished run's logs don't change (until it is replayed, which changes finished_at).
        self._final_progress: dict[tuple[str, str | None], list[str]] = {}

    async def list_cards(self, board: str) -> list[Card]:
        since = datetime.now(timezone.utc) - timedelta(days=7)
        summaries = await asyncio.to_thread(
            lambda: self.hatchet.runs.list(
                since=since, limit=200, additional_metadata={"board": board}, include_payloads=True
            ).rows
            or []
        )
        queued = await self._queued_ahead()
        cards = await asyncio.gather(*(self._card(s, board, queued) for s in summaries))
        return sorted(cards, key=lambda c: c.created_at or "")

    async def _card(self, s: Any, board: str, queued: dict[str, list[str]]) -> Card:
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
            title=str(run_input.get("goal") or run_input.get("topic") or s.display_name),
            task=task,
            column=column,
            status=s.status.value,
            created_at=_iso(s.created_at),
            started_at=_iso(s.started_at),
            finished_at=_iso(s.finished_at),
            output=s.output or None,
            error=s.error_message,
            steerable=task == AGENT_TASK and not is_final(column),
        )
        if column == "queued":
            ahead = queued.get(task, [])
            card.queue = {
                "name": task,
                "ahead": ahead.index(run_id) if run_id in ahead else None,
                "queued": len(ahead),
            }
        else:
            # A DAG workflow run has no logs of its own: its progress is its tasks' statuses.
            is_dag = s.type == V1WorkflowType.DAG
            fetch = self._dag_progress(run_id) if is_dag else self._progress(s.task_external_id)
            if is_final(column):
                key = (run_id, card.finished_at)
                if key not in self._final_progress:
                    self._final_progress[key] = await fetch
                else:
                    fetch.close()
                card.progress = self._final_progress[key]
            else:
                card.progress = await fetch
        return card

    async def _dag_progress(self, run_id: str) -> list[str]:
        dag = (await self.dag(run_id)).topologically_sorted()
        return [f"{node.label}: {node.status.lower()}" for node in dag.nodes]

    async def _progress(self, task_run_id: str) -> list[str]:
        try:
            logs = await asyncio.to_thread(lambda: self.hatchet.logs.list(task_run_id=task_run_id).rows or [])
        except NotFoundException:
            return []
        lines = sorted(logs, key=lambda line: line.created_at)
        # Durable tasks can replay after eviction and re-log a step; keep the first occurrence.
        return list(dict.fromkeys(line.message for line in lines))

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

    async def node_logs(self, ref: str) -> list[str]:
        return await self._progress(ref)

    async def create(self, board: str, kind: str, goal: str, turns: int, step_seconds: int) -> str:
        if kind == "research-report":
            workflow, run_input = "research-report", {"topic": goal}
        else:
            workflow, run_input = AGENT_TASK, {"goal": goal, "turns": turns, "step_seconds": step_seconds}
        details = await self.hatchet.runs.aio_create(workflow, run_input, additional_metadata={"board": board})
        return details.run.metadata.id

    async def steer(self, run_id: str, message: dict[str, Any]) -> None:
        await asyncio.to_thread(lambda: self.hatchet.event.push(STEER_EVENT, message, scope=run_id))

    async def cancel(self, run_id: str) -> None:
        await asyncio.to_thread(self.hatchet.runs.cancel, run_id)

    async def replay(self, run_id: str) -> None:
        await asyncio.to_thread(self.hatchet.runs.replay, run_id)

    async def set_queue_paused(self, queue: str, paused: bool) -> None:
        raise NotImplementedError("Hatchet has no queue pause; hold work with concurrency limits instead")
