"""A synthetic backend for load-testing the board UI: BOARD_LOADTEST_CARDS=10000 adds a "loadtest" tab.

Each board gets that many generated cards spread over every column, with run input, output and
execution workers, so filtering has realistic data to search. Every couple of seconds the feed moves
a few cards along (queued -> running -> done/stopped) and adds a line to running ones, which
exercises the same live-update path as a real engine. Nothing here reaches an engine.
"""

import asyncio
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from cards import Card, Dag, DagNode

TASKS = ["research-report", "steerable-agent", "summarise-docs", "triage-ticket", "slug-relay", "assess-risk"]
AGENTS = ["claude", "codex", "opencode", "pi", "omp", None]
TOPICS = ["vector databases", "GPU scheduling", "rust async runtimes", "kanban ergonomics", "sbom tooling",
          "offline package mirrors", "otel sampling", "postgres partitioning", "llm eval harnesses",
          "durable execution", "edge caching", "zero-downtime deploys", "flaky test triage", "cost dashboards"]
VERBS = ["Compare", "Summarise", "Investigate", "Benchmark", "Audit", "Draft a plan for", "Review"]
POOLS = {"gpu": ["gpu-01", "gpu-02", "gpu-03"], "lab-a": ["lab-a-01", "lab-a-02"], "default": ["pc-a", "pc-b", "pc-c", "pc-d"]}
TAGS = ["cuda", "windows", "linux", "docker", "playwright", "large-disk", "office"]
COLUMN_WEIGHTS = {"queued": 20, "scheduled": 5, "running": 10, "waiting": 5, "done": 45, "stopped": 15}
STATUS = {"queued": "QUEUED", "scheduled": "SCHEDULED", "running": "RUNNING", "waiting": "WAITING",
          "done": "COMPLETED", "stopped": "FAILED"}


def _iso(at: datetime | None) -> str | None:
    return at.isoformat() if at else None


class LoadTestAdapter:
    system = "loadtest"

    def __init__(self, count: int | None = None, seed: int = 7) -> None:
        self.count = count if count is not None else int(os.environ.get("BOARD_LOADTEST_CARDS", "10000"))
        self.seed = seed
        self.boards: dict[str, list[Card]] = {}
        self.lines: dict[str, list[str]] = {}
        self.tick = float(os.environ.get("BOARD_LOADTEST_TICK", "2"))

    def _make(self, board: str) -> list[Card]:
        rng = random.Random(f"{self.seed}/{board}")
        now = datetime.now(timezone.utc)
        columns = list(COLUMN_WEIGHTS)
        cards = []
        for i in range(self.count):
            column = rng.choices(columns, weights=list(COLUMN_WEIGHTS.values()))[0]
            task, agent, topic = rng.choice(TASKS), rng.choice(AGENTS), rng.choice(TOPICS)
            created = now - timedelta(seconds=rng.randint(60, 7 * 24 * 3600))
            started = created + timedelta(seconds=rng.randint(1, 600)) if column not in ("queued", "scheduled") else None
            finished = started + timedelta(seconds=rng.randint(5, 3600)) if column in ("done", "stopped") else None
            pool = rng.choice(list(POOLS))
            card = Card(
                id=f"lt-{board}-{i:05d}", system=self.system, board=board,
                title=f"{rng.choice(VERBS)} {topic} #{i}", task=task, column=column, status=STATUS[column],
                created_at=_iso(created), started_at=_iso(started), finished_at=_iso(finished),
                agent=agent, steerable=False, priority=float(rng.randint(1, 3)),
                input={"goal": f"{topic} for team {rng.choice(['infra', 'ml', 'web', 'data'])}", "depth": rng.randint(1, 5),
                       "sources": rng.sample(["arxiv", "github", "confluence", "jira", "web"], 2),
                       **({"agent": agent} if agent else {})},
                output={"summary": f"{topic}: {rng.choice(['3 options', 'recommend B', 'needs follow-up', 'no blockers'])}",
                        "score": round(rng.random(), 3)} if column == "done" else None,
                error=rng.choice(["Timed out after 3600 s", "Worker lost", "exit code 2", "Rate limited by provider"]) if column == "stopped" else None,
            )
            if started:
                worker = rng.choice(POOLS[pool])
                card.workers = [{"step": "main", "id": worker, "hostname": f"{worker}.lan", "pool": pool,
                                 "tags": rng.sample(TAGS, 2)}]
            card.order = created.timestamp() - card.priority * 1e7
            card.queue = {"name": task}
            self.lines[card.id] = [f"step 1: looking into {topic}"]
            card.apply_lines(self.lines[card.id])
            cards.append(card)
        return cards

    async def list_cards(self, board: str, light: bool = False) -> list[Card]:
        if board not in self.boards:
            self.boards[board] = self._make(board)
        # Fresh objects each time, as a real adapter returns: the server labels and annotates them.
        return [Card(**{**vars(c), "queue": dict(c.queue), "moves": {}}) for c in self.boards[board]]

    async def feed(self, board: str, wake: Callable[[], None]) -> None:
        rng = random.Random()
        while True:
            await asyncio.sleep(self.tick)
            cards = self.boards.get(board)
            if not cards:
                continue
            now = _iso(datetime.now(timezone.utc))
            for card in rng.sample(cards, min(20, len(cards))):
                if card.column == "queued":
                    card.column, card.status, card.started_at = "running", "RUNNING", now
                elif card.column == "running":
                    if rng.random() < 0.5:
                        done = rng.random() < 0.8
                        card.column = "done" if done else "stopped"
                        card.status, card.finished_at = STATUS[card.column], now
                        if done:
                            card.output = {"summary": "finished under load", "score": round(rng.random(), 3)}
                        else:
                            card.error = "Cancelled by the load test"
                    else:
                        lines = self.lines[card.id]
                        lines.append(f"step {len(lines) + 1}: still working")
                        card.apply_lines(lines)
            wake()

    async def run_lines(self, run_id: str) -> list[str]:
        return list(self.lines.get(run_id, []))

    async def dag(self, run_id: str) -> Dag:
        return Dag(model="declared", nodes=[DagNode(id=run_id, label="main", column="done", status="COMPLETED")])
