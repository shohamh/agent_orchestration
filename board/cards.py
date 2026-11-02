"""The common card model shared by both adapters."""

from dataclasses import asdict, dataclass, field
from typing import Any

# Board columns, left to right.
COLUMNS = ["scheduled", "queued", "running", "waiting", "done", "stopped"]


@dataclass
class Card:
    id: str  # the run id in the underlying system
    system: str  # "hatchet" | "trigger"
    board: str
    title: str
    task: str
    column: str  # one of COLUMNS
    status: str  # raw status from the underlying system
    created_at: str | None = None
    started_at: str | None = None
    finished_at: str | None = None
    progress: list[str] = field(default_factory=list)  # intermediate results, oldest first
    output: Any = None
    error: str | None = None
    queue: dict[str, Any] = field(default_factory=dict)  # {"name", "ahead", "running", "limit", "paused"}
    steerable: bool = False
    events: list[dict[str, Any]] = field(default_factory=list)
    session_state: str | None = None
    agent: str | None = None

    def apply_session_events(self, events: list[dict[str, Any]]) -> None:
        """Project the session status without hiding an engine failure/cancellation."""
        self.events = [event for event in events if isinstance(event, dict)]
        for event in self.events:
            if event.get("type") == "status":
                self.session_state = event.get("state")
        self.progress = [str(e.get("text") or e.get("message") or e.get("state"))[:200]
                         for e in self.events if e.get("type") in ("agent", "error")][-8:]
        if self.column == "done":
            self.session_state = "done"
        elif self.column == "stopped":
            self.session_state = "error"
        elif self.column not in ("queued", "scheduled") and self.session_state:
            waiting = {"approval", "question", "awaiting_review", "awaiting_input", "stuck"}
            self.column = "waiting" if self.session_state in waiting else "running"

    def to_json(self) -> dict[str, Any]:
        return asdict(self)


def is_final(column: str) -> bool:
    return column in ("done", "stopped")


@dataclass
class DagNode:
    id: str
    label: str
    column: str  # same vocabulary as cards, plus "pending" (not created yet)
    status: str
    ref: str | None = None  # id to fetch this node's logs with (task run id / run id)
    started_at: str | None = None
    finished_at: str | None = None
    output: Any = None
    error: str | None = None


@dataclass
class Dag:
    # "declared": edges are dependencies the engine enforces (Hatchet workflow DAG).
    # "calls": edges are parent -> child runs created in code (Trigger.dev); "then" edges between
    # siblings are inferred from timing (a child created only after its sibling finished).
    model: str
    nodes: list[DagNode] = field(default_factory=list)
    edges: list[dict[str, str]] = field(default_factory=list)  # {"from", "to", "kind": depends|calls|then}

    def to_json(self) -> dict[str, Any]:
        return asdict(self)

    def topologically_sorted(self) -> "Dag":
        """Order nodes so every node comes after its predecessors (stable; ignores any cycle)."""
        preds: dict[str, set[str]] = {n.id: set() for n in self.nodes}
        for edge in self.edges:
            if edge["to"] in preds and edge["from"] in preds:
                preds[edge["to"]].add(edge["from"])
        ordered: list[DagNode] = []
        placed: set[str] = set()
        remaining = list(self.nodes)
        while remaining:
            ready = [n for n in remaining if preds[n.id] <= placed] or remaining[:1]
            for node in ready:
                ordered.append(node)
                placed.add(node.id)
            remaining = [n for n in remaining if n.id not in placed]
        self.nodes = ordered
        return self
