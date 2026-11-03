"""The common card model shared by both adapters."""

from dataclasses import asdict, dataclass, field
from typing import Any

from transcript import digest, last_text

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
    output: Any = None
    error: str | None = None
    queue: dict[str, Any] = field(default_factory=dict)  # {"name", "ahead", "running", "limit", "paused"}
    steerable: bool = False
    session_state: str | None = None
    agent: str | None = None
    last: str | None = None  # the latest message / log line, for the card face
    version: str | None = None  # changes whenever the card's transcript or log does
    flow: str | None = None  # the portable flow (flows/*.yaml) this run belongs to
    backend_url: str | None = None

    def apply_session(self, session: Any) -> None:
        """Project the session status without hiding an engine failure/cancellation."""
        self.session_state = session.state
        self.last = last_text(session.rows)
        self.version = session.version
        if self.column == "done":
            self.session_state = "done"
        elif self.column == "stopped":
            self.session_state = "error"
        elif self.column not in ("queued", "scheduled") and self.session_state:
            waiting = {"approval", "question", "awaiting_review", "awaiting_input", "stuck"}
            self.column = "waiting" if self.session_state in waiting else "running"

    def apply_lines(self, lines: list[str]) -> None:
        self.last = lines[-1][:200] if lines else None
        self.version = digest([len(lines), lines[-50:]])

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
    step: str | None = None  # portable flows: the step id in the flow definition
    kind: str | None = None  # portable flows: "agent" | "program"
    agent: str | None = None
    backend_url: str | None = None
    restart_supported: bool = False
    restart_reason: str | None = None
    restart_mode: str | None = None
    execution: dict | None = None


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
