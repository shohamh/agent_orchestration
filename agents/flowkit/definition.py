"""Flow files: loading, validation and dependency order.

A flow is a YAML mapping (see flows/README.md):

    name: research-flow
    description: ...
    input: {topic: edge caching}
    steps:
      fetch_sources: {kind: program, run: "python {flow_dir}/steps/fetch_sources.py"}
      summarize: {kind: agent, needs: [fetch_sources], agent: demo, prompt: "..."}
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

FLOWS_DIR = Path(__file__).resolve().parents[2] / "flows"
AGENTS = ("demo", "codex", "claude", "opencode", "opencode2", "omp", "pi")
FLOW_NAME = re.compile(r"^[a-z][a-z0-9-]{0,47}$")
STEP_ID = re.compile(r"^[a-z][a-z0-9_]{0,47}$")
# Names the board and the example workers already use; a flow may not shadow them.
RESERVED = {"agent-session", "steerable-agent", "research-report", "agent-pipeline", "agent-loop",
            "uppercase", "read-sensor"}


class FlowError(ValueError):
    """The flow file is invalid; the message lists every problem found."""


@dataclass
class Step:
    id: str
    kind: str  # program | agent
    needs: list[str] = field(default_factory=list)
    run: str = ""  # program: shell command
    agent: str = "demo"  # agent
    prompt: str = ""  # agent
    review: bool = False  # agent: wait for "Mark done" before the flow continues
    retries: int = 0
    timeout_minutes: int = 60

    def to_json(self) -> dict[str, Any]:
        data = {"kind": self.kind, "needs": self.needs, "retries": self.retries, "timeout_minutes": self.timeout_minutes}
        if self.kind == "program":
            data["run"] = self.run
        else:
            data.update(agent=self.agent, prompt=self.prompt, review=self.review)
        return data


@dataclass
class Flow:
    name: str
    description: str
    input: dict[str, Any]
    steps: dict[str, Step]  # in dependency order

    def to_json(self) -> dict[str, Any]:
        return {"name": self.name, "description": self.description, "input": self.input,
                "steps": {sid: step.to_json() for sid, step in self.steps.items()}}

    def children(self, step_id: str) -> list[str]:
        return [s.id for s in self.steps.values() if step_id in s.needs]


def parse(data: Any) -> Flow:
    """Validate a decoded flow mapping; raises FlowError listing every problem."""
    errors: list[str] = []
    if not isinstance(data, dict):
        raise FlowError("A flow must be a mapping with name, input and steps")
    unknown = set(data) - {"name", "description", "input", "steps"}
    if unknown:
        errors.append(f"unknown top-level keys: {', '.join(sorted(unknown))}")
    name = data.get("name")
    if not isinstance(name, str) or not FLOW_NAME.match(name):
        errors.append("name must be lowercase letters, digits and '-', starting with a letter (max 48)")
    elif name in RESERVED or name.startswith("flow-"):
        errors.append(f"name {name!r} is reserved")
    description = data.get("description", "")
    if not isinstance(description, str):
        errors.append("description must be text")
    params = data.get("input") or {}
    if not isinstance(params, dict):
        errors.append("input must be a mapping of parameter names to default values")
        params = {}
    for key, value in params.items():
        if not isinstance(key, str) or not STEP_ID.match(key):
            errors.append(f"input name {key!r} must be lowercase letters, digits and '_'")
        if not (value is None or type(value) in (str, int, float, bool)):
            errors.append(f"input {key!r} must have a text, number or true/false default")
    raw_steps = data.get("steps")
    if not isinstance(raw_steps, dict) or not raw_steps:
        errors.append("steps must be a non-empty mapping of step ids to steps")
        raw_steps = {}
    steps: dict[str, Step] = {}
    for sid, raw in raw_steps.items():
        where = f"step {sid!r}"
        if not isinstance(sid, str) or not STEP_ID.match(sid):
            errors.append(f"{where}: id must be lowercase letters, digits and '_', starting with a letter")
            continue
        if not isinstance(raw, dict):
            errors.append(f"{where}: must be a mapping")
            continue
        kind = raw.get("kind")
        allowed = {"kind", "needs", "retries", "timeout_minutes", "description"} | (
            {"run"} if kind == "program" else {"agent", "prompt", "review"})
        extra = set(raw) - allowed
        if kind not in ("program", "agent"):
            errors.append(f"{where}: kind must be program or agent")
            continue
        if extra:
            errors.append(f"{where}: unknown keys for a {kind} step: {', '.join(sorted(extra))}")
        needs = raw.get("needs") or []
        if isinstance(needs, str):
            needs = [needs]
        if not isinstance(needs, list) or not all(isinstance(n, str) for n in needs):
            errors.append(f"{where}: needs must be a list of step ids")
            needs = []
        retries, timeout = raw.get("retries", 0), raw.get("timeout_minutes", 60)
        if type(retries) is not int or not 0 <= retries <= 10:
            errors.append(f"{where}: retries must be a whole number from 0 to 10")
        if type(timeout) is not int or not 1 <= timeout <= 24 * 60:
            errors.append(f"{where}: timeout_minutes must be from 1 to 1440")
        step = Step(sid, kind, list(dict.fromkeys(needs)), retries=retries if type(retries) is int else 0,
                    timeout_minutes=timeout if type(timeout) is int else 60)
        if kind == "program":
            step.run = raw.get("run")
            if not isinstance(step.run, str) or not step.run.strip():
                errors.append(f"{where}: a program step needs a run command")
        else:
            step.agent, step.prompt, step.review = raw.get("agent", "demo"), raw.get("prompt"), raw.get("review", False)
            if step.agent not in AGENTS:
                errors.append(f"{where}: agent must be one of {', '.join(AGENTS)}")
            if not isinstance(step.prompt, str) or not step.prompt.strip():
                errors.append(f"{where}: an agent step needs a prompt")
            if not isinstance(step.review, bool):
                errors.append(f"{where}: review must be true or false")
        steps[sid] = step
    for step in steps.values():
        for need in step.needs:
            if need not in steps:
                errors.append(f"step {step.id!r}: needs unknown step {need!r}")
            elif need == step.id:
                errors.append(f"step {step.id!r}: cannot need itself")
    ordered = _dependency_order(steps, errors)
    if errors:
        raise FlowError("\n".join(errors))
    return Flow(name, description or "", dict(params), ordered)


def _dependency_order(steps: dict[str, Step], errors: list[str]) -> dict[str, Step]:
    ordered: dict[str, Step] = {}
    remaining = dict(steps)
    while remaining:
        ready = [s for s in remaining.values() if all(n in ordered or n not in steps for n in s.needs)]
        if not ready:
            errors.append(f"steps form a cycle: {', '.join(sorted(remaining))}")
            return {**ordered, **remaining}
        for step in ready:
            ordered[step.id] = remaining.pop(step.id)
    return ordered


def parse_text(text: str) -> Flow:
    try:
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise FlowError(f"Not valid YAML: {exc}") from None
    return parse(data)


def load(path: Path) -> Flow:
    flow = parse_text(path.read_text(encoding="utf-8"))
    if path.stem != flow.name:
        raise FlowError(f"{path.name}: the file must be named {flow.name}.yaml")
    return flow


def load_all(directory: Path = FLOWS_DIR) -> tuple[dict[str, Flow], dict[str, str]]:
    """All valid flows in the folder, plus an error message per invalid file."""
    flows: dict[str, Flow] = {}
    broken: dict[str, str] = {}
    for path in sorted(directory.glob("*.yaml")):
        try:
            flow = load(path)
            flows[flow.name] = flow
        except (FlowError, OSError) as exc:
            broken[path.name] = str(exc)
    return flows, broken
