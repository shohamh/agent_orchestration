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
    """The flow file is invalid; the message lists every problem found.

    problems: the same problems as {message, step, field} (field: a step key, or a top-level key
    when step is None), for editors that highlight them."""

    def __init__(self, message: str, problems: list[dict] | None = None) -> None:
        super().__init__(message)
        self.problems = problems if problems is not None else [{"message": message, "step": None, "field": None}]


@dataclass
class Step:
    id: str
    kind: str  # program | agent
    needs: list[str] = field(default_factory=list)
    run: str = ""  # program: shell command
    requires_command: str = ""
    agent: str = "demo"  # agent
    prompt: str = ""  # agent
    review: bool = False  # agent: wait for "Mark done" before the flow continues
    retries: int = 0
    timeout_minutes: int = 60
    worker: dict = field(default_factory=dict)
    setup: dict | None = None

    def to_json(self) -> dict[str, Any]:
        data = {"kind": self.kind, "needs": self.needs, "retries": self.retries, "timeout_minutes": self.timeout_minutes}
        if self.kind == "program":
            data["run"] = self.run
            if self.requires_command: data["requires_command"] = self.requires_command
        else:
            data.update(agent=self.agent, prompt=self.prompt, review=self.review)
        if self.worker: data['worker'] = self.worker
        if self.setup is not None: data['setup'] = self.setup
        return data


@dataclass
class Flow:
    name: str
    description: str
    input: dict[str, Any]
    steps: dict[str, Step]  # in dependency order
    worker: dict = field(default_factory=dict)

    @property
    def environment(self):
        return next((s.setup for s in self.steps.values() if s.setup is not None), None)

    def to_json(self) -> dict[str, Any]:
        result = {"name": self.name, "description": self.description, "input": self.input,
                  "steps": {sid: step.to_json() for sid, step in self.steps.items()}}
        if self.worker: result['worker'] = self.worker
        return result

    def children(self, step_id: str) -> list[str]:
        return [s.id for s in self.steps.values() if step_id in s.needs]


def parse(data: Any) -> Flow:
    """Validate a decoded flow mapping; raises FlowError listing every problem."""
    errors = _Problems()
    if not isinstance(data, dict):
        raise FlowError("A flow must be a mapping with name, input and steps")
    unknown = set(data) - {"name", "description", "input", "steps", "worker", "environment"}
    if unknown:
        errors.add(f"unknown top-level keys: {', '.join(sorted(map(str, unknown)))}", field=next(iter(unknown)) if len(unknown) == 1 else None)
    name = data.get("name")
    if not isinstance(name, str) or not FLOW_NAME.match(name):
        errors.add("name must be lowercase letters, digits and '-', starting with a letter (max 48)", field="name")
    elif name in RESERVED or name.startswith("flow-"):
        errors.add(f"name {name!r} is reserved", field="name")
    description = data.get("description", "")
    if not isinstance(description, str):
        errors.add("description must be text", field="description")
    params = data.get("input") or {}
    if not isinstance(params, dict):
        errors.add("input must be a mapping of parameter names to default values", field="input")
        params = {}
    for key, value in params.items():
        if not isinstance(key, str) or not STEP_ID.match(key):
            errors.add(f"input name {key!r} must be lowercase letters, digits and '_'", field="input")
        if not (value is None or type(value) in (str, int, float, bool)):
            errors.add(f"input {key!r} must have a text, number or true/false default", field="input")
    raw_steps = data.get("steps")
    if not isinstance(raw_steps, dict) or not raw_steps:
        errors.add("steps must be a non-empty mapping of step ids to steps", field="steps")
        raw_steps = {}
    if 'environment' in data:
        from .environment import configuration
        try:
            environment = configuration(data['environment'])
            if 'setup_environment' in raw_steps: raise ValueError('setup_environment is reserved when environment is configured')
            raw_steps = {'setup_environment': {'kind':'agent','agent':environment.get('agent','codex'),
                'prompt':'Prepare the environment and wait for review.', 'review':True,'setup':environment,
                'worker':environment.get('worker',{})}, **{
                    sid:{**raw, 'needs':['setup_environment']} if isinstance(raw,dict) and not raw.get('needs') else raw
                    for sid,raw in raw_steps.items()}}
        except ValueError as exc: errors.add(str(exc), field="environment")
    steps: dict[str, Step] = {}
    for sid, raw in raw_steps.items():
        where = f"step {sid!r}"
        if not isinstance(sid, str) or not STEP_ID.match(sid):
            errors.add(f"{where}: id must be lowercase letters, digits and '_', starting with a letter", str(sid))
            continue
        if not isinstance(raw, dict):
            errors.add(f"{where}: must be a mapping", sid)
            continue
        kind = raw.get("kind")
        allowed = {"kind", "needs", "retries", "timeout_minutes", "description", "worker"} | (
            {"run", "requires_command"} if kind == "program" else {"agent", "prompt", "review", "setup"})
        extra = set(raw) - allowed
        if kind not in ("program", "agent"):
            errors.add(f"{where}: kind must be program or agent", sid, "kind")
            continue
        if extra:
            errors.add(f"{where}: unknown keys for a {kind} step: {', '.join(sorted(map(str, extra)))}", sid, next(iter(extra)) if len(extra) == 1 else None)
        needs = raw.get("needs") or []
        if isinstance(needs, str):
            needs = [needs]
        if not isinstance(needs, list) or not all(isinstance(n, str) for n in needs):
            errors.add(f"{where}: needs must be a list of step ids", sid, "needs")
            needs = []
        retries, timeout = raw.get("retries", 0), raw.get("timeout_minutes", 60)
        if type(retries) is not int or not 0 <= retries <= 10:
            errors.add(f"{where}: retries must be a whole number from 0 to 10", sid, "retries")
        if type(timeout) is not int or not 1 <= timeout <= 24 * 60:
            errors.add(f"{where}: timeout_minutes must be from 1 to 1440", sid, "timeout_minutes")
        step = Step(sid, kind, list(dict.fromkeys(needs)), retries=retries if type(retries) is int else 0,
                    timeout_minutes=timeout if type(timeout) is int else 60)
        from .workers import selector
        try: step.worker = selector(raw.get('worker'))
        except ValueError as exc: errors.add(f'{where}: {exc}', sid, 'worker')
        if kind == "program":
            step.run = raw.get("run")
            requirement = raw.get("requires_command", "")
            if not isinstance(requirement, str) or (requirement and (requirement != requirement.strip() or any(c in requirement for c in "\x00\r\n"))):
                errors.add(f"{where}: requires_command must be an executable name", sid, "requires_command")
            else:
                step.requires_command = requirement
            if not isinstance(step.run, str) or not step.run.strip():
                errors.add(f"{where}: a program step needs a run command", sid, "run")
        else:
            step.agent, step.prompt, step.review = raw.get("agent", "demo"), raw.get("prompt"), raw.get("review", False)
            if step.agent not in AGENTS:
                errors.add(f"{where}: agent must be one of {', '.join(AGENTS)}", sid, "agent")
            if not isinstance(step.prompt, str) or not step.prompt.strip():
                errors.add(f"{where}: an agent step needs a prompt", sid, "prompt")
            if not isinstance(step.review, bool):
                errors.add(f"{where}: review must be true or false", sid, "review")
            if 'setup' in raw:
                from .environment import configuration
                try: step.setup = configuration(raw['setup'])
                except ValueError as exc: errors.add(str(exc), sid, "setup")
        steps[sid] = step
    for step in steps.values():
        for need in step.needs:
            if need not in steps:
                errors.add(f"step {step.id!r}: needs unknown step {need!r}", step.id, "needs")
            elif need == step.id:
                errors.add(f"step {step.id!r}: cannot need itself", step.id, "needs")
    ordered = _dependency_order(steps, errors)
    from .workers import selector
    try: default_worker = selector(data.get('worker'))
    except ValueError as exc:
        default_worker = {}
        errors.add(str(exc), field="worker")
    if errors.problems:
        raise FlowError("\n".join(errors.lines), errors.problems)
    return Flow(name, description or "", dict(params), ordered, worker=default_worker)


class _Problems:
    """Problem messages (FlowError's text) and the same problems located by step and field."""

    def __init__(self) -> None:
        self.lines: list[str] = []
        self.problems: list[dict] = []

    def add(self, message: str, step: str | None = None, field: str | None = None) -> None:
        self.lines.append(message)
        self.problems.append({"message": message, "step": step, "field": field})


def _dependency_order(steps: dict[str, Step], errors: _Problems) -> dict[str, Step]:
    ordered: dict[str, Step] = {}
    remaining = dict(steps)
    while remaining:
        ready = [s for s in remaining.values() if all(n in ordered or n not in steps for n in s.needs)]
        if not ready:
            errors.lines.append(f"steps form a cycle: {', '.join(sorted(remaining))}")
            errors.problems += [{"message": f"step {sid!r}: is part of a dependency cycle", "step": sid, "field": "needs"}
                                for sid in _in_cycle(remaining)]
            return {**ordered, **remaining}
        for step in ready:
            ordered[step.id] = remaining.pop(step.id)
    return ordered


def _in_cycle(steps: dict[str, Step]) -> list[str]:
    """Steps that can reach themselves through needs (a cycle's members, not their dependents)."""
    def reaches(start: str) -> bool:
        seen, todo = set(), [n for n in steps[start].needs if n in steps]
        while todo:
            sid = todo.pop()
            if sid == start:
                return True
            if sid not in seen:
                seen.add(sid)
                todo += [n for n in steps[sid].needs if n in steps]
        return False
    return [sid for sid in steps if reaches(sid)]


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
