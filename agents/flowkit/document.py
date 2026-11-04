"""Editable flow documents: YAML text <-> the authored flow mapping, with located errors.

The document is the flow as authored, in authored order, without the generated setup_environment
step (it is derived from `environment`). Every step has `needs` as a list (empty when none was
written; an empty `needs` is not written back). serialize() writes canonical YAML; `lossless`
says whether that reproduces the text exactly, so an editor can warn before a graph edit
replaces comments or hand formatting. See docs/superpowers/specs/2026-11-03-workflow-builder-api.md.
"""

import copy
import datetime
import math
from typing import Any

import yaml

from . import definition
from .template import _EXPR

TOP_KEYS = ("name", "description", "input", "worker", "environment", "steps")
STEP_KEYS = ("kind", "description", "needs", "agent", "review", "worker", "retries", "timeout_minutes", "setup", "run", "prompt")
GENERATED = "setup_environment"


def error(message: str, step: str | None = None, field: str | None = None) -> dict:
    return {"message": message, "step": step, "field": field}


def parse(text: str, name: str | None = None) -> dict:
    """{"document": mapping | None, "errors": [...], "lossless": bool}; name: the expected flow name."""
    document, errors, lossless, _ = analyze(text, name)
    return {"document": document, "errors": errors, "lossless": lossless}


def analyze(text: str, name: str | None = None) -> tuple[dict | None, list[dict], bool, definition.Flow | None]:
    """parse() plus the validated Flow (None when there are errors)."""
    try:
        text.encode("utf-8")
    except UnicodeEncodeError:
        return None, [error("The text contains invalid Unicode (an unpaired surrogate)")], False, None
    try:
        # Aliases can expand exponentially ("billion laughs"); they are not editable anyway.
        alias = next((e for e in yaml.parse(text, Loader=yaml.SafeLoader) if isinstance(e, yaml.AliasEvent)), None)
        if alias is not None:
            return None, [error(f"YAML aliases (*{alias.anchor}, line {alias.start_mark.line + 1}) are not supported; "
                                "write the value out")], False, None
        node = yaml.compose(text, Loader=yaml.SafeLoader)
        data = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        return None, [error(f"Not valid YAML: {exc}")], False, None
    if not isinstance(data, dict):
        return None, [error("A flow must be a mapping with name, input and steps")], False, None
    errors = _duplicates(node)
    document = _jsonable(_normalized(data), (), errors)
    found, flow = check(document, name)
    errors += found
    return document, errors, serialize(document) == text, None if errors else flow


def check(document: Any, name: str | None = None) -> tuple[list[dict], definition.Flow | None]:
    """Every problem with a document, and its Flow when it has none."""
    if not isinstance(document, dict):
        return [error("A flow must be a mapping with name, input and steps")], None
    errors, flow = [], None
    try:
        flow = definition.parse(document)
    except definition.FlowError as exc:
        generated = "environment" in document and GENERATED not in (document.get("steps") or {})
        errors += [error(p["message"], None, "environment") if generated and p["step"] == GENERATED else p
                   for p in exc.problems]
    errors += _references(document)
    if name is not None and document.get("name") != name:
        errors.append(error(f"the flow's name is {document.get('name')!r}, but it is saved as {name!r}", None, "name"))
    return errors, None if errors else flow


def validate(document: Any, name: str | None = None) -> list[dict]:
    return check(document, name)[0]


def serialize(document: dict) -> str:
    """Canonical YAML: fixed key order, `|` blocks for multi-line text, Unicode kept."""
    return yaml.dump(canonical(document), Dumper=_Dumper, sort_keys=False, allow_unicode=True,
                     default_flow_style=False, width=1 << 16)


def canonical(document: dict) -> dict:
    result = _ordered(document, TOP_KEYS)
    steps = result.get("steps")
    if isinstance(steps, dict):
        ordered = {}
        for sid, raw in steps.items():
            if sid == GENERATED and "environment" in document and isinstance(raw, dict) and "setup" in raw:
                continue  # the generated environment step is derived, never authored
            if isinstance(raw, dict):
                raw = {k: v for k, v in _ordered(raw, STEP_KEYS).items() if not (k == "needs" and v in ([], None))}
            ordered[sid] = raw
        result["steps"] = ordered
    return result


def _ordered(mapping: dict, keys: tuple[str, ...]) -> dict:
    return {**{k: mapping[k] for k in keys if k in mapping}, **{k: v for k, v in mapping.items() if k not in keys}}


def _normalized(data: dict) -> dict:
    document = copy.deepcopy(data)
    steps = document.get("steps")
    if isinstance(steps, dict):
        for raw in steps.values():
            if isinstance(raw, dict):
                needs = raw.get("needs")
                if needs is None:
                    raw["needs"] = []
                elif isinstance(needs, str):
                    raw["needs"] = [needs]
    return document


def _location(path: tuple) -> tuple[str | None, str | None]:
    if len(path) >= 2 and path[0] == "steps":
        return path[1], path[2] if len(path) >= 3 else None
    return None, path[0] if path else None


def _jsonable(value: Any, path: tuple, errors: list[dict]) -> Any:
    """YAML can hold values JSON cannot (dates, binary, non-finite numbers, surrogate escapes):
    report them and show them as text."""
    if isinstance(value, dict):
        return {_jsonable(k if isinstance(k, str) else str(k), path, errors): _jsonable(v, path + (k,), errors)
                for k, v in value.items()}
    if isinstance(value, list):
        return [_jsonable(v, path, errors) for v in value]
    step, field = _location(path)
    where = ".".join(map(str, path)) or "value"
    if isinstance(value, str):
        try:
            value.encode("utf-8")
            return value
        except UnicodeEncodeError:  # a "😀"-style escape: PyYAML keeps the halves apart
            errors.append(error(f"{where}: a surrogate escape is not valid text; write the character itself", step, field))
            return value.encode("utf-8", errors="replace").decode("utf-8")
    if value is None or isinstance(value, (bool, int)) or (isinstance(value, float) and math.isfinite(value)):
        return value
    errors.append(error(f"{where}: {type(value).__name__} values are not supported; quote it as text", step, field))
    return value.isoformat() if isinstance(value, (datetime.date, datetime.time)) else str(value)


def _duplicates(node: Any, path: tuple = ()) -> list[dict]:
    """PyYAML silently keeps the last of two equal keys; an editor must not hide the other."""
    found = []
    if isinstance(node, yaml.MappingNode):
        seen = set()
        for key, value in node.value:
            name = key.value if isinstance(key, yaml.ScalarNode) else None
            if name is not None and name in seen:
                where = path + (name,)
                step, field = _location(where)
                line = key.start_mark.line + 1
                if where[:1] == ("steps",) and len(where) == 2:
                    found.append(error(f"step {name!r} is defined more than once (line {line})", step))
                else:
                    found.append(error(f"{'.'.join(map(str, path)) + ': ' if path else ''}duplicate key {name!r} (line {line})", step, field))
            seen.add(name)
            found += _duplicates(value, path + (name,))
    elif isinstance(node, yaml.SequenceNode):
        for item in node.value:
            found += _duplicates(item, path)
    return found


def _references(document: dict) -> list[dict]:
    """`{{ input.x }}` must be a declared input; `{{ steps.y.output }}` must be one of the step's needs."""
    inputs = document.get("input") if isinstance(document.get("input"), dict) else {}
    steps = document.get("steps") if isinstance(document.get("steps"), dict) else {}
    environment = "environment" in document
    errors = []
    for sid, raw in steps.items():
        if not isinstance(raw, dict):
            continue
        needs = raw.get("needs") or []
        needs = [needs] if isinstance(needs, str) else needs if isinstance(needs, list) else []
        if environment and not needs and sid != GENERATED:
            needs = [GENERATED]
        for field in ("run", "prompt"):
            text = raw.get(field)
            if not isinstance(text, str):
                continue
            for match in _EXPR.finditer(text):
                parts, expression = match.group(1).split("."), match.group(0)
                if parts[0] == "input":
                    if len(parts) > 1 and parts[1] not in inputs:
                        errors.append(error(f"step {sid!r}: {expression} is not a declared input", sid, field))
                elif parts[0] == "steps":
                    if len(parts) < 3 or parts[2] != "output":
                        errors.append(error(f"step {sid!r}: {expression} must be steps.<step>.output[.field]", sid, field))
                    elif parts[1] not in needs:
                        known = parts[1] in steps or (environment and parts[1] == GENERATED)
                        reason = "is not in this step's needs" if known else "is not a step"
                        errors.append(error(f"step {sid!r}: {expression}: {parts[1]!r} {reason}", sid, field))
                else:
                    errors.append(error(f"step {sid!r}: {expression} is unknown; use input.<name> or steps.<needed step>.output", sid, field))
    return errors


class _Dumper(yaml.SafeDumper):
    def ignore_aliases(self, data: Any) -> bool:
        return True


_Dumper.add_representer(str, lambda dumper, value: dumper.represent_scalar(
    "tag:yaml.org,2002:str", value, style="|" if "\n" in value else None))
_Dumper.add_representer(list, lambda dumper, value: dumper.represent_sequence(
    "tag:yaml.org,2002:seq", value, flow_style=all(not isinstance(v, (dict, list)) for v in value)))
