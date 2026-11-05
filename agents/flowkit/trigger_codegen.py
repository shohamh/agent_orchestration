"""Generate trigger.dev/example-project/src/trigger/flows.generated.ts from the active flows.

Trigger.dev tasks must exist as exported code, so each flow becomes `defineFlow(<definition>)`
and its parent and step tasks are exported. `trigger dev` reloads the file when it changes, which
interrupts its running tasks, so only activation (agents/flow_host.py) regenerates it: the input is
the active snapshots Trigger.dev serves (flowkit/activation.py), never the editable drafts.

    agents/.venv/Scripts/python.exe agents/flowkit/trigger_codegen.py
"""

import json
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit.activation import load_active  # noqa: E402
from flowkit.definition import Flow  # noqa: E402

TARGET = Path(__file__).resolve().parents[2] / "trigger.dev" / "example-project" / "src" / "trigger" / "flows.generated.ts"


def ident(text: str) -> str:
    return text.replace("-", "_")


def render(flows: dict[str, Flow]) -> str:
    # The header predates active snapshots; keeping it avoids a dev worker reload on upgrade.
    lines = ["// GENERATED from flows/*.yaml by agents/flowkit/trigger_codegen.py. Do not edit; edit the YAML.",
             'import { defineFlow } from "../flows-runtime.js";', ""]
    for flow in flows.values():
        name = ident(flow.name)
        definition = flow.to_json()
        if getattr(flow, "_asset_snapshot", None): definition["_asset_snapshot"] = flow._asset_snapshot
        lines.append(f"const {name} = defineFlow({json.dumps(definition, ensure_ascii=False)});")
        lines.append(f"export const flow_{name} = {name}.parent;")
        lines += [f'export const flow_{name}__{sid} = {name}.steps["{sid}"];' for sid in flow.steps]
        lines.append("")
    return "\n".join(lines)


def current(target: Path = TARGET) -> str | None:
    try:
        return target.read_bytes().decode("utf-8")
    except FileNotFoundError:
        return None


def generate(flows: dict[str, Flow] | None = None, target: Path = TARGET) -> bool:
    """Write the file; returns whether it changed (so an unchanged activation doesn't reload the worker)."""
    from flowkit.activation import atomic_write

    text = render(load_active("trigger")[0] if flows is None else flows)
    if current(target) == text:
        return False
    atomic_write(target, text)
    return True


if __name__ == "__main__":
    # Writing reloads a running `trigger dev` worker; activation through the board is the normal path.
    from flowkit.activation import Store

    if not Store().manifest_path.exists():
        raise SystemExit("No active flows yet (no flow state folder): start agents/flow_host.py once; it activates the drafts.")
    if "--check" in sys.argv[1:]:
        same = render(load_active("trigger")[0]) == current()
        print("up to date" if same else "differs from the active flows", TARGET)
        raise SystemExit(0 if same else 1)
    print("updated" if generate() else "unchanged", TARGET)
