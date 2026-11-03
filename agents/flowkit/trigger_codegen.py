"""Generate trigger.dev/example-project/src/trigger/flows.generated.ts from flows/*.yaml.

Trigger.dev tasks must exist as exported code, so each flow becomes `defineFlow(<definition>)`
and its parent and step tasks are exported. `trigger dev` reloads the file when it changes.

    agents/.venv/Scripts/python.exe agents/flowkit/trigger_codegen.py
"""

import json
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from flowkit.definition import FLOWS_DIR, Flow, load_all  # noqa: E402

TARGET = Path(__file__).resolve().parents[2] / "trigger.dev" / "example-project" / "src" / "trigger" / "flows.generated.ts"


def ident(text: str) -> str:
    return text.replace("-", "_")


def render(flows: dict[str, Flow]) -> str:
    lines = ["// GENERATED from flows/*.yaml by agents/flowkit/trigger_codegen.py. Do not edit; edit the YAML.",
             'import { defineFlow } from "../flows-runtime.js";', ""]
    for flow in flows.values():
        name = ident(flow.name)
        lines.append(f"const {name} = defineFlow({json.dumps(flow.to_json(), ensure_ascii=False)});")
        lines.append(f"export const flow_{name} = {name}.parent;")
        lines += [f'export const flow_{name}__{sid} = {name}.steps["{sid}"];' for sid in flow.steps]
        lines.append("")
    return "\n".join(lines)


def generate(directory: Path = FLOWS_DIR, target: Path = TARGET) -> bool:
    """Write the file; returns whether it changed (so an unchanged save doesn't reload the worker)."""
    flows, _ = load_all(directory)
    text = render(flows)
    if target.exists() and target.read_text(encoding="utf-8") == text:
        return False
    target.write_text(text, encoding="utf-8", newline="\n")
    return True


if __name__ == "__main__":
    print("updated" if generate() else "unchanged", TARGET)
