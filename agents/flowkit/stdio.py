"""JSONL bridge used by Trigger.dev step tasks. Stdout is reserved for step events.

stdin:  {"type": "start", "flow": <flow JSON>, "step": id, "context": {...}, "runId": id}
        then agent input messages (same as agent sessions), one per line
stdout: step events, one per line, then {"type": "result", "output": ...}
A failure prints the traceback to stderr and exits with 1.
"""

import asyncio
import json
import os
import sys
import traceback

from acp.stdio import stdio_streams

from .definition import parse
from .distributed import execute_step as run_step


async def main() -> None:
    reader, _ = await stdio_streams(limit=256 * 1024)

    async def receive() -> dict:
        line = await reader.readline()
        if not line:
            # Trigger.dev closes stdin when the step ends; an idle program step never reads it.
            await asyncio.Event().wait()
        return json.loads(line)

    def write(event: dict) -> None:
        sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
        sys.stdout.flush()

    async def sink(event: dict) -> None:
        write(event)

    start = json.loads(await reader.readline())
    if start.get("type") != "start":
        raise ValueError("First input must be a start message")
    definition = dict(start["flow"])
    asset_snapshot = definition.pop("_asset_snapshot", None)
    if asset_snapshot is None: raise ValueError("Flow has no immutable asset snapshot; reactivate it before launching")
    from .activation import Store
    flow = Store().load_bound_flow(asset_snapshot, definition)
    output = await run_step(flow, start["step"], start["context"], start["runId"], receive, sink, engine="trigger")
    write({"type": "result", "output": output})


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    code = 0
    try:
        asyncio.run(main())
    except BaseException:
        traceback.print_exc()
        code = 1
    finally:
        # Same Windows stdin-thread shutdown problem as agentdriver.stdio.
        if not sys.stdout.closed:
            sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)
