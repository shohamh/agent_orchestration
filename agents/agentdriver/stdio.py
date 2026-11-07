"""JSONL bridge used by Trigger.dev. Stdout is reserved for session events."""

import asyncio
import json
import os
import sys
import traceback

from .stdio_io import event_streams

from .session import run_session


async def main():
    reader, sink = await event_streams()

    async def receive():
        line = await reader.readline()
        if not line:
            raise EOFError("Agent session input closed")
        return json.loads(line)


    start = await receive()
    if start.pop("type", None) != "start":
        raise ValueError("First input must be a start message")
    if start.get("agent") == "program" or start.get("context"):  # needs the attached sources first
        import uuid
        from flowkit.distributed import execute_session
        run_id = start.pop("runId", None) or uuid.uuid4().hex
        await execute_session(start, run_id, receive, sink)
    else:
        await run_session(start, receive, sink)


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
        # On Windows the ACP stdio reader is a daemon thread blocked in stdin.read(); normal interpreter
        # shutdown then aborts on its buffer lock ("could not acquire lock for <stdin>", exit 0xC0000005)
        # and turns a finished session into a failed run. Flush and leave without finalization.
        if not sys.stdout.closed:
            sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)
