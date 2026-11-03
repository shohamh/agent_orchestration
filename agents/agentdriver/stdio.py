"""JSONL bridge used by Trigger.dev. Stdout is reserved for session events."""

import asyncio
import json
import os
import sys
import traceback

from acp.stdio import stdio_streams

from .session import run_session


async def main():
    reader, _ = await stdio_streams(limit=256 * 1024)

    async def receive():
        line = await reader.readline()
        if not line:
            raise EOFError("Agent session input closed")
        return json.loads(line)

    async def sink(event):
        sys.stdout.write(json.dumps(event, ensure_ascii=False) + "\n")
        sys.stdout.flush()

    start = await receive()
    if start.pop("type", None) != "start":
        raise ValueError("First input must be a start message")
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
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)
