"""JSONL bridge used by Trigger.dev. Stdout is reserved for session events."""

import asyncio
import json
import sys

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
    asyncio.run(main())
