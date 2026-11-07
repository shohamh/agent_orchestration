"""JSONL stdio with backpressure and no buffered events left at process exit."""
import json
import os
import sys

from acp.stdio import stdio_streams


async def event_streams(limit=256 * 1024):
    reader, writer = await stdio_streams(limit=limit)
    if os.name != 'nt':
        # ACP makes stdout nonblocking. Every event must drain before os._exit.
        writer.transport.set_write_buffer_limits(high=0, low=0)

    async def sink(event):
        line = json.dumps(event, ensure_ascii=False) + '\n'
        if os.name == 'nt':
            # ACP's Windows transport is synchronous and swallows write errors.
            sys.stdout.write(line)
            sys.stdout.flush()
        else:
            writer.write(line.encode('utf-8'))
            await writer.drain()

    return reader, sink
