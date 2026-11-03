"""Worker subprocess entrypoint. Local execution only; never recursively dispatch."""
import asyncio
import json
import os
import sys
import traceback

from acp.stdio import stdio_streams
from .definition import parse
from .executor import run_step


async def main():
    reader, _ = await stdio_streams(limit=256 * 1024)
    start = json.loads(await reader.readline())
    async def receive():
        line = await reader.readline()
        if not line: await asyncio.Event().wait()
        return json.loads(line)
    async def sink(event):
        print(json.dumps(event, ensure_ascii=False), flush=True)
    output = await run_step(parse(start['flow']), start['step'], start['context'], start['runId'], receive, sink)
    await sink({'type': 'result', 'output': output})


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'): sys.stdout.reconfigure(encoding='utf-8')
    code = 0
    try: asyncio.run(main())
    except BaseException:
        traceback.print_exc()
        code = 1
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)
