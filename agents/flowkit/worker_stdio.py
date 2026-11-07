"""Worker subprocess entrypoint. Local execution only; never recursively dispatch."""
import asyncio
import json
import os
import sys
import traceback

from agentdriver.stdio_io import event_streams
from .definition import parse
from .executor import run_step


async def main():
    reader, sink = await event_streams()
    start = json.loads(await reader.readline())
    async def receive():
        line = await reader.readline()
        if not line: await asyncio.Event().wait()
        return json.loads(line)
    flow = parse(start['flow']) if 'flow' in start else None
    if os.environ.get('FLOW_CAPACITY_HANDSHAKE') == '1':
        await sink({'type':'capacity_ready'})
        if json.loads(await reader.readline()) != {'type':'capacity_go'}:
            raise RuntimeError('Worker start was not authorized')
        await sink({'type':'capacity_started'})
    if 'attached' in start:  # fetch what was attached to a run; the job's files are the result
        from .executor import workspace_for
        from .launch_context import prepare_on_worker
        output = await prepare_on_worker(start['attached'], workspace_for(start['runId']), sink)
    elif 'session' in start:  # a standalone card placed on this PC
        from agentdriver.session import run_session
        from .executor import workspace_for
        session = dict(start['session'])
        # The files that came with the job are in the run's folder. Without any: a new workspace, as for a blank card.
        session['workspace'] = str(workspace_for(start['runId'])) if session.pop('attached', False) else ''
        output = await run_session(session, receive, sink)
    else:
        output = await run_step(flow, start['step'], start['context'], start['runId'], receive, sink)
    await sink({'type': 'result', 'output': output})


if __name__ == '__main__':
    if hasattr(sys.stdout, 'reconfigure'): sys.stdout.reconfigure(encoding='utf-8')
    code = 0
    try: asyncio.run(main())
    except BaseException:
        traceback.print_exc()
        code = 1
    finally:
        if not sys.stdout.closed: sys.stdout.flush()
        sys.stderr.flush()
        os._exit(code)
