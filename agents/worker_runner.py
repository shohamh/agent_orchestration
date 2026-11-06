"""Run a relayed execution PC: the execution service on localhost, the outbound relay connector and any extra components.

One process, started by the per-user startup entry the installer creates (manage it with `agent-worker`). Nothing
listens on the network: the service binds to 127.0.0.1 and the connector only makes outbound calls.

    python agents/worker_runner.py --home <install folder>

The folder holds worker.json ({"id", "slots", "server", "port"}), a `token` file readable only by this user, a
`version` file (the installed worker version), optional components.json (see flowkit/components.py) and `runtime/`
(job workspaces and the capacity database). FLOW_WORKER_HOME is an alternative to --home.
"""
import argparse
import asyncio
import json
import logging
import os
import signal
from pathlib import Path

from aiohttp import web

from flowkit.components import Supervisor
from flowkit.connector import Connector
from flowkit.worker_service import make_app


def read_version(home):
    try: return (home / 'version').read_text(encoding='utf-8').strip()
    except OSError: return ''


async def run(home):
    config = json.loads((home / 'worker.json').read_text(encoding='utf-8'))
    token = (home / 'token').read_text(encoding='utf-8').strip()
    if not token: raise SystemExit('The token file is empty')
    port = int(config.get('port', 8096))
    os.environ['FLOW_WORKER_TOKEN'] = token
    os.environ['FLOW_WORKER_VERSION'] = read_version(home)
    pidfile = home / 'worker.pid'
    pidfile.write_text(str(os.getpid()), encoding='utf-8')  # `agent-worker stop` and status find the runner through this
    app = make_app(worker_id=config['id'], slots=int(config.get('slots', 1)), root=home / 'runtime', token=token)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '127.0.0.1', port).start()
    logging.info('Execution service for %s %s on 127.0.0.1:%s; relaying through %s', config['id'], read_version(home) or '(no version)', port, config['server'])
    supervisor = Supervisor(home, logging.getLogger('components'))
    tasks = [asyncio.create_task(Connector(config['server'], config['id'], token, port, home=home).run()), asyncio.create_task(supervisor.run())]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        supervisor.stopping = True
        for task in tasks: task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()
        try:
            if pidfile.read_text(encoding='utf-8').strip() == str(os.getpid()): pidfile.unlink()
        except OSError: pass


async def main(home):
    task = asyncio.create_task(run(home))
    if os.name != 'nt':
        loop = asyncio.get_running_loop()
        for name in ('SIGTERM', 'SIGINT'): loop.add_signal_handler(getattr(signal, name), task.cancel)  # components are stopped on the way out
    try: await task
    except asyncio.CancelledError: pass


def start(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home', default=os.environ.get('FLOW_WORKER_HOME'))
    parser.add_argument('--log', help='append the log to this file (for a startup entry with no console)')
    args = parser.parse_args(argv)
    if not args.home: parser.error('--home or FLOW_WORKER_HOME is required')
    handlers = [logging.FileHandler(args.log, encoding='utf-8')] if args.log else None
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s', handlers=handlers)
    asyncio.run(main(Path(args.home)))


if __name__ == '__main__':
    start()
