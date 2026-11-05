"""Run a relayed execution PC: the execution service on localhost plus the outbound relay connector.

One process, started by the per-user startup entry the installer creates. Nothing listens on the
network: the service binds to 127.0.0.1 and the connector only makes outbound calls.

    python agents/worker_runner.py --home <install folder>

The folder holds worker.json ({"id", "slots", "server", "port"}), a `token` file readable only by this
user, and `runtime/` (job workspaces and the capacity database). FLOW_WORKER_HOME is an alternative to --home.
"""
import argparse
import asyncio
import json
import logging
import os
from pathlib import Path

from aiohttp import web

from flowkit.connector import Connector
from flowkit.worker_service import make_app


async def run(home):
    config = json.loads((home / 'worker.json').read_text(encoding='utf-8'))
    token = (home / 'token').read_text(encoding='utf-8').strip()
    if not token: raise SystemExit('The token file is empty')
    port = int(config.get('port', 8096))
    os.environ['FLOW_WORKER_TOKEN'] = token
    app = make_app(worker_id=config['id'], slots=int(config.get('slots', 1)), root=home / 'runtime', token=token)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '127.0.0.1', port).start()
    logging.info('Execution service for %s on 127.0.0.1:%s; relaying through %s', config['id'], port, config['server'])
    try: await Connector(config['server'], config['id'], token, port).run()
    finally: await runner.cleanup()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--home', default=os.environ.get('FLOW_WORKER_HOME'))
    parser.add_argument('--log', help='append the log to this file (for a startup entry with no console)')
    args = parser.parse_args(argv)
    if not args.home: parser.error('--home or FLOW_WORKER_HOME is required')
    handlers = [logging.FileHandler(args.log, encoding='utf-8')] if args.log else None
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(message)s', handlers=handlers)
    asyncio.run(run(Path(args.home)))


if __name__ == '__main__':
    main()
