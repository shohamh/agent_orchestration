"""Run the execution service using the same agents virtualenv as native coordinators."""
import os
from aiohttp import web
from flowkit.worker_service import make_app

if __name__ == '__main__':
    bind = os.environ.get('FLOW_WORKER_BIND', '127.0.0.1')
    if bind not in ('127.0.0.1', 'localhost', '::1') and not os.environ.get('FLOW_WORKER_TOKEN'):
        raise SystemExit('FLOW_WORKER_TOKEN is required for a network execution service')
    web.run_app(make_app(), host=bind, port=int(os.environ.get('FLOW_WORKER_PORT', '8096')))
