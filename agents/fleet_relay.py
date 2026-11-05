"""Run the relay the coordinator and the PCs meet at (flow_host starts it)."""
import os
from aiohttp import web
from flowkit.install import add_routes
from flowkit.relay import DEFAULT_PORT, make_app

if __name__ == '__main__':
    web.run_app(make_app(install=add_routes), host=os.environ.get('FLOW_RELAY_BIND', '127.0.0.1'), port=int(os.environ.get('FLOW_RELAY_PORT', DEFAULT_PORT)))
