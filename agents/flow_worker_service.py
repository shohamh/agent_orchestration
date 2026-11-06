"""One engine's flow worker as its own container: keeps flow_worker_<engine>.py running on the active
versions, and lets the flow host restart it when a flow is activated.

    python -u agents/flow_worker_service.py hatchet

  GET  /state             -> {"running": bool, "revisions": {flow: revision the worker was started with}}
  POST /restart {"wait"}  -> {"error": why the worker did not stay up for `wait` seconds, or null}

Env: FLOW_WORKER_SERVICE_PORT (8098), FLOW_HOST_TOKEN (required, the flow host sends it).
"""

import hmac
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from flow_host import ENGINES, Store, Supervisor, activation_lock


def serve(supervisor, engine: str, token: str, bind: str = "0.0.0.0", port: int = 8098) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def _send(self, status: int, body: dict) -> None:
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _allowed(self) -> bool:
            if hmac.compare_digest(self.headers.get("Authorization", ""), f"Bearer {token}"):
                return True
            self._send(401, {"error": "unauthorized"})
            return False

        def do_GET(self) -> None:
            if not self._allowed():
                return
            if self.path != "/state":
                return self._send(404, {"error": "not found"})
            self._send(200, {"running": supervisor.running(engine), "revisions": supervisor.revisions.get(engine)})

        def do_POST(self) -> None:
            if not self._allowed():
                return
            if self.path != "/restart":
                return self._send(404, {"error": "not found"})
            try:
                wait = float(json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}").get("wait", 0))
            except (AttributeError, TypeError, ValueError):
                return self._send(400, {"error": "wait must be a number of seconds"})
            with activation_lock:  # not while the watcher below is restarting it
                self._send(200, {"error": supervisor.restart_one(engine, wait)})

        def log_message(self, *args) -> None:
            pass

    return ThreadingHTTPServer((bind, port), Handler)


def main() -> None:
    engine = sys.argv[1] if len(sys.argv) == 2 else ""
    if engine not in ENGINES or engine == "trigger":
        raise SystemExit("usage: flow_worker_service.py hatchet|prefect")
    token = os.environ.get("FLOW_HOST_TOKEN", "")
    if not token:
        raise SystemExit("FLOW_HOST_TOKEN is required")
    supervisor = Supervisor([engine], Store())
    supervisor.restart()
    threading.Thread(target=supervisor.watch, daemon=True).start()
    serve(supervisor, engine, token, port=int(os.environ.get("FLOW_WORKER_SERVICE_PORT", "8098"))).serve_forever()


if __name__ == "__main__":
    main()
