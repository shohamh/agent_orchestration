"""Flow host: owns flows/, keeps every backend's native workflows in sync, serves the board's API.

    agents/.venv/Scripts/python.exe -u agents/flow_host.py

- Runs the Hatchet and Prefect flow workers (flow_worker_hatchet.py / flow_worker_prefect.py) as
  child processes and restarts them whenever a flow file changes, so each backend re-registers
  the flows. Steps running on those workers at that moment are interrupted.
- Regenerates trigger.dev/example-project/src/trigger/flows.generated.ts; the `trigger dev` worker
  (scripts/dev-worker.ps1) reloads it on its own.
- HTTP API for the board, on 127.0.0.1:8095 by default (Docker Desktop forwards the board's
  host.docker.internal to host loopback, so nothing is exposed to the network):
    GET    /flows                -> {"flows": [{name, description, input, steps, yaml}], "broken": {file: error}}
    POST   /validate  {yaml}     -> {"ok": true, "flow": {...}} | {"ok": false, "error": "..."}
    PUT    /flows/<name> {yaml}  -> save (the YAML's name must match)
    DELETE /flows/<name>
  With FLOW_HOST_TOKEN set, requests need `Authorization: Bearer <token>`.

Env: FLOW_HOST_BIND (127.0.0.1), FLOW_HOST_PORT (8095), FLOW_HOST_TOKEN, FLOW_BACKENDS
(hatchet,prefect,trigger: which to keep in sync).
"""

import hmac
import asyncio
import re
import json
import os
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from flowkit import trigger_codegen  # noqa: E402
from flowkit.definition import FLOWS_DIR, FLOW_NAME, FlowError, load_all, parse_text  # noqa: E402

WORKERS = {"hatchet": "flow_worker_hatchet.py", "prefect": "flow_worker_prefect.py"}
lock = threading.Lock()


def log(text: str) -> None:
    print(f"[flow-host] {text}", flush=True)


class Supervisor:
    """Keeps one process per backend worker running; restart() replaces them all."""

    def __init__(self, backends: list[str]) -> None:
        self.backends = [b for b in backends if b in WORKERS]
        self.processes: dict[str, subprocess.Popen] = {}
        self.stopping = False

    def _start(self, backend: str) -> None:
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        self.processes[backend] = subprocess.Popen(
            [sys.executable, "-u", str(HERE / WORKERS[backend])], cwd=HERE.parent, creationflags=flags,
            start_new_session=os.name != "nt", env={**os.environ, "PYTHONUTF8": "1"})
        log(f"started {backend} flow worker (pid {self.processes[backend].pid})")

    def _stop(self, backend: str) -> None:
        process = self.processes.pop(backend, None)
        if not process or process.poll() is not None:
            return
        if os.name == "nt":  # the Prefect worker runs each flow run in a child process
            subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True)
        else:
            import signal

            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(10)
        except subprocess.TimeoutExpired:
            process.kill()

    def restart(self) -> None:
        for backend in self.backends:
            self._stop(backend)
            self._start(backend)

    def watch(self) -> None:
        """Restart a worker that exits on its own (after a short pause, so a crash loop stays slow)."""
        while not self.stopping:
            time.sleep(5)
            with lock:
                for backend in self.backends:
                    process = self.processes.get(backend)
                    if process is None or process.poll() is not None:
                        log(f"{backend} flow worker exited ({process.returncode if process else '-'}); restarting")
                        self._start(backend)

    def stop(self) -> None:
        self.stopping = True
        for backend in list(self.processes):
            self._stop(backend)


def snapshot() -> dict[str, float]:
    return {p.name: p.stat().st_mtime for p in FLOWS_DIR.glob("*.yaml")}


def sync(supervisor: Supervisor, trigger: bool) -> None:
    if trigger:
        changed = trigger_codegen.generate()
        log("regenerated Trigger.dev flows" if changed else "Trigger.dev flows unchanged")
    supervisor.restart()


def watch_files(supervisor: Supervisor, trigger: bool) -> None:
    """Also picks up flows edited by hand, not only those saved through the API."""
    seen = snapshot()
    while not supervisor.stopping:
        time.sleep(2)
        current = snapshot()
        if current != seen:
            seen = current
            log("flows changed; re-registering")
            with lock:
                sync(supervisor, trigger)


def flow_summary(name: str, text: str) -> dict:
    flow = parse_text(text)
    return {**flow.to_json(), "yaml": text}


class Api(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002 - quiet access log
        pass

    def _send(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _authorized(self) -> bool:
        token = os.environ.get("FLOW_HOST_TOKEN", "")
        if not token:
            return True
        given = self.headers.get("Authorization", "").removeprefix("Bearer ")
        if hmac.compare_digest(given.encode(), token.encode()):
            return True
        self._send(401, {"error": "unauthorized"})
        return False

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length > 256 * 1024:
            raise ValueError("request too large")
        body = json.loads(self.rfile.read(length) or b"{}")
        if not isinstance(body, dict) or not isinstance(body.get("yaml"), str):
            raise ValueError("body must be {\"yaml\": \"...\"}")
        return body

    def _name(self) -> str:
        name = self.path.split("/", 2)[2] if self.path.count("/") >= 2 else ""
        if not FLOW_NAME.match(name):
            raise ValueError("invalid flow name")
        return name

    def do_GET(self) -> None:  # noqa: N802
        if not self._authorized():
            return
        if self.path != "/flows":
            return self._send(404, {"error": "not found"})
        flows, broken = load_all()
        items = [flow_summary(n, (FLOWS_DIR / f"{n}.yaml").read_text(encoding="utf-8")) for n in flows]
        self._send(200, {"flows": items, "broken": broken})

    def do_POST(self) -> None:  # noqa: N802
        if not self._authorized():
            return
        match = re.fullmatch(r'/runs/(prefect|trigger-dev)/([A-Za-z0-9_-]+)/nodes/([A-Za-z0-9_-]+)/restart', self.path)
        if match:
            from flowkit.restart_service import restart_node
            try:
                self._send(200, asyncio.run(restart_node(*match.groups())))
            except ValueError as exc:
                self._send(400, {'error': str(exc)})
            except Exception as exc:
                log(f'restart failed: {type(exc).__name__}: {exc}')
                self._send(502, {'error': str(exc)})
            return
        if self.path != "/validate":
            return self._send(404, {"error": "not found"})
        try:
            text = self._body()["yaml"]
            self._send(200, {"ok": True, "flow": flow_summary("", text)})
        except FlowError as exc:
            self._send(200, {"ok": False, "error": str(exc)})
        except ValueError as exc:
            self._send(400, {"error": str(exc)})

    def do_PUT(self) -> None:  # noqa: N802
        if not self._authorized():
            return
        try:
            name, text = self._name(), self._body()["yaml"]
            flow = parse_text(text)
            if flow.name != name:
                raise FlowError(f"the YAML's name is {flow.name!r}, but it is being saved as {name!r}")
            with lock:
                (FLOWS_DIR / f"{name}.yaml").write_text(text, encoding="utf-8", newline="\n")
            log(f"saved {name}")
            self._send(200, {"ok": True, "flow": flow_summary(name, text)})
        except (FlowError, ValueError) as exc:
            self._send(400, {"error": str(exc)})

    def do_DELETE(self) -> None:  # noqa: N802
        if not self._authorized():
            return
        try:
            path = FLOWS_DIR / f"{self._name()}.yaml"
        except ValueError as exc:
            return self._send(400, {"error": str(exc)})
        if not path.exists():
            return self._send(404, {"error": "no such flow"})
        with lock:
            path.unlink()
        log(f"deleted {path.stem}")
        self._send(200, {"ok": True})


def main() -> None:
    backends = [b.strip() for b in os.environ.get("FLOW_BACKENDS", "hatchet,prefect,trigger").split(",") if b.strip()]
    supervisor = Supervisor(backends)
    with lock:
        sync(supervisor, "trigger" in backends)
    threading.Thread(target=supervisor.watch, daemon=True).start()
    threading.Thread(target=watch_files, args=(supervisor, "trigger" in backends), daemon=True).start()
    bind, port = os.environ.get("FLOW_HOST_BIND", "127.0.0.1"), int(os.environ.get("FLOW_HOST_PORT", "8095"))
    server = ThreadingHTTPServer((bind, port), Api)
    log(f"API on http://{bind}:{port} - flows in {FLOWS_DIR} - backends {', '.join(backends)}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        supervisor.stop()


if __name__ == "__main__":
    main()
