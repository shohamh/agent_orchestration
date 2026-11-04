"""Flow host: owns the flow drafts and active snapshots, runs the native flow workers, serves the board's API.

    agents/.venv/Scripts/python.exe -u agents/flow_host.py

- Drafts are flows/<name>.yaml (saved through the API or edited by hand). Saving a draft never
  restarts a worker or regenerates Trigger.dev code; a hand edit simply shows as a changed draft.
- Activation (POST /documents/<name>/activate) copies a draft into an immutable active snapshot
  (FLOW_STATE_DIR, default <repo>/.flow-state; see flowkit/activation.py) and only then applies it:
  restarts the Hatchet and Prefect flow workers (flow_worker_hatchet.py / flow_worker_prefect.py;
  steps running on them are interrupted) and regenerates
  trigger.dev/example-project/src/trigger/flows.generated.ts (the `trigger dev` worker reloads it
  on its own, interrupting its runs). The board asks for confirmation first, from the active runs
  it can see on every engine. An engine that fails keeps its previous snapshot; retrying is safe.
- First start without a state folder: every valid draft is activated (workers start then anyway).
- Program steps' {flow_dir} is still the repository's flows/ folder (their helper scripts).
- HTTP API for the board, on 127.0.0.1:8095 by default (Docker Desktop forwards the board's
  host.docker.internal to host loopback, so nothing is exposed to the network):
    GET    /flows                -> {"flows": [active {name, description, input, steps, yaml, revision}], "broken": {draft file: error}}
    POST   /validate  {yaml}     -> {"ok": true, "flow": {...}} | {"ok": false, "error": "..."}
    PUT    /flows/<name> {yaml}  -> save a draft (the YAML's name must match); nothing is activated
    DELETE /flows/<name>         -> delete a draft; the active version stays until a removal is activated
    GET    /documents[/<name>], POST /documents/parse|serialize, PUT /documents/<name>[/layout],
    DELETE /documents/<name>?revision=, GET /documents/<name>/activation, POST /documents/<name>/activate
                                 -> the workflow builder contract, docs/superpowers/specs/2026-11-03-workflow-builder-api.md
  With FLOW_HOST_TOKEN set, requests need `Authorization: Bearer <token>`. Writes (POST/PUT/DELETE)
  need `Content-Type: application/json`.

Env: FLOW_HOST_BIND (127.0.0.1), FLOW_HOST_PORT (8095), FLOW_HOST_TOKEN, FLOW_BACKENDS
(hatchet,prefect,trigger: which to keep in sync), FLOW_STATE_DIR, FLOW_ACTIVATION_READY_SECONDS (5:
how long a restarted worker must stay up to count as active).
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
from urllib.parse import parse_qs, urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from flowkit import document, trigger_codegen  # noqa: E402
from flowkit.activation import ENGINES, Store  # noqa: E402
from flowkit.definition import FLOW_NAME, FlowError, parse_text  # noqa: E402

WORKERS = {"hatchet": "flow_worker_hatchet.py", "prefect": "flow_worker_prefect.py", 'execution': 'flow_execution_worker.py'}
# `lock` guards drafts and layouts (held briefly); `activation_lock` serializes activations and worker
# restarts, which take seconds, so saving a draft never waits for a restart.
lock = threading.RLock()
activation_lock = threading.RLock()


def log(text: str) -> None:
    print(f"[flow-host] {text}", flush=True)


class Supervisor:
    """Keeps one process per backend worker running. A flow worker is started with the exact
    active revisions it serves (FLOW_ACTIVE_REVISIONS), so its flow-run subprocesses agree with it."""

    def __init__(self, backends: list[str], store: Store | None = None) -> None:
        self.backends = [b for b in backends if b in WORKERS]
        self.store = store
        self.processes: dict[str, subprocess.Popen] = {}
        self.stopping = False

    def _start(self, backend: str) -> None:
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        env = {**os.environ, "PYTHONUTF8": "1"}
        if self.store is not None and backend in ENGINES:
            env.update(FLOW_STATE_DIR=str(self.store.state), FLOW_ACTIVE_REVISIONS=json.dumps(self.store.active_revisions(backend)))
        self.processes[backend] = subprocess.Popen(
            [sys.executable, "-u", str(HERE / WORKERS[backend])], cwd=HERE.parent, creationflags=flags,
            start_new_session=os.name != "nt", env=env)
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

    def running(self, backend: str) -> bool:
        process = self.processes.get(backend)
        return process is not None and process.poll() is None

    def restart_many(self, backends: list[str], wait: float) -> dict[str, str | None]:
        """Restart workers together (so the interruption is one moment, right after the board's
        last check), then wait; per worker, an error message if it did not stay up for `wait` s."""
        errors: dict[str, str | None] = {}
        for backend in backends:
            self._stop(backend)
        for backend in backends:
            try:
                self._start(backend)
            except OSError as exc:
                errors[backend] = f"could not start the {backend} flow worker: {exc}"
        deadline = time.monotonic() + wait
        waiting = [b for b in backends if b not in errors]
        while waiting:
            for backend in list(waiting):
                code = self.processes[backend].poll()
                if code is not None:
                    errors[backend] = f"the {backend} flow worker exited with code {code}; see the flow host log"
                    waiting.remove(backend)
            if not waiting or time.monotonic() >= deadline:
                break
            time.sleep(0.1)
        return {backend: errors.get(backend) for backend in backends}

    def restart_one(self, backend: str, wait: float) -> str | None:
        return self.restart_many([backend], wait)[backend]

    def watch(self) -> None:
        """Restart a worker that exits on its own (after a short pause, so a crash loop stays slow)."""
        while not self.stopping:
            time.sleep(5)
            if not activation_lock.acquire(blocking=False):
                continue  # an activation is restarting workers right now
            try:
                for backend in self.backends:
                    process = self.processes.get(backend)
                    if process is None or process.poll() is not None:
                        log(f"{backend} flow worker exited ({process.returncode if process else '-'}); restarting")
                        self._start(backend)
            except OSError as exc:
                log(f"could not restart a flow worker: {exc}")
            finally:
                activation_lock.release()

    def stop(self) -> None:
        self.stopping = True
        for backend in list(self.processes):
            self._stop(backend)


class HostError(Exception):
    def __init__(self, status: int, body: dict) -> None:
        super().__init__(body.get("error") or body)
        self.status, self.body = status, body


def missing() -> HostError:
    return HostError(404, {"error": "no such flow"})


def invalid(errors: list[dict]) -> HostError:
    return HostError(400, {"error": errors[0]["message"], "errors": errors})


class Host:
    """Drafts, documents and activation; writes are atomic on disk (see the locks above)."""

    def __init__(self, store: Store, supervisor: Supervisor, backends: list[str], ready_seconds: float | None = None,
                 trigger_target: Path | None = None) -> None:
        self.store, self.supervisor = store, supervisor
        self.enabled = {engine: engine in backends for engine in ENGINES}
        self.ready_seconds = float(os.environ.get("FLOW_ACTIVATION_READY_SECONDS", "5")) if ready_seconds is None else ready_seconds
        self.trigger_target = trigger_target or trigger_codegen.TARGET

    def startup(self) -> None:
        with activation_lock:
            migrated = self.store.migrate()
            if migrated:
                log(f"first start: activated {', '.join(migrated)} (snapshots in {self.store.state})")
            if self.enabled["trigger"] and trigger_codegen.render(self.store.load_active("trigger")[0]) != trigger_codegen.current(self.trigger_target):
                log("the generated Trigger.dev tasks differ from the active flows; the next activation regenerates them")
                manifest = self.store.manifest()
                for entry in manifest["flows"].values():
                    if entry.get("engines", {}).get("trigger"):
                        entry["engines"]["trigger"].update(state="pending", error="the generated Trigger.dev tasks are out of date")
                self.store.write_manifest(manifest)
            self.supervisor.restart()

    # Drafts and documents
    @staticmethod
    def analyze(name: str, draft) -> tuple[dict | None, list[dict], bool]:
        if draft.error:
            return None, [{"message": draft.error, "step": None, "field": None}], False
        return document.analyze(draft.text, name)[:3]

    def doc(self, name: str, manifest: dict | None = None) -> dict | None:
        manifest = manifest or self.store.manifest()
        entry, draft = manifest["flows"].get(name), self.store.draft(name)
        active = (entry or {}).get("revision")
        if draft is None:
            served = [e["revision"] for e in (entry or {}).get("engines", {}).values() if e and e.get("revision")]
            if not active and not served:
                return None
            try:
                text = self.store.snapshot_text(name, active or served[0])
            except (OSError, ValueError, FlowError) as exc:  # one damaged snapshot must not hide the other flows
                return {"name": name, "revision": None, "yaml": "", "document": None, "lossless": False, "layout": None,
                        "errors": [{"message": f"the active snapshot cannot be read: {exc}", "step": None, "field": None}],
                        "state": "broken", "activeRevision": active}
            parsed, errors, lossless = document.analyze(text, name)[:3]
        else:
            text = draft.text
            parsed, errors, lossless = self.analyze(name, draft)
        state = ("removed" if draft is None else "broken" if errors else "inactive" if not active
                 else "active" if draft.revision == active else "changed")
        return {"name": name, "revision": draft.revision if draft else None, "yaml": text, "document": parsed, "errors": errors,
                "lossless": lossless, "layout": self.store.layout(name) if draft else None, "state": state,
                "activeRevision": active}

    def documents(self) -> list[dict]:
        manifest = self.store.manifest()
        names = sorted(set(self.store.draft_names()) | set(manifest["flows"]))
        return [doc for doc in (self.doc(name, manifest) for name in names) if doc]

    def save(self, name: str, text: str, expected: str | None) -> dict:
        errors = document.analyze(text, name)[1]  # outside the lock: parsing may take a while
        with lock:
            current = self.store.draft(name)
            if expected != (current.revision if current else None):
                raise HostError(409, {"error": "conflict", "current": self.doc(name)})
            if errors:
                raise invalid(errors)
            self.store.write_draft(name, text)
            log(f"saved draft {name}")
        return self.doc(name)

    def save_layout(self, name: str, layout: dict) -> dict:
        with lock:
            if self.store.draft(name) is None:
                raise missing()
            self.store.write_layout(name, layout)
        return {"ok": True}

    def delete(self, name: str, rev: str | None) -> dict:
        with lock:
            current = self.store.draft(name)
            if current is None:
                raise missing()
            if rev != current.revision:
                raise HostError(409, {"error": "conflict", "current": self.doc(name)})
            self.store.delete_draft(name)
            log(f"deleted draft {name}")
        return {"ok": True}

    def active_flows(self) -> list[dict]:
        manifest = self.store.manifest()
        revisions = self.store.active_revisions(manifest=manifest)
        flows, _ = self.store.load(revisions)
        return [{**flow.to_json(), "environment": flow.environment, "revision": revisions[name],
                 "yaml": self.store.snapshot_text(name, revisions[name])} for name, flow in flows.items()]

    def broken_drafts(self) -> dict[str, str]:
        broken = {}
        for name in self.store.draft_names():
            draft = self.store.draft(name)
            errors = self.analyze(name, draft)[1] if draft else []
            if errors:
                broken[f"{name}.yaml"] = "\n".join(e["message"] for e in errors)
        return broken

    def served(self, manifest: dict) -> dict[str, dict]:
        """Every flow some engine still serves (also after a removal that failed on one engine)."""
        result = {}
        for name, entry in manifest["flows"].items():
            for rev in filter(None, [entry.get("revision"), *((e or {}).get("revision") for e in entry.get("engines", {}).values())]):
                flow = self.store.load({name: rev})[0].get(name)
                if flow:
                    result[name] = flow.to_json()
                    break
        return result

    # Activation
    def _trigger_flows(self, name: str, target: str | None, manifest: dict, text: str | None = None) -> dict:
        revisions = self.store.active_revisions("trigger", manifest)
        revisions.pop(name, None)
        flows, _ = self.store.load(revisions)
        if target:
            if text is None:
                loaded, broken = self.store.load({name: target})
                if name not in loaded:
                    raise FlowError(f"the active snapshot of {name} cannot be read: {broken.get(name)}")
                flows[name] = loaded[name]
            else:
                flows[name] = parse_text(text)
        return dict(sorted(flows.items()))

    def preview(self, name: str) -> dict:
        """Which engines an activation of the current draft (or of its removal) would change."""
        manifest = self.store.manifest()
        entry, draft = manifest["flows"].get(name), self.store.draft(name)
        if draft is None and not entry:
            raise missing()
        target = draft.revision if draft else None
        errors = self.analyze(name, draft)[1] if draft else []
        active = (entry or {}).get("revision")
        change = "remove" if draft is None else "add" if not active else "update" if target != active else "none"
        engines = []
        for engine in ENGINES:
            state = ((entry or {}).get("engines") or {}).get(engine) or {}
            action = "none"
            if self.enabled[engine] and not errors:
                if engine == "trigger":
                    try:
                        code = trigger_codegen.render(self._trigger_flows(name, target, manifest, draft.text if draft else None))
                    except FlowError:
                        code = None
                    action = "regenerate tasks" if code != trigger_codegen.current(self.trigger_target) else "none"
                elif state.get("revision") != target or state.get("state") != "active":
                    action = "reload worker"
            engines.append({"engine": engine, "action": action, "enabled": self.enabled[engine],
                            "state": state.get("state"), "error": state.get("error")})
        return {"name": name, "revision": target, "activeRevision": active, "change": change, "engines": engines,
                "errors": errors, "served": self.served(manifest)}

    def activate(self, name: str, rev: str | None) -> dict:
        with activation_lock:
            draft = self.store.draft(name)
            errors = self.analyze(name, draft)[1] if draft else []
            with lock:  # check and snapshot the draft; restarts happen after the draft lock is released
                manifest = self.store.manifest()
                current = self.store.draft(name)
                if current is None and name not in manifest["flows"]:
                    raise missing()
                target = current.revision if current else None
                if rev != target:
                    raise HostError(409, {"error": "conflict", "preview": self.preview(name)})
                if current and (draft is None or current.revision != draft.revision):
                    draft, errors = current, self.analyze(name, current)[1]
                if errors:
                    raise invalid(errors)
                if current and self.store.write_snapshot(name, current.text) != target:
                    raise invalid([{"message": "the draft could not be stored exactly; save it as UTF-8 text", "step": None, "field": None}])
            entry = manifest["flows"].setdefault(name, {"revision": None, "engines": {}})
            entry["revision"] = target
            self.store.write_manifest(manifest)
            results = self._apply(name, target, manifest)
            if target is None and not any((e or {}).get("revision") for e in entry["engines"].values()):
                del manifest["flows"][name]
                self.store.write_manifest(manifest)
            log(f"activated {name} {target or '(removal)'}: " + ", ".join(f"{r['engine']} {r['state']}" for r in results))
            return {"name": name, "activeRevision": target, "engines": results}

    def _apply(self, name: str, target: str | None, manifest: dict) -> list[dict]:
        """Trigger.dev first (a file write), then every worker restart at once, then wait for them."""
        engines = manifest["flows"][name].setdefault("engines", {})
        old = {e: engines.get(e) or {"revision": None, "state": None, "error": None} for e in ENGINES}
        for engine in ENGINES:
            if not self.enabled[engine]:
                # Nothing runs there; it starts with the active versions if it is enabled later.
                engines[engine] = {"revision": target, "state": "unavailable", "error": f"{engine} is not enabled on the flow host (FLOW_BACKENDS)"}
        if self.enabled["trigger"]:
            engines["trigger"] = self._apply_trigger(name, target, old["trigger"], manifest)
        self.store.write_manifest(manifest)
        restart = [e for e in ("hatchet", "prefect") if self.enabled[e] and not (
            old[e].get("revision") == target and old[e].get("state") == "active" and self.supervisor.running(e))]
        for engine in restart:
            engines[engine] = {"revision": target, "state": "pending", "error": None}
        self.store.write_manifest(manifest)
        try:
            outcome = self.supervisor.restart_many(restart, self.ready_seconds) if restart else {}
        except Exception as exc:  # report it per engine rather than leave the manifest half-applied
            outcome = {engine: f"restarting the {engine} flow worker failed: {type(exc).__name__}: {exc}" for engine in restart}
        failed = [engine for engine in restart if outcome.get(engine)]
        for engine in restart:
            engines[engine] = ({**old[engine], "state": "failed", "error": outcome[engine]} if engine in failed
                               else {"revision": target, "state": "active", "error": None})
        self.store.write_manifest(manifest)
        if failed:
            log(f"{', '.join(failed)}: keeping the previous version of {name}")
            try:
                self.supervisor.restart_many(failed, 0)  # back on the previous snapshot
            except Exception as exc:
                log(f"could not restart {', '.join(failed)} on the previous version: {exc}")
        return [{"engine": e, "state": engines[e]["state"], "error": engines[e]["error"]} for e in ENGINES]

    def _apply_trigger(self, name: str, target: str | None, old: dict, manifest: dict) -> dict:
        try:
            changed = trigger_codegen.generate(self._trigger_flows(name, target, manifest), self.trigger_target)
        except Exception as exc:
            return {**old, "state": "failed", "error": f"could not regenerate the Trigger.dev tasks: {type(exc).__name__}: {exc}"}
        # The `trigger dev` worker rebuilds on its own; the flow host cannot confirm it, so a change stays pending.
        unchanged_state = old.get("state") if old.get("revision") == target and old.get("state") in ("active", "pending") else "active"
        return {"revision": target, "state": "pending" if changed else unchanged_state, "error": None}


def watch_files(store: Store, stopping: threading.Event) -> None:
    """Hand edits become drafts: logged here, activated only on request."""
    def snapshot() -> dict[str, float]:
        return {p.name: p.stat().st_mtime for p in store.drafts.glob("*.yaml")}
    seen = snapshot()
    while not stopping.is_set():
        time.sleep(2)
        current = snapshot()
        if current != seen:
            log(f"drafts changed: {', '.join(sorted(n for n in set(current) | set(seen) if current.get(n) != seen.get(n)))} (activate to apply)")
            seen = current


def flow_summary(name: str, text: str) -> dict:
    flow = parse_text(text)
    return {**flow.to_json(), 'environment': flow.environment, "yaml": text}


class Api(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002 - quiet access log
        pass

    @property
    def host(self) -> Host:
        return self.server.host

    def _send(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode()  # ASCII escapes: YAML text may hold lone surrogates
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

    def _allowed(self) -> bool:
        """Authorized, and for writes a JSON body: a cross-site form or text/plain request cannot send that."""
        if not self._authorized():
            return False
        if self.command != "GET" and self.headers.get_content_type() != "application/json":
            self._send(415, {"error": "requests must have Content-Type: application/json"})
            return False
        return True

    def _json_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length > 1024 * 1024:
            raise ValueError("request too large")
        body = json.loads(self.rfile.read(length) or b"{}")
        if not isinstance(body, dict): raise ValueError('body must be an object')
        return body

    def _body(self) -> dict:
        body = self._json_body()
        if not isinstance(body.get("yaml"), str):
            raise ValueError("body must be {\"yaml\": \"...\"}")
        return body

    def _name(self) -> str:
        name = self.path.split("/", 2)[2] if self.path.count("/") >= 2 else ""
        if not FLOW_NAME.match(name):
            raise ValueError("invalid flow name")
        return name

    def _documents(self, method: str) -> bool:
        """The workflow builder routes (/documents...); False when the path is not one of them."""
        url = urlsplit(self.path)
        match = re.fullmatch(r"/documents(?:/([^/]+)(?:/(layout|activation|activate))?)?", url.path)
        if not match:
            return False
        name, action = match.groups()
        try:
            if name in ("parse", "serialize") and method == "POST" and not action:
                body = self._json_body()
                if name == "parse":
                    if not isinstance(body.get("yaml"), str): raise ValueError('body must be {"yaml": "..."}')
                    return self._send(200, document.parse(body["yaml"])) or True
                errors = document.validate(body.get("document"))
                text = document.serialize(body["document"]) if isinstance(body.get("document"), dict) else ""
                return self._send(200, {"yaml": text, "errors": errors}) or True
            if name is None:
                if method != "GET": raise HostError(405, {"error": "method not allowed"})
                return self._send(200, {"documents": self.host.documents()}) or True
            if not FLOW_NAME.match(name):
                raise ValueError("invalid flow name")
            route = (method, action)
            if route == ("GET", None):
                doc = self.host.doc(name)
                if doc is None: raise missing()
                result = doc
            elif route == ("PUT", None):
                body = self._body()
                expected = body.get("expectedRevision")
                if expected is not None and not isinstance(expected, str): raise ValueError("expectedRevision must be a revision or null")
                result = self.host.save(name, body["yaml"], expected)
            elif route == ("PUT", "layout"):
                result = self.host.save_layout(name, self._json_body().get("layout"))
            elif route == ("DELETE", None):
                result = self.host.delete(name, (parse_qs(url.query).get("revision") or [None])[0])
            elif route == ("GET", "activation"):
                result = self.host.preview(name)
            elif route == ("POST", "activate"):
                body = self._json_body()
                if "revision" not in body or not (body["revision"] is None or isinstance(body["revision"], str)):
                    raise ValueError("revision is required (null to activate a removal)")
                result = self.host.activate(name, body["revision"])
            else:
                raise HostError(405, {"error": "method not allowed"})
            self._send(200, result)
        except HostError as exc:
            self._send(exc.status, exc.body)
        except (ValueError, FlowError) as exc:
            self._send(400, {"error": str(exc)})
        except OSError as exc:
            log(f"{method} {url.path} failed: {type(exc).__name__}: {exc}")
            self._send(500, {"error": f"flow host storage error: {exc}"})
        return True

    def do_GET(self) -> None:  # noqa: N802
        if not self._allowed() or self._documents("GET"):
            return
        if self.path == '/workers':
            from flowkit.workers import catalog
            try: return self._send(200, {'workers': asyncio.run(catalog())})
            except ValueError as exc: return self._send(400, {'error': str(exc)})
        match = re.fullmatch(r'/execution/([A-Za-z0-9_-]+)', self.path)
        if match:
            from flowkit.distributed import execution_info
            return self._send(200, {'steps': execution_info(match[1])})
        if self.path != "/flows":
            return self._send(404, {"error": "not found"})
        try:
            self._send(200, {"flows": self.host.active_flows(), "broken": self.host.broken_drafts()})
        except (OSError, ValueError) as exc:
            self._send(500, {"error": str(exc)})

    def do_POST(self) -> None:  # noqa: N802
        if not self._allowed() or self._documents("POST"):
            return
        if self.path == '/workers':
            from flowkit.fleet_registry import save_worker
            try: return self._send(200, save_worker(self._json_body()))
            except ValueError as exc: return self._send(400, {'error': str(exc)})
        if self.path == '/workers/migrate':
            from flowkit.fleet_registry import migrate_legacy_registry
            try: return self._send(200, migrate_legacy_registry())
            except ValueError as exc: return self._send(400, {'error': str(exc)})
        if self.path == '/workers/validate':
            from flowkit.workers import routing
            try:
                body = self._json_body()
                return self._send(200, routing(body['execution'], body['steps']))
            except (ValueError, KeyError) as exc: return self._send(400, {'error': str(exc)})
        match = re.fullmatch(r'/execution/([A-Za-z0-9_-]+)/selection', self.path)
        if match:
            from flowkit.distributed import set_selection
            from flowkit.workers import choose
            try:
                body = self._json_body()
                asyncio.run(choose(body['worker']))
                set_selection(match[1], body['step'], body['worker'])
                return self._send(200, {'ok': True})
            except (ValueError, KeyError) as exc: return self._send(400, {'error': str(exc)})
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
        if not self._allowed() or self._documents("PUT"):
            return
        match = re.fullmatch(r'/workers/([A-Za-z0-9_-]+)', self.path)
        if match:
            from flowkit.fleet_registry import save_worker
            try:
                body = self._json_body()
                if 'id' in body and body['id'] != match[1]: raise ValueError('Worker id mismatch')
                return self._send(200, save_worker({**body, 'id': match[1]}))
            except ValueError as exc: return self._send(400, {'error': str(exc)})
        try:  # the original save: a draft only; checks the revision only when the client sends one
            name, body = self._name(), self._body()
            text = body["yaml"]
            errors = document.analyze(text, name)[1]
            if errors:
                raise FlowError("\n".join(e["message"] for e in errors))
            with lock:
                current = self.host.store.draft(name)
                if "expectedRevision" in body and body["expectedRevision"] != (current.revision if current else None):
                    return self._send(409, {"error": "conflict", "current": self.host.doc(name)})
                self.host.store.write_draft(name, text)
            log(f"saved draft {name}")
            self._send(200, {"ok": True, "flow": flow_summary(name, text)})
        except (FlowError, ValueError) as exc:
            self._send(400, {"error": str(exc)})

    def do_DELETE(self) -> None:  # noqa: N802
        if not self._allowed() or self._documents("DELETE"):
            return
        match = re.fullmatch(r'/workers/([A-Za-z0-9_-]+)', self.path)
        if match:
            from flowkit.fleet_registry import delete_worker
            try:
                delete_worker(match[1])
                return self._send(200, {'ok': True})
            except ValueError as exc: return self._send(400, {'error': str(exc)})
        try:
            name = self._name()
        except ValueError as exc:
            return self._send(400, {"error": str(exc)})
        with lock:
            if self.host.store.draft(name) is None:
                return self._send(404, {"error": "no such flow"})
            self.host.store.delete_draft(name)
        log(f"deleted draft {name}")
        self._send(200, {"ok": True})


def serve(host: Host, bind: str, port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((bind, port), Api)
    server.host = host
    return server


def main() -> None:
    backends = [b.strip() for b in os.environ.get("FLOW_BACKENDS", "hatchet,prefect,trigger").split(",") if b.strip()]
    store = Store()
    supervisor = Supervisor(backends, store)
    host = Host(store, supervisor, backends)
    bind, port = os.environ.get("FLOW_HOST_BIND", "127.0.0.1"), int(os.environ.get("FLOW_HOST_PORT", "8095"))
    if bind not in ('127.0.0.1','localhost','::1') and not os.environ.get('FLOW_HOST_TOKEN'):
        raise SystemExit('FLOW_HOST_TOKEN is required for a network flow host')
    execution = Supervisor(['execution'] if os.environ.get('FLOW_EXECUTION_SERVICE', '1') != '0' else [])
    execution.restart()
    threading.Thread(target=execution.watch, daemon=True).start()
    host.startup()
    threading.Thread(target=supervisor.watch, daemon=True).start()
    stopping = threading.Event()
    threading.Thread(target=watch_files, args=(store, stopping), daemon=True).start()
    server = serve(host, bind, port)
    log(f"API on http://{bind}:{port} - drafts in {store.drafts} - active snapshots in {store.state} - backends {', '.join(backends)}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stopping.set()
        supervisor.stop()
        execution.stop()


if __name__ == "__main__":
    main()
