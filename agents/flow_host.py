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
    DELETE /documents/<name>?revision=, GET /documents/<name>/activation[?disable=1], POST /documents/<name>/activate
    POST /documents/<name>/deactivate {revision} -> stop serving the active version; the draft stays, so it can be enabled again
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
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from flowkit import document, trigger_codegen  # noqa: E402
from flowkit.activation import ENGINES, Store  # noqa: E402
from flowkit.definition import FLOW_NAME, FlowError, parse_text  # noqa: E402

WORKERS = {"hatchet": "flow_worker_hatchet.py", "prefect": "flow_worker_prefect.py", 'execution': 'flow_execution_worker.py', 'relay': 'fleet_relay.py'}
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
                # An already-snapshotted revision must render with its immutable asset
                # reference in the preview, just as it does during activation.
                loaded, _ = self.store.load({name: target})
                flows[name] = loaded.get(name) or parse_text(text)
        return dict(sorted(flows.items()))

    def preview(self, name: str, disable: bool = False) -> dict:
        """Which engines an activation of the current draft (or of its removal, or of disabling it) would change."""
        manifest = self.store.manifest()
        entry, draft = manifest["flows"].get(name), self.store.draft(name)
        if draft is None and not entry:
            raise missing()
        target = draft.revision if draft and not disable else None
        errors = self.analyze(name, draft)[1] if draft and not disable else []
        active = (entry or {}).get("revision")
        if draft and not errors and not disable:
            try:
                _, asset_digest = self.store.current_assets()
                ref_path = self.store.asset_manifest_path(name, target).with_suffix('.assetref')
                if ref_path.is_file() and self.store.asset_reference(name, target)['digest'] != asset_digest:
                    raise FlowError('Flow assets have changed for this revision; change the flow YAML before activating')
            except (FlowError, OSError, ValueError) as exc:
                errors.append({'message': str(exc), 'step': None, 'field': 'assets'})
        if disable:
            change = "remove" if active else "none"
        else:
            change = "remove" if draft is None else "add" if not active else "update" if target != active else "none"
        assets_missing = bool(target and not self.store.asset_manifest_path(name, target).with_suffix('.assetref').is_file())
        engines = []
        for engine in ENGINES:
            state = ((entry or {}).get("engines") or {}).get(engine) or {}
            action = "none"
            if self.enabled[engine] and not errors and not (disable and change == "none"):
                if engine == "trigger":
                    try:
                        code = trigger_codegen.render(self._trigger_flows(name, target, manifest, draft.text if draft and not disable else None))
                    except FlowError:
                        code = None
                    action = "regenerate tasks" if code != trigger_codegen.current(self.trigger_target) else "none"
                elif assets_missing or state.get("revision") != target or state.get("state") != "active":
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
                assets_missing = bool(target and not self.store.asset_manifest_path(name, target).with_suffix('.assetref').is_file())
                if current and self.store.write_snapshot(name, current.text) != target:
                    raise invalid([{"message": "the draft could not be stored exactly; save it as UTF-8 text", "step": None, "field": None}])
            entry = manifest["flows"].setdefault(name, {"revision": None, "engines": {}})
            entry["revision"] = target
            self.store.write_manifest(manifest)
            results = self._apply(name, target, manifest, force_reload=assets_missing)
            if target is None and not any((e or {}).get("revision") for e in entry["engines"].values()):
                del manifest["flows"][name]
                self.store.write_manifest(manifest)
            log(f"activated {name} {target or '(removal)'}: " + ", ".join(f"{r['engine']} {r['state']}" for r in results))
            return {"name": name, "activeRevision": target, "engines": results}

    def deactivate(self, name: str, rev: str | None) -> dict:
        """Stop serving the active version. The draft stays and the flow can be activated again."""
        with activation_lock:
            with lock:
                manifest = self.store.manifest()
                entry = manifest["flows"].get(name)
                if self.store.draft(name) is None and not entry:
                    raise missing()
                if not entry or not entry.get("revision") or rev != entry["revision"]:
                    raise HostError(409, {"error": "conflict", "preview": self.preview(name, disable=True)})
                entry["revision"] = None
                self.store.write_manifest(manifest)
            results = self._apply(name, None, manifest)
            if not any((e or {}).get("revision") for e in entry["engines"].values()):
                del manifest["flows"][name]
                self.store.write_manifest(manifest)
            log(f"disabled {name}: " + ", ".join(f"{r['engine']} {r['state']}" for r in results))
            return {"name": name, "activeRevision": None, "engines": results}

    def _apply(self, name: str, target: str | None, manifest: dict, force_reload: bool = False) -> list[dict]:
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
            not force_reload and old[e].get("revision") == target and old[e].get("state") == "active" and self.supervisor.running(e))]
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


FLEET_FIELDS = {'id', 'pools', 'tags', 'engines', 'slots', 'cpu_limit', 'ram_limit', 'url', 'token_env'}
WORKER_ID = re.compile(r'^[A-Za-z0-9][A-Za-z0-9._-]{0,99}$')


def coordinator_states(host: "Host | None") -> dict[str, str]:
    """Per engine, whether its coordinator worker (which places work on the fleet) is up here."""
    states = {}
    for engine in ENGINES:
        if host is None: states[engine] = 'unknown'
        elif not host.enabled.get(engine): states[engine] = 'not_enabled'
        elif engine == 'trigger': states[engine] = 'unconfirmed'  # `trigger dev` runs outside the flow host
        else: states[engine] = 'running' if host.supervisor.running(engine) else 'stopped'
    return states


def relay_state() -> dict[str, dict]:
    """Per relayed PC (by uuid): online, version, os and update state, as the relay service reports (loopback); {} when it is down."""
    from flowkit.relay import relay_base
    try:
        with urllib.request.urlopen(relay_base().rstrip('/') + '/relay-state', timeout=2) as response:
            return json.loads(response.read())
    except (OSError, ValueError, AttributeError):
        return {}


def relay_online() -> dict[str, bool]:
    """Which relayed PCs have a live connector."""
    return {k: bool(v.get('online')) for k, v in relay_state().items()}


def update_link_for(body: dict) -> dict:
    """A one-time update link for one PC (for a PC that was down while the fleet updated)."""
    from flowkit import fleet_registry, install, update
    name = body.get('worker')
    worker = next((w for w in fleet_registry.load_fleet() if w['id'] == name), None)
    if worker is None:
        raise LookupError(f'Unknown worker {name}')
    live = relay_state().get(worker.get('uuid') or worker['id'], {})
    os_name = body.get('os') or live.get('os')
    if os_name not in install.OSES:
        raise ValueError("Say which operating system the PC runs (windows or linux): it has not reported one yet")
    server = install.server_address(body.get('server'))
    record = update.create_link(name, os_name)
    return {'link': {'code': record['code'], 'id': name, 'os': os_name, 'expires_at': record['expires_at']}, 'commands': update.link_commands(record, server)}


def install_listing() -> list[dict]:
    """Install links for the Fleet tab: pending ones with their commands, finished ones with whether the PC is connected."""
    from flowkit import install
    online = relay_online()
    out = []
    for record in sorted(install.all_records(), key=lambda r: r['created_at'], reverse=True):
        if record['state'] == 'cancelled':
            continue
        view = install.view(record)
        entry = {**view, 'connected': online.get(record['code'], False) if record['state'] == 'registered' else False}
        if view['state'] == 'pending':
            entry['commands'] = install.commands(record)
        out.append(entry)
    return out


def fleet_worker(body: dict, worker_id: str | None) -> dict:
    """A fleet registration or edit: known fields only; new tag and pool names must be plain labels."""
    from flowkit.fleet_registry import label, load_fleet, tag_catalog
    if set(body) - FLEET_FIELDS: raise ValueError('Unknown worker fields: ' + ', '.join(sorted(map(str, set(body) - FLEET_FIELDS))))
    if worker_id is not None:
        if 'id' in body and body['id'] != worker_id: raise ValueError('Worker id mismatch')
        body = {**body, 'id': worker_id}
    elif not isinstance(body.get('id'), str) or not WORKER_ID.match(body['id']):
        raise ValueError('A worker ID is 1 to 100 letters, digits, dots, dashes or underscores (it must equal FLOW_WORKER_ID on the PC)')
    known_tags, known_pools = set(tag_catalog()), {p for w in load_fleet() for p in w['pools']}
    for name in body.get('tags', []) if isinstance(body.get('tags'), list) else []:
        if name not in known_tags: label(name, 'tag')
    for name in body.get('pools', []) if isinstance(body.get('pools'), list) else []:
        if name not in known_pools: label(name, 'pool')
    if body.get('url') == '': body.pop('url')  # a blank endpoint keeps the existing one
    return body


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

    def _json_body(self, limit: int = 1024 * 1024) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length > limit:
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
        match = re.fullmatch(r"/documents(?:/([^/]+)(?:/(layout|activation|activate|deactivate))?)?", url.path)
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
                result = self.host.preview(name, disable=(parse_qs(url.query).get("disable") or [""])[0] == "1")
            elif route == ("POST", "activate"):
                body = self._json_body()
                if "revision" not in body or not (body["revision"] is None or isinstance(body["revision"], str)):
                    raise ValueError("revision is required (null to activate a removal)")
                result = self.host.activate(name, body["revision"])
            elif route == ("POST", "deactivate"):
                body = self._json_body()
                if not isinstance(body.get("revision"), str):
                    raise ValueError("revision is required: the active revision to disable")
                result = self.host.deactivate(name, body["revision"])
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

    def _fleet(self, method: str) -> bool:
        """The fleet management routes (/fleet...) behind the board's fleet tab; False otherwise."""
        path = urlsplit(self.path).path
        if path != '/fleet' and not path.startswith('/fleet/'):
            return False
        from urllib.parse import unquote
        from flowkit import fleet_registry, workers
        try:
            if (method, path) in (('GET', '/fleet'), ('GET', '/fleet/metrics')):
                return self._send(200, asyncio.run(workers.fleet_view(coordinator_states(self.host), inventory=path == '/fleet'))) or True
            if (method, path) == ('GET', '/fleet/tags'):
                return self._send(200, {'tags': fleet_registry.tag_catalog()}) or True
            if (method, path) == ('POST', '/fleet/tags'):
                return self._send(200, {'tags': fleet_registry.add_tag(self._json_body().get('name'))}) or True
            if (method, path) == ('GET', '/fleet/updates'):
                from flowkit import update
                return self._send(200, update.view(relay_state())) or True
            if (method, path) == ('POST', '/fleet/updates'):
                from flowkit import update
                update.start_fleet_update()
                return self._send(200, update.view(relay_state())) or True
            if (method, path) == ('DELETE', '/fleet/updates'):
                from flowkit import update
                update.stop_fleet_update()
                return self._send(200, update.view(relay_state())) or True
            if (method, path) == ('POST', '/fleet/updates/link'):
                return self._send(200, update_link_for(self._json_body())) or True
            if (method, path) == ('GET', '/fleet/installs'):
                return self._send(200, {'installs': install_listing()}) or True
            if (method, path) == ('POST', '/fleet/installs'):
                from flowkit import install
                record = install.create(self._json_body(), registered={w['id'] for w in fleet_registry.load_fleet()})
                return self._send(200, {'install': install.view(record), 'commands': install.commands(record)}) or True
            install_match = re.fullmatch(r'/fleet/installs/([A-Za-z0-9_-]{43})', path)
            if install_match and method == 'DELETE':
                from flowkit import install
                return self._send(200, {'install': install.cancel(install_match[1])}) or True
            if (method, path) == ('POST', '/fleet/workers'):
                saved = fleet_registry.save_worker(fleet_worker(self._json_body(), None), create=True)
                return self._send(200, {'worker': saved, 'settings_sync': asyncio.run(workers.sync_limits(saved['id']))}) or True
            jobs = re.fullmatch(r'/fleet/workers/([^/]+)/jobs', path)
            if jobs and method == 'GET':
                from flowkit.distributed import worker_jobs
                return self._send(200, {'jobs': worker_jobs(unquote(jobs[1], errors='strict'))}) or True
            match = re.fullmatch(r'/fleet/workers/([^/]+)(/capabilities/refresh)?', path)
            if match:
                worker_id = unquote(match[1], errors='strict')
                if worker_id not in {w['id'] for w in fleet_registry.load_fleet()}:
                    return self._send(404, {'error': f'Unknown worker {worker_id}'}) or True
                if method == 'PUT' and not match[2]:
                    saved = fleet_registry.save_worker(fleet_worker(self._json_body(), worker_id))
                    return self._send(200, {'worker': saved, 'settings_sync': asyncio.run(workers.sync_limits(worker_id))}) or True
                if method == 'POST' and match[2]:
                    return self._send(200, {'inventory': asyncio.run(workers.refresh_capabilities(worker_id))}) or True
            self._send(404, {'error': 'not found'})
        except fleet_registry.AlreadyRegistered as exc:
            self._send(409, {'error': str(exc)})
        except LookupError as exc:
            self._send(404, {'error': str(exc)})
        except ConnectionError as exc:
            self._send(409, {'error': str(exc)})
        except ValueError as exc:
            self._send(400, {'error': str(exc)})
        except Exception as exc:  # an unexpected worker answer: never echo connection details
            log(f'{method} {path} failed: {type(exc).__name__}')
            self._send(502, {'error': f'The worker did not answer as expected ({type(exc).__name__})'})
        return True

    def _launch(self, method: str) -> bool:
        """Uploads, attached sources and schedules behind the board's New card form; False otherwise."""
        url = urlsplit(self.path)
        if not re.match(r'/(uploads|schedules|launch/context)(/|$)', url.path):
            return False
        from flowkit import launch_context, schedules
        store = schedules.Schedules(self.host.store.state / 'schedules.json')
        try:
            target = (method, url.path)
            found = re.fullmatch(r'/schedules/([0-9a-f]{32})(?:/(enable|disable|result))?', url.path)
            upload = re.fullmatch(r'/uploads/([0-9a-f]{32})', url.path)
            if target == ('POST', '/launch/context'):
                result = {'context': launch_context.normalize(self._json_body().get('context'))}
            elif target == ('POST', '/uploads'):
                body = self._json_body(limit=90 * 1024 * 1024)  # 64 MiB as base64, the transfer limit of a run
                result = launch_context.save_upload(body.get('name'), body.get('data'), keep=store.referenced_uploads())
            elif method == 'DELETE' and upload:
                launch_context.delete_upload(upload[1])
                result = {'ok': True}
            elif target == ('GET', '/schedules'):
                query = parse_qs(url.query)
                result = {'schedules': store.list(query.get('system', [None])[0], query.get('board', [None])[0])}
            elif target == ('POST', '/schedules'):
                result = store.create(self._json_body(limit=256 * 1024))
            elif target == ('POST', '/schedules/preview'):
                result = {'next': schedules.upcoming(self._json_body())}
            elif target == ('POST', '/schedules/claim'):
                result = {'schedules': store.claim(self._json_body().get('systems') or [])}
            elif method == 'GET' and found and not found[2]:
                result = store.get(found[1])
            elif method == 'DELETE' and found and not found[2]:
                store.delete(found[1])
                result = {'ok': True}
            elif method == 'POST' and found and found[2] in ('enable', 'disable'):
                result = store.set_enabled(found[1], found[2] == 'enable')
            elif method == 'POST' and found and found[2] == 'result':
                body = self._json_body()
                result = store.record(found[1], run=body.get('run'), error=body.get('error'), skipped=body.get('skipped')) or {'ok': True}
            else:
                result = None
            self._send(*((200, result) if result is not None else (404, {'error': 'not found'})))
        except LookupError as exc:
            self._send(404, {'error': str(exc)})
        except ValueError as exc:
            self._send(400, {'error': str(exc)})
        return True

    def do_GET(self) -> None:  # noqa: N802
        if not self._allowed() or self._documents("GET") or self._fleet("GET") or self._launch("GET"):
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
        if not self._allowed() or self._documents("POST") or self._fleet("POST") or self._launch("POST"):
            return
        if self.path == '/workers':
            from flowkit.fleet_registry import save_worker
            from flowkit.workers import sync_limits
            try:
                saved = save_worker(self._json_body())
                return self._send(200, {**saved, 'settings_sync': asyncio.run(sync_limits(saved['id']))})
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
        if self.path == '/launch/validate':
            from flowkit.distributed import validate_card_launch, validate_flow_launch
            from flowkit.definition import parse
            try:
                body = self._json_body()
                engine = body.get('engine')
                if engine is not None and engine not in ENGINES: raise ValueError('Unknown engine')
                card = body.get('card')
                if card is not None:  # a standalone card: {agent, worker}
                    if not isinstance(card, dict) or set(card) - {'agent', 'worker'}: raise ValueError('card must contain agent and worker')
                    return self._send(200, asyncio.run(validate_card_launch(card.get('agent'), card.get('worker'), engine, card.get('command'))))
                source = body.get('flow')
                flow = parse_text(source) if isinstance(source, str) else parse(source)
                return self._send(200, asyncio.run(validate_flow_launch(flow, body.get('execution'), engine)))
            except (ValueError, KeyError, FlowError) as exc: return self._send(400, {'error': str(exc)})
        match = re.fullmatch(r'/execution/([A-Za-z0-9_-]+)/cancel', self.path)
        if match:
            from flowkit.distributed import mark_cancelled
            try: return self._send(200, {'marked': mark_cancelled(match[1])})
            except ValueError as exc: return self._send(400, {'error': str(exc)})
        match = re.fullmatch(r'/execution/([A-Za-z0-9_-]+)/selection', self.path)
        if match:
            from flowkit.distributed import set_selection
            from flowkit.definition import parse
            from flowkit.fleet_matching import requirements_for, validate_launch
            from flowkit.workers import fleet_status, selector
            try:
                body = self._json_body()
                wanted = selector(body['worker'])
                requirements = dict(wanted)
                if body.get('flow') is not None:  # the step's software counts, as at launch
                    source = body['flow']
                    flow = parse_text(source) if isinstance(source, str) else parse(source)
                    if body['step'] not in flow.steps: raise ValueError('Unknown step')
                    requirements = requirements_for(flow, body['step'], selection=wanted)
                # An offline, busy or unchecked match is accepted: the restarted step waits for it.
                decision = validate_launch(requirements, asyncio.run(fleet_status([requirements]))[0])
                if decision['status'] == 'blocked': raise ValueError(f"{decision['message']}. {decision['resolution']}")
                set_selection(match[1], body['step'], wanted)
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
        if not self._allowed() or self._documents("PUT") or self._fleet("PUT"):
            return
        match = re.fullmatch(r'/workers/([^/]+)', urlsplit(self.path).path)
        if match:
            from flowkit.fleet_registry import save_worker
            from flowkit.workers import sync_limits
            from urllib.parse import unquote
            try:
                worker_id = unquote(match[1], errors='strict')
                body = self._json_body()
                if 'id' in body and body['id'] != worker_id: raise ValueError('Worker id mismatch')
                saved = save_worker({**body, 'id': worker_id})
                return self._send(200, {**saved, 'settings_sync': asyncio.run(sync_limits(worker_id))})
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
        if not self._allowed() or self._documents("DELETE") or self._fleet("DELETE") or self._launch("DELETE"):
            return
        match = re.fullmatch(r'/workers/([^/]+)', urlsplit(self.path).path)
        if match:
            from flowkit.fleet_registry import delete_worker
            from urllib.parse import unquote
            try:
                delete_worker(unquote(match[1], errors='strict'))
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
    execution = Supervisor((['execution'] if os.environ.get('FLOW_EXECUTION_SERVICE', '1') != '0' else [])
                           + (['relay'] if os.environ.get('FLOW_RELAY_SERVICE', '1') != '0' else []))
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
