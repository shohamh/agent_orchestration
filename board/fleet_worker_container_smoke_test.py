"""Exercise Fleet installation and native-engine dispatch with the built Rust worker container.

Build worker/ci/Dockerfile.fleet-e2e from the repository root, then run:
  python -u board/fleet_worker_container_smoke_test.py --base http://127.0.0.1:8093 \
      --engine hatchet --image agent-worker-fleet-e2e --server-address http://<runner-ip>:8093
"""
import argparse
import asyncio
import ipaddress
import json
import os
import shlex
import subprocess
import re
import sys
import time
import uuid
from urllib.parse import quote, urlencode, urlsplit

import aiohttp

INSTALL_TIMEOUT = 360
FLOW_TIMEOUT = 300


class HarnessError(RuntimeError):
    """An actionable diagnostic that does not include install credentials or response bodies."""


def origin(value):
    parts = urlsplit(value)
    if (parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password
            or parts.query or parts.fragment or parts.path not in ("", "/")):
        raise HarnessError("addresses must be an http(s) origin without credentials or a path")
    try:
        port = parts.port
    except ValueError:
        raise HarnessError("address has an invalid port") from None
    host = f"[{parts.hostname}]" if ":" in parts.hostname else parts.hostname
    return f"{parts.scheme}://{host}" + (f":{port}" if port else "")


def worker_server_address(base, explicit):
    """Choose a host-reachable, non-loopback address for the install record and container."""
    candidate = explicit or os.environ.get("CI_SERVER_ADDRESS")
    if not candidate:
        host_ip = os.environ.get("CI_HOST_IP", "").strip()
        if host_ip:
            base_parts = urlsplit(base)
            host = f"[{host_ip}]" if ":" in host_ip and not host_ip.startswith("[") else host_ip
            candidate = f"{base_parts.scheme}://{host}" + (f":{base_parts.port}" if base_parts.port else "")
        else:
            try:
                address = ipaddress.ip_address(urlsplit(base).hostname or "")
            except ValueError:
                address = None
            if address is None or not address.is_loopback:
                candidate = base
    if not candidate:
        raise HarnessError("loopback --base requires --server-address or CI_HOST_IP for the worker container")
    address = origin(candidate)
    host = urlsplit(address).hostname or ""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        ip = None
    if host.lower() == "localhost" or host.lower().endswith(".localhost") or ip and (ip.is_loopback or ip.is_unspecified):
        raise HarnessError("--server-address must be reachable from the container and must not be loopback")
    return address


def docker(*args, timeout=60):
    try:
        result = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise HarnessError(f"docker {args[0]} failed ({type(exc).__name__})") from None
    if result.returncode:
        raise HarnessError(f"docker {args[0]} failed (exit {result.returncode})")
    return result.stdout.strip()


async def docker_async(*args, timeout=60):
    return await asyncio.to_thread(docker, *args, timeout=timeout)



def print_container_diagnostics(container, install_code):
    try:
        result = subprocess.run(
            ["docker", "logs", "--tail", "80", container],
            capture_output=True, text=True, timeout=15,
        )
    except (OSError, subprocess.TimeoutExpired):
        return
    text = result.stdout + "\n" + result.stderr
    for component in ("hatchet", "prefect"):
        try:
            logs = subprocess.run(
                ["docker", "exec", container, "tail", "-n", "80",
                 f"/home/node/.agent-worker/component-{component}.log"],
                capture_output=True, text=True, timeout=15,
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        if logs.returncode == 0:
            text += f"\n--- {component} component ---\n{logs.stdout}\n{logs.stderr}"
    if install_code:
        text = text.replace(install_code, "[redacted install code]")
    text = re.sub(
        r"\b[A-Za-z0-9_-]{24,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\b",
        "[redacted JWT]", text,
    )
    text = re.sub(
        r"(?i)(\b(?:[A-Z_]*TOKEN|token)\s*[=:]\s*)[^\s,;]+",
        r"\1[redacted]", text,
    )
    lines = [line for line in text.splitlines()
             if "==>" in line or any(word in line.lower() for word in
                                     ("error", "failed", "refused", "could not", "did not start",
                                      "connect", "unreachable", "unauthorized", "permission"))]
    if lines:
        print("Worker container diagnostics:\n" + "\n".join(lines[-80:]), file=sys.stderr)

async def api(session, base, method, path, body=None):
    async with session.request(method, base + path, json=body) as response:
        try:
            payload = await response.json()
        except (aiohttp.ContentTypeError, json.JSONDecodeError):
            payload = {}
        if response.status >= 400:
            detail = payload.get("error")
            if path == "/api/fleet/installs" and isinstance(detail, str) and detail:
                raise HarnessError(f"Board API returned HTTP {response.status}: {detail[:400]}")
            raise HarnessError(f"Board API returned HTTP {response.status}")
        return payload


async def wait_registered(session, base, install_code, worker_id, engine, container, timeout=INSTALL_TIMEOUT):
    deadline = time.monotonic() + timeout
    last_inspect = 0.0
    while time.monotonic() < deadline:
        data = await api(session, base, "GET", "/api/fleet/installs")
        record = next((item for item in data.get("installs", []) if item.get("code") == install_code), None)
        if record and record.get("state") in ("failed", "expired", "cancelled", "unregistered"):
            print_container_diagnostics(container, install_code)
            raise HarnessError(f"worker install ended in state {record['state']}")
        fleet = await api(session, base, "GET", "/api/fleet")
        worker = next((item for item in fleet.get("workers", []) if item.get("id") == worker_id), None)
        engine_status = (worker or {}).get("engine_status", {}).get(engine, {})
        native_ready = (engine_status.get("route") == "native" and engine_status.get("state") == "connected"
                        and engine_status.get("connected") is True)
        if record and record.get("state") == "registered" and worker and worker.get("online") and native_ready:
            return worker
        if time.monotonic() - last_inspect >= 5:
            try:
                running = await docker_async("inspect", "--format={{.State.Running}}", container, timeout=15)
            except HarnessError:
                running = "unknown"
            if running == "false":
                print_container_diagnostics(container, install_code)
                raise HarnessError("worker container exited before registration completed")
            last_inspect = time.monotonic()
        await asyncio.sleep(2)
    raise HarnessError("timed out waiting for the install and worker to become ready")


async def activate(session, base, flow_name):
    encoded = quote(flow_name, safe="")
    preview = await api(session, base, "GET", f"/api/flow-documents/{encoded}/activation")
    return await api(session, base, "POST", f"/api/flow-documents/{encoded}/activate",
                     {"revision": preview.get("revision"), "impact": preview.get("impact"), "confirm": True})


async def wait_flow_done(session, base, engine, run_id, flow_name):
    task = urlencode({"task": flow_name})
    path = f"/api/cards/{quote(engine, safe='')}/{quote(run_id, safe='')}/dag?{task}"
    deadline = time.monotonic() + FLOW_TIMEOUT
    latest = {"nodes": []}
    while time.monotonic() < deadline:
        latest = await api(session, base, "GET", path)
        nodes = latest.get("nodes", [])
        if any(node.get("column") == "stopped" for node in nodes):
            raise HarnessError("native-engine flow step stopped")
        if nodes and all(node.get("column") == "done" for node in nodes):
            return latest
        await asyncio.sleep(2)
    raise HarnessError("timed out waiting for the native-engine flow DAG")


async def wait_card_done(session, base, engine, board, run_id):
    path = f"/api/boards/{quote(engine, safe='')}/{quote(board, safe='')}/cards"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        cards = (await api(session, base, "GET", path)).get("cards", [])
        card = next((item for item in cards if item.get("id") == run_id), None)
        if card and card.get("column") == "done":
            return
        if card and card.get("column") == "stopped":
            raise HarnessError("native-engine card stopped")
        await asyncio.sleep(1)
    raise HarnessError("timed out waiting for the native-engine card to finish")


async def cleanup_run(session, base, engine, board, run_id):
    if not run_id:
        return
    try:
        path = f"/api/boards/{quote(engine, safe='')}/{quote(board, safe='')}/cards"
        cards = (await api(session, base, "GET", path)).get("cards", [])
        card = next((item for item in cards if item.get("id") == run_id), None)
        if card and card.get("column") in ("done", "stopped"):
            return
    except Exception:
        pass
    try:
        await api(session, base, "POST", f"/api/cards/{quote(engine, safe='')}/{quote(run_id, safe='')}/cancel", {})
    except Exception as exc:
        print(f"cleanup: card cancellation failed ({type(exc).__name__})", file=sys.stderr)


async def cleanup_flow(session, base, flow_name):
    encoded = quote(flow_name, safe="")
    try:
        await api(session, base, "DELETE", f"/api/flows/{encoded}")
        await activate(session, base, flow_name)
    except Exception as exc:
        print(f"cleanup: flow removal failed ({type(exc).__name__})", file=sys.stderr)


async def cleanup_install(session, base, worker_id, install_code):
    if not install_code:
        return
    try:
        fleet = await api(session, base, "GET", "/api/fleet")
        owned = any(item.get("id") == worker_id for item in fleet.get("workers", []))
        if owned:
            await api(session, base, "DELETE", f"/api/fleet/workers/{quote(worker_id, safe='')}")
    except Exception as exc:
        print(f"cleanup: worker unregister failed ({type(exc).__name__})", file=sys.stderr)
    try:
        await api(session, base, "DELETE", f"/api/fleet/installs/{quote(install_code, safe='')}")
    except Exception as exc:
        print(f"cleanup: install cancellation failed ({type(exc).__name__})", file=sys.stderr)


async def run(args):
    base = origin(args.base)
    server = worker_server_address(base, args.server_address)
    nonce = uuid.uuid4().hex
    worker_id = f"fleet-e2e-{nonce[:16]}"
    flow_name = f"fleet-e2e-{nonce}"
    pool = f"fleet-e2e-{nonce[:12]}"
    tag = pool
    marker = uuid.uuid4().hex
    marker_path = f"/tmp/fleet-e2e-{marker}.txt"
    container = f"fleet-worker-e2e-{nonce[:16]}"
    install_code = None
    flow_saved = False
    run_id = None
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30, connect=10)) as session:
        try:
            created = await api(session, base, "POST", "/api/fleet/installs", {
                "id": worker_id, "os": "linux", "engines": [args.engine], "pool": pool, "tags": [tag],
                "slots": 1, "cpu_limit": 100, "ram_limit": 100, "autostart": "none",
                "auto_restart": True, "server": server,
            })
            install = created.get("install", {})
            install_code = install.get("code")
            if not install_code:
                raise HarnessError("Board did not return an install claim")
            try:
                await docker_async("run", "-d", "--name", container, "--network", "host",
                                   "--env", "AGENT_WORKER_SERVICE=process", args.image,
                                   "install", install_code, "--server", server)
            except HarnessError:
                raise HarnessError("could not start the Rust worker container") from None
            await wait_registered(session, base, install_code, worker_id, args.engine, container)

            script = (f"from pathlib import Path; Path({marker_path!r}).write_text({marker!r}, "
                      "encoding='utf-8')")
            flow = {
                "name": flow_name,
                "description": "Disposable real Rust worker container E2E",
                "steps": {
                    "mark_container": {
                        "kind": "program",
                        "requires_command": "python3",
                        "worker": {"id": worker_id, "pool": pool, "tags": [tag]},
                        "run": f"python3 -c {shlex.quote(script)}",
                    }
                },
            }
            await api(session, base, "PUT", f"/api/flows/{quote(flow_name, safe='')}",
                      {"yaml": json.dumps(flow)})
            flow_saved = True
            activated = await activate(session, base, flow_name)
            engines = activated.get("engines", [])
            selected = next((item for item in engines if item.get("engine") == args.engine), None)
            if selected and selected.get("state") == "failed":
                raise HarnessError("the selected engine did not activate the flow")
            # Native engine workers re-register activated flows asynchronously.
            await asyncio.sleep(25)

            validation = await api(session, base, "POST", "/api/launch/validate",
                                   {"flow": flow_name, "system": args.engine})
            if validation.get("status") == "blocked":
                raise HarnessError("launch validation blocked the selected worker")
            step = validation.get("steps", {}).get("mark_container", {})
            if step and not any(item.get("worker") == worker_id for item in step.get("workers", [])):
                raise HarnessError("launch validation did not select the installed worker")
            launched = await api(session, base, "POST",
                                 f"/api/boards/{quote(args.engine, safe='')}/{quote(flow_name, safe='')}/cards",
                                 {"kind": "flow", "flow": flow_name})
            run_id = launched.get("id")
            if not run_id:
                raise HarnessError("the engine did not return a flow run id")
            dag = await wait_flow_done(session, base, args.engine, run_id, flow_name)
            await wait_card_done(session, base, args.engine, flow_name, run_id)
            node = next((item for item in dag.get("nodes", []) if item.get("step") == "mark_container"), None)
            actual_worker = ((node or {}).get("execution") or {}).get("worker") or {}
            if actual_worker.get("id") != worker_id:
                raise HarnessError("DAG execution metadata did not report the installed worker id")
            actual_marker = await docker_async("exec", container, "cat", marker_path)
            if actual_marker != marker:
                raise HarnessError("the UUID-specific marker was not found in the worker container")
            print(json.dumps({"engine": args.engine, "worker_id": worker_id, "run_id": run_id,
                              "container_execution": "verified"}), flush=True)
        finally:
            await cleanup_run(session, base, args.engine, flow_name, run_id)
            try:
                await docker_async("rm", "-f", container, timeout=30)
            except HarnessError:
                pass
            if flow_saved:
                await cleanup_flow(session, base, flow_name)
            if install_code:
                await cleanup_install(session, base, worker_id, install_code)


def main():
    parser = argparse.ArgumentParser(description="Run a native-engine job in an installed Rust worker container")
    parser.add_argument("--base", required=True, help="Board API origin, e.g. http://127.0.0.1:8093")
    parser.add_argument("--engine", choices=("hatchet", "prefect"), required=True)
    parser.add_argument("--image", required=True, help="Built image containing the Linux Rust worker")
    parser.add_argument("--server-address", help="Non-loopback Board origin reachable from the container; defaults to CI_SERVER_ADDRESS, CI_HOST_IP, or a non-loopback --base")
    args = parser.parse_args()
    try:
        asyncio.run(run(args))
    except HarnessError as exc:
        print(f"Fleet worker container E2E failed: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Fleet worker container E2E failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
