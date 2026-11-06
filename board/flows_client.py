"""Client for the flow host (agents/flow_host.py), which owns the flow drafts and active snapshots on the worker PC.

The board runs in Docker and can't mount flows/, so it lists, validates, saves and activates flows
through the flow host's API (env FLOW_HOST_URL, default http://host.docker.internal:8095; FLOW_HOST_TOKEN).
catalog()/flows() are the active definitions (what can be launched); the document methods return
(HTTP status, body) so the board can pass conflicts and validation errors through unchanged.
"""

import asyncio
import os
import time
from typing import Any
from urllib.parse import quote

import aiohttp
import json


class FlowHost:
    def __init__(self) -> None:
        self.url = os.environ.get("FLOW_HOST_URL", "http://host.docker.internal:8095").rstrip("/")
        token = os.environ.get("FLOW_HOST_TOKEN", "")
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._session: aiohttp.ClientSession | None = None
        self._cache: tuple[float, dict[str, Any]] | None = None
        self._down_until = 0.0
        self._executions: dict[str, dict[str, Any]] = {}  # run id -> {step: execution}, see executions()

    async def _request(self, method: str, path: str, body: Any = None, timeout: float = 10) -> Any:
        status, data = await self._call(method, path, body, timeout)
        if status >= 400:
            raise ValueError((data or {}).get("error") or f"flow host {method} {path} -> {status}")
        return data

    async def _call(self, method: str, path: str, body: Any = None, timeout: float = 10) -> tuple[int, Any]:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self.headers, timeout=aiohttp.ClientTimeout(total=10, connect=2))
        # The flow host only accepts JSON writes, so a cross-site form post cannot reach it.
        headers = {} if method == "GET" else {"Content-Type": "application/json"}
        try:
            async with self._session.request(method, self.url + path, data=None if body is None else json.dumps(body),
                                             headers=headers, timeout=aiohttp.ClientTimeout(total=timeout, connect=2)) as response:
                data = await response.json(content_type=None)
        except asyncio.TimeoutError:
            raise RuntimeError(f"The flow host at {self.url} is busy: no answer within {timeout:g} s (an activation may be "
                               "restarting workers). Reload to see whether the change was applied.") from None
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"The flow host at {self.url} is not reachable ({exc.__class__.__name__}). "
                               "Start it on the worker PC: agents/.venv/Scripts/python.exe -u agents/flow_host.py") from None
        return response.status, data

    async def catalog(self, fresh: bool = False) -> dict[str, Any]:
        """{"flows": [...], "broken": {...}}, cached briefly: every board refresh asks for it."""
        if not fresh and self._cache and time.monotonic() - self._cache[0] < 3:
            return self._cache[1]
        data = await self._request("GET", "/flows")
        self._cache = (time.monotonic(), data)
        return data

    async def flows(self) -> dict[str, dict[str, Any]]:
        # The board labels cards on every refresh: while the flow host is down, don't retry (and wait
        # for a timeout) more than every few seconds.
        if self._down_until > time.monotonic():
            return {}
        try:
            return {flow["name"]: flow for flow in (await self.catalog())["flows"]}
        except Exception:  # unreachable, busy or a bad answer: never break the card lists
            self._down_until = time.monotonic() + 5
            return {}  # board keeps working without the flow host; flow cards just lose step kinds

    async def validate(self, text: str) -> dict[str, Any]:
        return await self._request("POST", "/validate", {"yaml": text})

    async def save(self, name: str, text: str, expected: Any = ...) -> tuple[int, Any]:
        """The original draft save; with expected (a revision, or None to create), stale saves are 409."""
        self._cache = None
        body = {"yaml": text} if expected is ... else {"yaml": text, "expectedRevision": expected}
        return await self._call("PUT", f"/flows/{quote(name, safe='')}", body)

    async def delete(self, name: str) -> None:
        self._cache = None
        await self._request("DELETE", f"/flows/{quote(name, safe='')}")

    # Workflow builder documents: drafts, layouts and activation (each returns (status, body)).
    async def documents(self) -> tuple[int, Any]:
        return await self._call("GET", "/documents")

    async def document(self, name: str) -> tuple[int, Any]:
        return await self._call("GET", f"/documents/{quote(name, safe='')}")

    async def parse_document(self, text: str) -> tuple[int, Any]:
        return await self._call("POST", "/documents/parse", {"yaml": text})

    async def serialize_document(self, document: Any) -> tuple[int, Any]:
        return await self._call("POST", "/documents/serialize", {"document": document})

    async def save_document(self, name: str, text: str, expected: str | None) -> tuple[int, Any]:
        return await self._call("PUT", f"/documents/{quote(name, safe='')}", {"yaml": text, "expectedRevision": expected})

    async def save_layout(self, name: str, layout: Any) -> tuple[int, Any]:
        return await self._call("PUT", f"/documents/{quote(name, safe='')}/layout", {"layout": layout})

    async def delete_document(self, name: str, revision: str) -> tuple[int, Any]:
        return await self._call("DELETE", f"/documents/{quote(name, safe='')}?revision={quote(revision, safe='')}")

    async def activation(self, name: str, disable: bool = False) -> tuple[int, Any]:
        return await self._call("GET", f"/documents/{quote(name, safe='')}/activation{'?disable=1' if disable else ''}")

    async def deactivate(self, name: str, revision: str) -> tuple[int, Any]:
        self._cache = None
        result = await self._call("POST", f"/documents/{quote(name, safe='')}/deactivate", {"revision": revision}, timeout=180)
        self._cache = None
        return result

    async def activate(self, name: str, revision: str | None) -> tuple[int, Any]:
        self._cache = None
        # Each restarted worker must stay up for a few seconds before it counts as active.
        result = await self._call("POST", f"/documents/{quote(name, safe='')}/activate", {"revision": revision}, timeout=180)
        self._cache = None
        return result

    async def restart_node(self, system: str, run_id: str, ref: str) -> dict:
        return await self._request('POST', f'/runs/{quote(system, safe="")}/{quote(run_id, safe="")}/nodes/{quote(ref, safe="")}/restart', timeout=90)

    async def workers(self):
        return await self._request('GET', '/workers')

    async def validate_routing(self, execution, steps):
        return await self._request('POST', '/workers/validate', {'execution': execution, 'steps': list(steps)})

    @staticmethod
    def _definition(flow):
        # The definition text avoids re-expanding a synthesized setup step.
        return flow.get('yaml') or {k: v for k, v in flow.items() if k not in ('yaml', 'environment', 'revision')}

    async def validate_launch(self, flow, execution=None, engine=None):
        body = {'flow': self._definition(flow), **({'execution': execution} if execution is not None else {}),
                **({'engine': engine} if engine else {})}
        return await self._request('POST', '/launch/validate', body, timeout=30)

    async def validate_card(self, agent, worker, engine=None):
        body = {'card': {'agent': agent, 'worker': worker}, **({'engine': engine} if engine else {})}
        return await self._request('POST', '/launch/validate', body, timeout=30)

    # Fleet management (the board's fleet tab). Each returns (HTTP status, body) for pass-through.
    async def fleet(self) -> tuple[int, Any]:
        return await self._call('GET', '/fleet', timeout=30)

    async def fleet_metrics(self) -> tuple[int, Any]:
        return await self._call('GET', '/fleet/metrics')

    async def fleet_tags(self) -> tuple[int, Any]:
        return await self._call('GET', '/fleet/tags')

    async def add_fleet_tag(self, name: str) -> tuple[int, Any]:
        return await self._call('POST', '/fleet/tags', {'name': name})

    async def register_worker(self, worker: dict) -> tuple[int, Any]:
        return await self._call('POST', '/fleet/workers', worker, timeout=30)

    async def update_worker(self, worker_id: str, worker: dict) -> tuple[int, Any]:
        return await self._call('PUT', f'/fleet/workers/{quote(worker_id, safe="")}', worker, timeout=30)

    async def fleet_updates(self) -> tuple[int, Any]:
        return await self._call('GET', '/fleet/updates', timeout=15)

    async def start_fleet_update(self) -> tuple[int, Any]:
        return await self._call('POST', '/fleet/updates', {}, timeout=30)

    async def stop_fleet_update(self) -> tuple[int, Any]:
        return await self._call('DELETE', '/fleet/updates', timeout=15)

    async def update_link(self, body: dict) -> tuple[int, Any]:
        return await self._call('POST', '/fleet/updates/link', body, timeout=15)

    async def fleet_installs(self) -> tuple[int, Any]:
        return await self._call('GET', '/fleet/installs', timeout=15)

    async def create_install(self, form: dict) -> tuple[int, Any]:
        return await self._call('POST', '/fleet/installs', form, timeout=15)

    async def cancel_install(self, code: str) -> tuple[int, Any]:
        return await self._call('DELETE', f'/fleet/installs/{quote(code, safe="")}', timeout=15)

    async def refresh_capabilities(self, worker_id: str) -> tuple[int, Any]:
        return await self._call('POST', f'/fleet/workers/{quote(worker_id, safe="")}/capabilities/refresh', {}, timeout=90)

    async def execution(self, run_id):
        return (await self._request('GET', f'/execution/{quote(run_id, safe="")}'))['steps']

    async def executions(self, runs: dict[str, bool]) -> dict[str, dict[str, Any]]:
        """{run id: {step: execution}} for many runs ({run id: finished}). A finished run's workers
        don't change, so it is read once; failures leave the run out (or at its last answer)."""
        limit = asyncio.Semaphore(8)

        async def read(run_id: str) -> None:
            async with limit:
                try:
                    self._executions[run_id] = await self.execution(run_id)
                except Exception:
                    pass
        if self._down_until <= time.monotonic():
            await asyncio.gather(*(read(r) for r, final in runs.items() if not (final and r in self._executions)))
        return {r: self._executions[r] for r in runs if r in self._executions}

    async def set_selection(self, run_id, step, worker, flow=None):
        body = {'step': step, 'worker': worker, **({'flow': self._definition(flow)} if flow is not None else {})}
        return await self._request('POST', f'/execution/{quote(run_id, safe="")}/selection', body)

    async def worker_jobs(self, worker_id: str) -> tuple[int, Any]:
        return await self._call("GET", f"/fleet/workers/{quote(worker_id, safe='')}/jobs")

    async def mark_cancelled(self, run_id):
        return await self._request('POST', f'/execution/{quote(run_id, safe="")}/cancel', {}, timeout=3)

    async def close(self) -> None:
        if self._session:
            await self._session.close()


def flow_for_task(task: str, flows: dict[str, Any]) -> str | None:
    """The flow a card's task belongs to: Hatchet/Prefect use the flow name, Trigger.dev `flow-<name>`."""
    if task in flows:
        return task
    if task.startswith("flow-") and task[5:] in flows:
        return task[5:]
    return None
