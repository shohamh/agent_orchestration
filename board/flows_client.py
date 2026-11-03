"""Client for the flow host (agents/flow_host.py), which owns flows/*.yaml on the worker PC.

The board runs in Docker and can't mount flows/, so it lists, validates and saves flows through
the flow host's API (env FLOW_HOST_URL, default http://host.docker.internal:8095; FLOW_HOST_TOKEN).
"""

import os
import time
from typing import Any
from urllib.parse import quote

import aiohttp


class FlowHost:
    def __init__(self) -> None:
        self.url = os.environ.get("FLOW_HOST_URL", "http://host.docker.internal:8095").rstrip("/")
        token = os.environ.get("FLOW_HOST_TOKEN", "")
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._session: aiohttp.ClientSession | None = None
        self._cache: tuple[float, dict[str, Any]] | None = None
        self._down_until = 0.0

    async def _request(self, method: str, path: str, body: Any = None, timeout: float = 10) -> Any:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self.headers, timeout=aiohttp.ClientTimeout(total=10, connect=2))
        try:
            async with self._session.request(method, self.url + path, json=body, timeout=aiohttp.ClientTimeout(total=timeout, connect=2)) as response:
                data = await response.json(content_type=None)
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"The flow host at {self.url} is not reachable ({exc.__class__.__name__}). "
                               "Start it on the worker PC: agents/.venv/Scripts/python.exe -u agents/flow_host.py") from None
        if response.status >= 400:
            raise ValueError((data or {}).get("error") or f"flow host {method} {path} -> {response.status}")
        return data

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
        except RuntimeError:
            self._down_until = time.monotonic() + 5
            return {}  # board keeps working without the flow host; flow cards just lose step kinds

    async def validate(self, text: str) -> dict[str, Any]:
        return await self._request("POST", "/validate", {"yaml": text})

    async def save(self, name: str, text: str) -> dict[str, Any]:
        self._cache = None
        return await self._request("PUT", f"/flows/{quote(name, safe='')}", {"yaml": text})

    async def delete(self, name: str) -> None:
        self._cache = None
        await self._request("DELETE", f"/flows/{quote(name, safe='')}")

    async def restart_node(self, system: str, run_id: str, ref: str) -> dict:
        return await self._request('POST', f'/runs/{quote(system, safe="")}/{quote(run_id, safe="")}/nodes/{quote(ref, safe="")}/restart', timeout=90)

    async def workers(self):
        return await self._request('GET', '/workers')

    async def validate_routing(self, execution, steps):
        return await self._request('POST', '/workers/validate', {'execution': execution, 'steps': list(steps)})

    async def execution(self, run_id):
        return (await self._request('GET', f'/execution/{quote(run_id, safe="")}'))['steps']

    async def set_selection(self, run_id, step, worker):
        return await self._request('POST', f'/execution/{quote(run_id, safe="")}/selection', {'step': step, 'worker': worker})

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
