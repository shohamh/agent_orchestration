"""Native Prefect process that serves the `agent-session/local` deployment on this PC.

Run from agents/ with its virtualenv (PREFECT_API_URL defaults to http://localhost:4200/api):

    .venv/Scripts/python.exe -u prefect_worker.py

Session events go to the flow run's logs as `EVT <json>` lines (persisted, so the board can read
the whole transcript). Inputs from the board arrive as Prefect flow run inputs whose keys start
with `agentinput-auto-`; the session polls them in creation order.
"""

import asyncio
import json
import os
from typing import Any, Literal

INPUT_PREFIX = "agentinput-auto"


class InputInbox:
    """Poll a flow run's inputs with our key prefix, oldest first, delivering each once."""

    def __init__(self, client: Any, flow_run_id: Any, key_prefix: str = INPUT_PREFIX) -> None:
        self.client, self.flow_run_id, self.key_prefix = client, flow_run_id, key_prefix
        self.queue: asyncio.Queue[dict] = asyncio.Queue()
        self.seen: set[str] = set()

    async def poll(self) -> None:
        rows = await self.client.filter_flow_run_input(
            flow_run_id=self.flow_run_id, key_prefix=self.key_prefix, limit=100, exclude_keys=self.seen)
        for row in sorted(rows, key=lambda r: (r.created or 0, r.key)):
            self.seen.add(row.key)
            try:
                message = json.loads(row.value)
            except (TypeError, ValueError):
                continue
            if isinstance(message, dict):
                self.queue.put_nowait(message)

    async def receive(self) -> dict:
        while self.queue.empty():
            await self.poll()
            if self.queue.empty():
                await asyncio.sleep(0.5)
        return self.queue.get_nowait()


def build_flow() -> Any:
    from prefect import flow, get_client, get_run_logger
    from prefect.runtime import flow_run

    from agentdriver.session import run_session

    @flow(name="agent-session", retries=0, log_prints=False, persist_result=False)
    async def agent_session(goal: str,
                            agent: Literal["codex", "claude", "opencode", "opencode2", "omp", "pi", "demo"] = "codex",
                            workspace: str = "", simulateFailures: int = 0) -> dict:
        logger = get_run_logger()
        async with get_client() as client:
            inbox = InputInbox(client, flow_run.get_id())

            async def sink(event: dict) -> None:
                logger.info("EVT " + json.dumps(event, ensure_ascii=False))

            config = {"goal": goal, "agent": agent, "workspace": workspace, "simulateFailures": simulateFailures}
            return await run_session(config, inbox.receive, sink)

    return agent_session


if __name__ == "__main__":
    import sys

    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    os.environ.setdefault("PREFECT_API_URL", "http://localhost:4200/api")
    # The Prefect client sends anonymous usage data by default; this setup runs offline.
    os.environ.setdefault("DO_NOT_TRACK", "1")
    # Ship logs promptly: the board renders the transcript from them.
    os.environ.setdefault("PREFECT_LOGGING_TO_API_BATCH_INTERVAL", "0.5")
    build_flow().serve(name="local", limit=4)
