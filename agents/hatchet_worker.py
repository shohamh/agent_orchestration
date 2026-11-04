"""Native worker: run from agents/ with its virtualenv and Hatchet client env set."""
import asyncio
import json
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Any


class InputInbox:
    """Poll a run's persisted input events, preserving order and avoiding redelivery.

    `since` drops events created before it: a replayed task keeps its id, so without it a restarted
    step would receive (and act on) everything sent to its earlier attempts."""
    def __init__(self, events: Any, run_id: str, since: datetime | None = None) -> None:
        self.events, self.run_id = events, run_id
        self.queue: asyncio.Queue[dict] = asyncio.Queue()
        self.seen: set[str] = set()
        self.start = since
        self.since: datetime | None = since

    async def poll(self) -> None:
        rows, offset = [], 0
        until = datetime.now(timezone.utc)
        while True:
            page = await self.events.aio_list(keys=["agent:input"], scopes=[self.run_id],
                                              since=self.since, until=until, limit=100, offset=offset)
            batch = page.rows or []
            rows.extend(batch)
            if len(batch) < 100:
                break
            offset += len(batch)
        for event in sorted(rows, key=lambda event: (event.metadata.created_at, event.metadata.id)):
            if event.metadata.id in self.seen:
                continue
            self.seen.add(event.metadata.id)
            if self.start and event.metadata.created_at < self.start:
                continue
            if isinstance(event.payload, dict):
                self.queue.put_nowait(event.payload)
        if rows:
            # Inclusive overlap tolerates events sharing timestamps and late visibility.
            self.since = max(event.metadata.created_at for event in rows) - timedelta(seconds=1)
            if self.start:
                self.since = max(self.since, self.start)

    async def receive(self) -> dict:
        while self.queue.empty():
            await self.poll()
            if self.queue.empty():
                await asyncio.sleep(0.5)
        return self.queue.get_nowait()


def build_task(hatchet: Any) -> Any:
    from hatchet_sdk import Context
    from pydantic import BaseModel, Field
    from typing import Literal
    from agentdriver.session import run_session

    class SessionInput(BaseModel):
        goal: str = Field(min_length=1, max_length=20000)
        agent: Literal["codex", "claude", "opencode", "opencode2", "omp", "pi", "demo"] = "codex"
        workspace: str = Field(default="", max_length=4096)
        simulateFailures: int = Field(default=0, ge=0, le=20)
        resumeSessionId: str = Field(default="", max_length=512)

    @hatchet.task(name="agent-session", input_validator=SessionInput, retries=0,
                  execution_timeout=timedelta(hours=24))
    async def agent_session(payload: SessionInput, ctx: Context) -> dict:
        inbox = InputInbox(hatchet.event, ctx.workflow_run_id)

        async def sink(event: dict) -> None:
            await ctx.aio_log("EVT " + json.dumps(event, ensure_ascii=False), raise_on_error=True)
            # Wakes the board's live subscription; the transcript itself is read from the log.
            await ctx.aio_put_stream(event.get("type") or "event")

        return await run_session(payload.model_dump(), inbox.receive, sink)

    return agent_session


if __name__ == "__main__":
    import sys
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[1] / "hatchet" / ".env")
    os.environ.setdefault("HATCHET_CLIENT_HOST_PORT", "localhost:7077")
    os.environ.setdefault("HATCHET_CLIENT_SERVER_URL", "http://localhost:8080")
    os.environ.setdefault("HATCHET_CLIENT_TLS_STRATEGY", "none")
    from hatchet_windows import install
    install()
    from hatchet_sdk import Hatchet
    hatchet = Hatchet()
    hatchet.worker("native-agent-sessions", slots=4, workflows=[build_task(hatchet)]).start()
