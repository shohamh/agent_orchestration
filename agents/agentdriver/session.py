"""Conversation lifetime, separate from the lifetime of an agent's single turn."""

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import jsonschema

from .events import Emitter, Sink

WAITING = {"awaiting_review", "awaiting_input", "stuck"}


class Session:
    def __init__(self, config: dict, receive: Callable[[], Awaitable[dict]], sink: Sink):
        self.config, self.receive = config, receive
        self.history: list[dict] = []

        async def record(event):
            if event["type"] == "agent":
                if self.history and self.history[-1].get("message_id") == event.get("message_id") and event.get("message_id"):
                    self.history[-1]["text"] += event["text"]
                else:
                    self.history.append({"role": "agent", "text": event["text"], "message_id": event.get("message_id")})
            await sink(event)

        self.emit = Emitter(record)
        self.state = "starting"
        self.messages: asyncio.Queue = asyncio.Queue()
        self.pending: dict[str, dict] = {}
        self.pending_lock = asyncio.Lock()
        self.engine = None
        self.active: asyncio.Task | None = None
        self.interrupted = False
        self.finishing = False
        self.interrupt_done = asyncio.Event()
        self.interrupt_done.set()
        self.cancel_event = asyncio.Event()
        self.connections = 0
        self.failures = int(config.get("simulateFailures", 0))
        self.engine_factory = self._engine_factory

    def _engine_factory(self):
        from .acp_engine import AcpEngine, DemoEngine

        return DemoEngine(self) if self.config.get("agent") == "demo" else AcpEngine(self)

    async def status(self, state: str, **fields):
        self.state = state
        await self.emit("status", state=state, **fields)

    async def permission(self, tool: str, summary: str, detail: str, options: list[dict]):
        if self.interrupted or self.finishing:
            return None
        request_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = {"type": "approval", "future": future, "options": options}
        await self.emit("approval", id=request_id, tool=tool, summary=summary, detail=detail, options=options)
        await self.status("approval", reason="Permission needed")
        try:
            return await future
        finally:
            self.pending.pop(request_id, None)

    async def question(self, message: str, schema: dict):
        if self.interrupted or self.finishing:
            return None
        request_id = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[request_id] = {"type": "answer", "future": future, "schema": schema}
        await self.emit("question", id=request_id, message=message, schema=schema)
        await self.status("question", reason="Your answer is needed")
        try:
            return await future
        finally:
            self.pending.pop(request_id, None)

    async def _resolve(self, message: dict):
        async with self.pending_lock:
            await self._resolve_locked(message)

    async def _resolve_locked(self, message: dict):
        request = self.pending.get(message.get("id"))
        if not request or request["future"].done() or message["type"] != request["type"]:
            await self.emit("error", message="This request is no longer pending.", will_retry=False)
            return
        if message["type"] == "approval":
            option = next((o for o in request["options"] if o["id"] == message.get("optionId")), None)
            if not option:
                await self.emit("error", message="Choose one of the permission options shown.", will_retry=False)
                return
            decision = {"allow_once": "allow", "allow_always": "allow_session"}.get(option["kind"], "deny")
            await self.emit("approval_resolved", id=message["id"], decision=decision, optionId=option["id"])
            request["future"].set_result(option["id"])
        else:
            try:
                jsonschema.validate(message.get("answers"), request["schema"])
            except (jsonschema.ValidationError, jsonschema.SchemaError) as exc:
                await self.emit("error", message=f"Please check your answer: {exc.message[:400]}", will_retry=False)
                return
            await self.emit("answer", id=message["id"], answers=message["answers"])
            request["future"].set_result(message["answers"])
        remaining = [p for p in self.pending.values() if not p["future"].done()]
        await self.status("approval" if any(p["type"] == "approval" for p in remaining) else "question" if remaining else "working")

    async def _cancel_pending(self):
        async with self.pending_lock:
            await self._cancel_pending_locked()

    async def _cancel_pending_locked(self):
        for request_id, request in list(self.pending.items()):
            if request["future"].done():
                continue
            if request["type"] == "approval":
                await self.emit("approval_resolved", id=request_id, decision="cancelled")
            else:
                await self.emit("answer", id=request_id, answers={}, cancelled=True)
            request["future"].set_result(None)

    async def _inputs(self):
        while True:
            message = await self.receive()
            kind = message.get("type")
            if kind == "message" and str(message.get("text", "")).strip():
                text = str(message["text"]).strip()
                await self.emit("user", text=text, queued=self.state not in WAITING)
                await self.messages.put(text)
            elif kind in ("approval", "answer"):
                await self._resolve(message)
            elif kind == "interrupt":
                self.interrupted = True
                self.cancel_event.set()
                self.interrupt_done.clear()
                turn, engine = self.active, self.engine
                try:
                    await self._cancel_pending()
                    if turn and not turn.done():
                        if engine:
                            try:
                                await asyncio.wait_for(engine.cancel(), 3)
                            except Exception:
                                pass
                        # Keep a reference to this turn; the actor may already have observed its end.
                        try:
                            await asyncio.wait_for(asyncio.shield(turn), 5)
                        except Exception:
                            turn.cancel()
                    else:
                        await self.status("awaiting_input", reason="Interrupted. Send a new instruction to continue.")
                finally:
                    self.interrupt_done.set()
            elif kind == "done":
                if self.state not in WAITING:
                    await self.emit("error", message="Interrupt or wait for the current turn before marking done.", will_retry=False)
                else:
                    self.finishing = True
                    # Done takes precedence over any messages queued just before the click.
                    await self.messages.put(None)

    async def _close_engine(self):
        if self.engine:
            try:
                await self.engine.close()
            finally:
                self.engine = None

    async def _turn(self, text: str):
        if self.engine is None:
            self.engine = self.engine_factory()
            await self.engine.start()
            if self.connections:
                # A failed transport loses the provider's in-memory conversation. Restore user intent.
                context = json.dumps(self.history, ensure_ascii=False)
                if len(context) > 48000:
                    context = "[Earlier conversation omitted]\n" + context[-48000:]
                text = ("The agent connection restarted. Continue the existing task; files may already contain earlier work.\n"
                        f"Original task: {self.config['goal']}\nConversation so far:\n{context}\nLatest instruction: {text}")
            self.connections += 1
        if self.failures:
            self.failures -= 1
            raise RuntimeError("Simulated failure (demo switch)")
        return await self.engine.prompt(text)

    async def run(self):
        await self.status("starting", workspace=self.config.get("workspace"), agent=self.config.get("agent"))
        await self.emit("user", text=self.config["goal"])
        await self.messages.put(self.config["goal"])
        pump = asyncio.create_task(self._inputs())
        try:
            while not self.finishing:
                # If the transport goes away, propagate that failure instead of waiting forever.
                next_message = asyncio.create_task(self.messages.get())
                completed, _ = await asyncio.wait([next_message, pump], return_when=asyncio.FIRST_COMPLETED)
                if pump in completed:
                    next_message.cancel()
                    await asyncio.gather(next_message, return_exceptions=True)
                    await pump
                    raise EOFError("Session input stream closed")
                text = next_message.result()
                if text is None or self.finishing:
                    break
                self.interrupted = False
                self.cancel_event.clear()
                self.history.append({"role": "user", "text": text})
                max_attempts = 3
                for attempt in range(1, max_attempts + 1):
                    if self.interrupted or self.finishing:
                        break
                    await self.status("working", attempt=attempt, max_attempts=max_attempts)
                    self.active = asyncio.create_task(self._turn(text))
                    try:
                        completed, _ = await asyncio.wait([self.active, pump], return_when=asyncio.FIRST_COMPLETED)
                        if pump in completed:
                            self.active.cancel()
                            await asyncio.gather(self.active, return_exceptions=True)
                            await pump
                            raise EOFError("Session input stream closed")
                        reason = await self.active
                        await self.interrupt_done.wait()
                        if self.interrupted or reason == "cancelled":
                            await self.status("awaiting_input", reason="Interrupted. Send an instruction to continue.")
                        elif reason == "end_turn":
                            await self.status("awaiting_review", reason="Turn finished. Review the result, continue, or mark done.")
                        else:
                            await self.status("awaiting_input", reason=f"Agent stopped: {reason}. Send a message to continue.")
                        break
                    except asyncio.CancelledError:
                        if not self.interrupted:
                            raise
                        await self._close_engine()
                        await self.status("awaiting_input", reason="Interrupted. The agent connection will restart on your next message.")
                        break
                    except Exception as exc:
                        if pump.done():
                            raise
                        await self._cancel_pending()
                        await self._close_engine()
                        if self.interrupted:
                            await self.status("awaiting_input", reason="Interrupted. Send an instruction to continue.")
                            break
                        retry = attempt < max_attempts
                        await self.emit("error", message=str(exc), will_retry=retry)
                        await self.status("retrying" if retry else "stuck", attempt=attempt, max_attempts=max_attempts,
                                          reason="Retrying failed turn" if retry else "Retries exhausted. Send Continue to retry.")
                        if retry:
                            try:
                                await asyncio.wait_for(self.cancel_event.wait(), float(self.config.get("retryDelay", 1)))
                            except asyncio.TimeoutError:
                                pass
                    finally:
                        self.active = None
            await self.status("done", reason="Marked done by you")
            return {"state": "done", "agent": self.config.get("agent"), "workspace": self.config.get("workspace")}
        finally:
            pump.cancel()
            await asyncio.gather(pump, return_exceptions=True)
            await self._cancel_pending()
            if self.active and not self.active.done():
                self.active.cancel()
                await asyncio.gather(self.active, return_exceptions=True)
            await self._close_engine()


async def run_session(config: dict, receive: Callable[[], Awaitable[dict]], sink: Sink) -> dict:
    from .workspace import prepare_workspace

    config = dict(config)
    config["workspace"] = str(prepare_workspace(config.get("workspace")))
    return await Session(config, receive, sink).run()
