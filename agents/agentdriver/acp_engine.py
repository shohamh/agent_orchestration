"""ACP transport and its UI callbacks. Agent executables keep their own tools and logins."""

import asyncio
import json
import os
import shutil
import uuid
from pathlib import Path

import acp
from acp import schema as s

ROOT = Path(__file__).resolve().parents[1]


def as_dict(value):
    return value.model_dump(by_alias=True, exclude_none=True) if hasattr(value, "model_dump") else value


def text_content(content) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, dict):
        if content.get("type") == "text":
            return content.get("text", "")
        if "content" in content:
            return text_content(content["content"])
        return json.dumps(content, ensure_ascii=False, default=str)
    if isinstance(content, list):
        return "\n".join(text_content(c) for c in content)
    return ""


# Agents without native ACP support, reached through an npm adapter installed in agents/node_modules.
# pi-acp drives the user's `pi` CLI (`pi --mode rpc`), which must be on PATH.
ADAPTERS = {
    "claude": ("@agentclientprotocol", "claude-agent-acp"),
    "codex": ("@agentclientprotocol", "codex-acp"),
    "pi": (None, "pi-acp"),
}


def agent_command(agent: str) -> list[str]:
    if agent == "opencode2":
        # opencode v2 (npm @opencode/cli), installed locally so it doesn't replace the global v1
        # `opencode`. (`acp` rejects --standalone, so it uses v2's shared background service.)
        binary = ROOT / "node_modules" / "@opencode" / "cli" / "bin" / ("opencode.exe" if os.name == "nt" else "opencode")
        if not binary.is_file():
            raise RuntimeError("opencode v2 is missing. Run npm ci in agents/.")
        return [str(binary), "acp"]
    if agent in ADAPTERS:
        scope, package = ADAPTERS[agent]
        adapter = ROOT / "node_modules" / Path(*filter(None, (scope, package))) / "dist" / "index.js"
        if not adapter.is_file():
            raise RuntimeError(f"{agent} ACP adapter is missing. Run npm ci in agents/.")
        return [shutil.which("node") or "node", str(adapter)]
    executable = shutil.which(agent)
    if not executable:
        raise RuntimeError(f"{agent} is not installed or not on PATH.")
    # npm's Windows .cmd launchers cannot be passed to CreateProcess directly.
    if os.name == "nt" and executable.lower().endswith((".cmd", ".bat")):
        # npm shims wrap a package bin. Resolve the installed bin directly; no shell quoting.
        package = "opencode-ai" if agent == "opencode" else agent
        package_root = Path(executable).parent / "node_modules" / package
        manifest = package_root / "package.json"
        if manifest.is_file():
            bins = json.loads(manifest.read_text(encoding="utf-8")).get("bin", {})
            relative = bins.get(agent) if isinstance(bins, dict) else bins
            binary = package_root / relative if relative else None
            if binary and binary.is_file():
                if binary.suffix.lower() == ".exe":
                    return [str(binary), "acp"]
                return [shutil.which("node") or "node", str(binary), "acp"]
        raise RuntimeError(f"Cannot resolve {agent}'s npm executable. Install a native ACP executable on PATH.")
    return [executable, "acp"]


class AcpEngine:
    def __init__(self, session, command: list[str] | None = None):
        self.session = session
        self.command = command
        self.context = None
        self.connection = None
        self.process = None
        self.session_id = None
        self.message_id = None
        self.tools: dict[str, dict[str, str]] = {}  # toolCallId -> last known {name, summary}
        self.stderr_task = None
        self.stderr_tail = ""
        self.process_tree = None

    async def start(self):
        # A reconnect creates a new ACP session; old commands no longer apply.
        await self.session.emit("commands", commands=[])
        command = self.command or agent_command(self.session.config["agent"])
        env = dict(os.environ)
        # Inherited nesting markers belong to this board's host, not the coding session.
        env.pop("CLAUDECODE", None)
        if self.session.config["agent"] == "codex":
            env.setdefault("INITIAL_AGENT_MODE", "read-only")
            env.setdefault("NO_BROWSER", "1")
        if self.session.config["agent"] in ("opencode", "opencode2"):
            # opencode allows every tool by default, so a board session would never see a permission
            # prompt. Ask for edits, shell and web fetches; merged over the user's own opencode config.
            env.setdefault("OPENCODE_CONFIG_CONTENT", json.dumps(
                {"permission": {"edit": "ask", "bash": "ask", "webfetch": "ask"}}))
        self.context = acp.spawn_agent_process(self, command[0], *command[1:],
            cwd=self.session.config["workspace"], env=env, use_unstable_protocol=True)
        self.connection, self.process = await self.context.__aenter__()
        if os.name == "nt":
            from .process_tree import ProcessTree
            self.process_tree = ProcessTree(self.process.pid)
        self.stderr_task = asyncio.create_task(self._drain_stderr())
        async with asyncio.timeout(60):
            await self.connection.initialize(protocol_version=acp.PROTOCOL_VERSION,
                client_capabilities=s.ClientCapabilities(elicitation=s.ElicitationCapabilities(form=s.ElicitationFormCapabilities())))
            response = await self.connection.new_session(cwd=self.session.config["workspace"], mcp_servers=[])
        self.session_id = response.session_id

    async def _drain_stderr(self):
        if self.process.stderr:
            while chunk := await self.process.stderr.read(4096):
                self.stderr_tail = (self.stderr_tail + chunk.decode("utf-8", errors="replace"))[-4000:]

    async def prompt(self, text: str):
        self.message_id = None
        result = await self.connection.prompt(session_id=self.session_id, prompt=[acp.text_block(text)])
        return result.stop_reason

    async def cancel(self):
        if self.connection and self.session_id:
            await self.connection.cancel(session_id=self.session_id)

    async def close(self):
        if self.process_tree:
            self.process_tree.close()
            self.process_tree = None
        # Kill descendants too: adapters spawn an app-server/CLI of their own.
        if self.process and self.process.returncode is None:
            if os.name == "nt":
                killer = await asyncio.create_subprocess_exec("taskkill", "/PID", str(self.process.pid), "/T", "/F",
                    stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
                await killer.wait()
            else:
                try:
                    self.process.terminate()
                except ProcessLookupError:
                    pass
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        if self.context:
            context, self.context = self.context, None
            await context.__aexit__(None, None, None)
        if self.stderr_task:
            self.stderr_task.cancel()
            await asyncio.gather(self.stderr_task, return_exceptions=True)

    def on_connect(self, conn):
        pass

    async def session_update(self, session_id, update, **kwargs):
        data = as_dict(update)
        kind = data.get("sessionUpdate")
        if kind == "agent_message_chunk":
            text = text_content(data.get("content"))
            if text:
                self.message_id = self.message_id or uuid.uuid4().hex
                await self.session.emit("agent", text=text, message_id=self.message_id)
        elif kind in ("tool_call", "tool_call_update"):
            self.message_id = None
            status = {"completed": "ok", "failed": "error"}.get(data.get("status"), "running")
            tool_id = data["toolCallId"]
            # Updates often omit kind/title. Repeat the last known ones on every event: Trigger.dev's
            # rolling transcript may keep only a tool's latest event, which must still say what it was.
            known = self.tools.setdefault(tool_id, {"name": "tool", "summary": ""})
            if data.get("kind") and data["kind"] != "other":
                known["name"] = data["kind"]
            if data.get("title"):
                known["summary"] = data["title"]
            fields = {"id": tool_id, "status": status, **{k: v for k, v in known.items() if v}}
            if data.get("content"):
                fields["output"] = text_content(data["content"])
            elif data.get("rawOutput") is not None:
                raw = data["rawOutput"]
                fields["output"] = raw if isinstance(raw, str) else json.dumps(raw, ensure_ascii=False, default=str)
            if data.get("rawInput"):
                fields["detail"] = json.dumps(data["rawInput"], ensure_ascii=False, default=str)
            await self.session.emit("tool", **fields)
        elif kind == "plan":
            entries = data.get("entries", [])
            await self.session.emit("plan", entries=entries)
        elif kind == "available_commands_update":
            await self.session.emit("commands", commands=data.get("availableCommands", []))

    async def request_permission(self, session_id, tool_call, options, **kwargs):
        tool = as_dict(tool_call)
        choices = [{"id": o.option_id, "label": o.name, "kind": o.kind} for o in options]
        option_id = await self.session.permission(tool.get("kind", "tool"), tool.get("title", "Permission requested"),
            text_content(tool.get("content")) or json.dumps(tool.get("rawInput", {}), ensure_ascii=False, default=str), choices)
        outcome = s.AllowedOutcome(outcome="selected", option_id=option_id) if option_id is not None else s.DeniedOutcome(outcome="cancelled")
        return s.RequestPermissionResponse(outcome=outcome)

    async def create_elicitation(self, message, mode, **kwargs):
        schema = as_dict(mode.requested_schema)
        answer = await self.session.question(message, schema)
        return s.CancelElicitationResponse(action="cancel") if answer is None else s.AcceptElicitationResponse(action="accept", content=answer)

    async def complete_elicitation(self, elicitation_id, **kwargs):
        pass


class DemoEngine:
    """Explicitly simulated interactions, useful offline and without CLI logins.

    quiet=True (flow steps): no permission or question; it "reads" its prompt and answers from it,
    so a flow can run unattended while still showing a live transcript.
    """
    def __init__(self, session, quiet=False):
        self.session = session
        self.cancelled = False
        self.turn = 0
        self.quiet = quiet

    async def start(self):
        pass

    async def _quiet_prompt(self, text):
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        tool_id = uuid.uuid4().hex
        await self.session.emit("tool", id=tool_id, name="read", summary=f"Read the step input ({len(text)} chars, simulated)", status="running")
        await asyncio.sleep(.5)
        if self.cancelled:
            return "cancelled"
        await self.session.emit("tool", id=tool_id, status="ok", output="\n".join(lines[:12]))
        if self.turn == 1:
            reply = [f"Simulated agent, no model is called. The task: {lines[0] if lines else '(empty)'}",
                     "Key points from the input:"] + [f"- {line[:160]}" for line in lines[1:6]]
        else:
            reply = [f"Noted: {text.strip()[:300]}", "I would apply that instruction here (simulated)."]
        message_id = uuid.uuid4().hex
        for line in reply:
            await asyncio.sleep(.3)
            if self.cancelled:
                return "cancelled"
            await self.session.emit("agent", message_id=message_id, text=line + "\n")
        return "end_turn"

    async def prompt(self, text):
        self.cancelled = False
        self.turn += 1
        if self.quiet:
            return await self._quiet_prompt(text)
        message_id = uuid.uuid4().hex
        await self.session.emit("agent", message_id=message_id,
            text="This is the simulated UI demo. No model is being called. I'll show a permission request and a question.")
        option = await self.session.permission("execute", "Run the demo test suite", "python -m unittest -v", [
            {"id": "allow", "label": "Allow once", "kind": "allow_once"},
            {"id": "session", "label": "Allow for session", "kind": "allow_always"},
            {"id": "deny", "label": "Deny", "kind": "reject_once"}])
        if option is None or self.cancelled:
            return "cancelled"
        if option == "deny":
            await self.session.emit("agent", text="Permission denied. I won't run that tool.", message_id=uuid.uuid4().hex)
            return "end_turn"
        answer = await self.session.question("How should I describe the result?", {"type": "object", "properties": {
            "style": {"type": "string", "title": "Summary style", "oneOf": [{"const": "brief", "title": "Brief"}, {"const": "detailed", "title": "Detailed"}]},
            "checks": {"type": "array", "title": "Checks to mention", "items": {"anyOf": [{"const": "tests", "title": "Tests"}, {"const": "changes", "title": "Changes"}]}},
            "other": {"type": "string", "title": "Other instructions"}}, "required": ["style"]})
        if answer is None or self.cancelled:
            return "cancelled"
        tool_id = uuid.uuid4().hex
        await self.session.emit("tool", id=tool_id, name="execute", summary="python -m unittest -v (simulated)", status="running")
        for _ in range(8):
            await asyncio.sleep(.25)
            if self.cancelled:
                await self.session.emit("tool", id=tool_id, status="error", output="Interrupted")
                return "cancelled"
        await self.session.emit("tool", id=tool_id, status="ok", output="Simulated: 2 tests passed. No workspace files were changed.")
        await self.session.emit("agent", text=f"Demo turn {self.turn} finished ({answer['style']} summary). Review this result. Continue starts another turn; Mark done ends the session.", message_id=uuid.uuid4().hex)
        return "end_turn"

    async def cancel(self):
        self.cancelled = True

    async def close(self):
        self.cancelled = True
