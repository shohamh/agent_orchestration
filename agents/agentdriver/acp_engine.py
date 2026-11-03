"""ACP transport and its UI callbacks. Agent executables keep their own tools and logins."""

import asyncio
import json
import os
import shutil
import uuid
from pathlib import Path
from urllib.parse import quote

import acp
from acp import schema as s

from .session import InputRejected

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
        self.acp_state = {"configOptions": [], "modes": None, "usage": None, "capabilities": {}}
        self.resumed = False
        self.replaying = False
        self.terminals = {}

    async def publish_state(self, **fields):
        self.acp_state.update(fields)
        await self.session.emit("acp_state", **self.acp_state)

    async def start(self):
        # A reconnect creates a new ACP session; old commands no longer apply.
        await self.session.emit("commands", commands=[])
        await self.publish_state()
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
            init = await self.connection.initialize(protocol_version=acp.PROTOCOL_VERSION,
                client_capabilities=s.ClientCapabilities(terminal=True, elicitation=s.ElicitationCapabilities(form=s.ElicitationFormCapabilities())))
            caps = as_dict(init.agent_capabilities) or {}
            prior = self.session.config.get("_acp_session_id") or self.session.config.get("resumeSessionId")
            if prior and not caps.get("loadSession") and self.session.config.get("resumeSessionId"):
                raise ValueError('This agent does not support loading previous sessions')
            if prior and caps.get("loadSession"):
                self.replaying = bool(self.session.config.get("_acp_session_id"))
                try:
                    response = await self.connection.load_session(session_id=prior, cwd=self.session.config["workspace"], mcp_servers=[])
                finally:
                    self.replaying = False
                self.session_id, self.resumed = prior, True
                self.session.config["_acp_session_id"] = prior
            else:
                response = await self.connection.new_session(cwd=self.session.config["workspace"], mcp_servers=[])
                self.session_id = response.session_id
                self.session.config["_acp_session_id"] = self.session_id
        info = as_dict(response)
        await self.publish_state(capabilities=caps, configOptions=info.get("configOptions", self.acp_state['configOptions']),
                                 modes=info.get("modes", self.acp_state['modes']), sessionId=self.session_id)

    async def set_option(self, option_id, value):
        result = await self.connection.set_config_option(session_id=self.session_id, config_id=option_id, value=value)
        await self.publish_state(configOptions=[as_dict(option) for option in result.config_options])

    async def set_mode(self, mode_id):
        await self.connection.set_session_mode(session_id=self.session_id, mode_id=mode_id)
        modes = dict(self.acp_state.get("modes") or {})
        modes["currentModeId"] = mode_id
        await self.publish_state(modes=modes)

    async def create_terminal(self, session_id, command, args=None, env=None, cwd=None, output_byte_limit=None, **kwargs):
        if session_id != self.session_id: raise ValueError('Unknown session')
        workspace = Path(self.session.config['workspace']).resolve()
        directory = Path(cwd or workspace).resolve()
        if not directory.is_relative_to(workspace): raise ValueError('Terminal directory must be inside the session workspace')
        env_names = [as_dict(entry)['name'] for entry in env or []]
        choice = await self.session.permission('execute', 'Run terminal command',
            json.dumps({'command': command, 'args': args or [], 'cwd': str(directory), 'environmentNames': env_names}, ensure_ascii=False),
            [{'id': 'allow', 'label': 'Allow once', 'kind': 'allow_once'}, {'id': 'deny', 'label': 'Deny', 'kind': 'reject_once'}])
        if choice != 'allow': raise PermissionError('Terminal command was not approved')
        environment = dict(os.environ)
        for entry in env or []:
            value = as_dict(entry)
            environment[value['name']] = value['value']
        process = await asyncio.create_subprocess_exec(command, *(args or []), cwd=directory,
            env=environment, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
            start_new_session=os.name != 'nt')
        from flowkit.executor import _own
        try:
            tree = _own(process.pid)
        except OSError:
            try:
                await asyncio.wait_for(process.wait(), .1)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise
            tree = None
        terminal_id = uuid.uuid4().hex
        terminal = {'process': process, 'tree': tree, 'output': b'', 'truncated': False,
                    'limit': min(max(output_byte_limit if output_byte_limit is not None else 65536, 1), 1024 * 1024)}
        self.terminals[terminal_id] = terminal
        async def read_output():
            await self.session.emit('tool', id='terminal:' + terminal_id, name='terminal', summary=command, status='running')
            while chunk := await process.stdout.read(4096):
                terminal['output'] += chunk
                if len(terminal['output']) > terminal['limit']:
                    terminal['output'] = terminal['output'][-terminal['limit']:]
                    while terminal['output'] and terminal['output'][0] & 0xc0 == 0x80:
                        terminal['output'] = terminal['output'][1:]
                    terminal['truncated'] = True
                await self.session.emit('tool', id='terminal:' + terminal_id, name='terminal', summary=command,
                    status='running', output=terminal['output'][-8000:].decode('utf-8', 'replace'))
            code = await process.wait()
            await self.session.emit('tool', id='terminal:' + terminal_id, name='terminal', summary=command,
                status='ok' if code == 0 else 'error', output=terminal['output'][-8000:].decode('utf-8', 'replace'))
        terminal['task'] = asyncio.create_task(read_output())
        return s.CreateTerminalResponse(terminal_id=terminal_id)

    def _terminal(self, session_id, terminal_id):
        if session_id != self.session_id or terminal_id not in self.terminals: raise ValueError('Unknown terminal')
        return self.terminals[terminal_id]

    async def terminal_output(self, session_id, terminal_id, **kwargs):
        terminal = self._terminal(session_id, terminal_id)
        code = terminal['process'].returncode
        status = s.TerminalExitStatus(exit_code=code if code is not None and code >= 0 else None,
                                      signal=str(-code) if code is not None and code < 0 else None) if code is not None else None
        return s.TerminalOutputResponse(output=terminal['output'].decode('utf-8', 'replace'), truncated=terminal['truncated'], exit_status=status)

    async def wait_for_terminal_exit(self, session_id, terminal_id, **kwargs):
        terminal = self._terminal(session_id, terminal_id)
        await terminal['task']
        code = terminal['process'].returncode
        return s.WaitForTerminalExitResponse(exit_code=code if code >= 0 else None, signal=str(-code) if code < 0 else None)

    async def kill_terminal(self, session_id, terminal_id, **kwargs):
        terminal = self._terminal(session_id, terminal_id)
        from flowkit.executor import _kill
        _kill(terminal['process'], terminal['tree'])
        await terminal['task']
        return s.KillTerminalResponse()

    async def release_terminal(self, session_id, terminal_id, **kwargs):
        terminal = self._terminal(session_id, terminal_id)
        if terminal['process'].returncode is None: await self.kill_terminal(session_id, terminal_id)
        else: await terminal['task']
        if terminal['tree']: terminal['tree'].close()
        del self.terminals[terminal_id]
        return s.ReleaseTerminalResponse()

    async def _drain_stderr(self):
        if self.process.stderr:
            while chunk := await self.process.stderr.read(4096):
                self.stderr_tail = (self.stderr_tail + chunk.decode("utf-8", errors="replace"))[-4000:]

    async def prompt(self, text: str | dict):
        self.message_id = None
        message = text if isinstance(text, dict) else {"text": text}
        blocks = [acp.text_block(message.get("text", ""))]
        caps = self.acp_state.get("capabilities", {}).get("promptCapabilities", {})
        for item in message.get("attachments", []):
            mime = item['mimeType']
            if mime.startswith('image/'):
                if not caps.get('image'): raise InputRejected('This agent does not support image attachments')
                blocks.append(s.ImageContentBlock(type='image', data=item['data'], mime_type=mime))
            else:
                if not caps.get('embeddedContext'): raise InputRejected('This agent does not support file attachments')
                resource = s.BlobResourceContents(uri='attachment:///' + quote(item['name']), mime_type=mime, blob=item['data'])
                blocks.append(s.EmbeddedResourceContentBlock(type='resource', resource=resource))
        result = await self.connection.prompt(session_id=self.session_id, prompt=blocks)
        return result.stop_reason

    async def cancel(self):
        if self.connection and self.session_id:
            await self.connection.cancel(session_id=self.session_id)

    async def close(self):
        for terminal_id in list(self.terminals):
            await self.release_terminal(self.session_id, terminal_id)
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
        if self.replaying and kind in ("agent_message_chunk", "user_message_chunk", "tool_call", "tool_call_update", "plan"):
            return
        if kind == "agent_message_chunk":
            text = text_content(data.get("content"))
            if text:
                self.message_id = self.message_id or uuid.uuid4().hex
                await self.session.emit("agent", text=text, message_id=self.message_id)
        elif kind == "user_message_chunk":
            text = text_content(data.get("content"))
            if text:
                self.message_id = None
                await self.session.emit("user", text=text, replayed=True)
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
                fields["content"] = data["content"]
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
        elif kind == "usage_update":
            await self.publish_state(usage={k: data[k] for k in ("used", "size", "cost") if k in data})
        elif kind == "config_option_update":
            await self.publish_state(configOptions=data.get("configOptions", []))
        elif kind == "current_mode_update":
            modes = dict(self.acp_state.get("modes") or {})
            modes["currentModeId"] = data.get("currentModeId")
            await self.publish_state(modes=modes)

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
