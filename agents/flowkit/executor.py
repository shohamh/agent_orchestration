"""Run one flow step. Every backend calls this, so a step behaves the same everywhere.

Events go to `sink` using the session schema (agentdriver/events.py) plus:

  log    {stream: stdout|stderr, text}          one line of a program step's output
  step   {state: running|done|failed, kind, output?, error?, exit_code?}   the last event of a
         finished step is {state: done, output}

A program step's events go through their own Emitter, an agent step's through the session's, so
`seq` restarts with the final step event; order them by position, not seq.

Agent steps receive the human's input through `receive` (same messages as agent sessions).
"""

import asyncio
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from agentdriver.events import Emitter, Sink
from .definition import FLOWS_DIR, Flow, Step
from .template import _EXPR, command_argv, render
from .restart import prepare_workspace, save_result, read_result, descendants, pin_definition

STDOUT_TAIL = 40


def workspace_for(run_id: str) -> Path:
    """All steps of one flow run on this worker share a folder."""
    base = Path(os.environ.get("FLOW_WORKSPACES") or Path(tempfile.gettempdir()) / "agent-flows")
    path = base / "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def step_context(flow: Flow, step: Step, params: dict[str, Any], outputs: dict[str, Any]) -> dict[str, Any]:
    """What a step may reference: the flow input and the outputs of the steps it needs."""
    params = dict(params or {})
    plan = params.pop('flowRestart', None)
    execution = params.pop('flowExecution', None)
    attached = params.pop('flowContext', None)
    # What was typed in New card. A flow reads it as {{ input.text }}; its own `text` input wins.
    if attached and attached.get('text') and 'text' not in flow.input: params['text'] = attached['text']
    return {"input": {**flow.input, **params}, "_flow_restart": plan, '_flow_execution': execution, '_flow_context': attached,
            "steps": {need: {"output": outputs.get(need)} for need in step.needs}}


async def run_step(flow: Flow, step_id: str, context: dict[str, Any], run_id: str,
                   receive: Callable[[], Awaitable[dict]], sink: Sink) -> Any:
    step = flow.steps[step_id]
    plan = context.get('_flow_restart')
    workspace = prepare_workspace(flow, run_id, plan)
    if not os.environ.get('FLOW_MANAGED_JOB'):  # a managed job's workspace is new; its coordinator pinned the run
        pin_definition(workspace, flow)  # a newer revision never runs the rest of an older run
    emit = Emitter(sink)
    if plan and step_id not in descendants(flow, plan['step']):
        output = read_result(workspace, step_id)
        await emit('step', state='done', kind=step.kind, reused=True, output=_bounded(output))
        return output
    # Managed workers recheck immediately before launch. Scheduling can retry elsewhere.
    if os.environ.get('FLOW_MANAGED_JOB'):
        from .capabilities import probe
        from .fleet_matching import software_requirement
        needed = software_requirement(step)
        if needed.get('ambiguous_command'):
            raise ValueError('Ambiguous program command: declare requires_command')
        for kind, requirement in needed.items():  # shell builtins need no executable
            result = probe(kind, requirement)
            if result['status'] != 'installed':
                raise RuntimeError(f"Required software {requirement!r} is {result['status']} on this worker")
    if step.kind == "agent":
        try:
            if step.setup is not None:
                from .environment import setup_environment
                output = await setup_environment(step.setup, workspace, receive, sink)
            else:
                output = await _agent(step, context, workspace, receive, sink)
        except Exception as exc:
            await emit("step", state="failed", kind="agent", error=str(exc)[:2000])
            raise
    else:
        output = await _program(step, context, workspace, emit)
    # The last event carries the output, so a restart can reuse it on any backend (Prefect doesn't
    # persist task results here) and the board can show it.
    save_result(flow, run_id, step_id, output)
    await emit("step", state="done", kind=step.kind, output=_bounded(output))
    return output


def _bounded(output: Any, limit: int = 60000) -> Any:
    text = json.dumps(output, ensure_ascii=False, default=str)
    return output if len(text) <= limit else {"truncated": True, "preview": text[:2000]}


async def _agent(step: Step, context: dict, workspace: Path, receive, sink) -> str:
    from agentdriver.session import run_session

    goal = render(step.prompt, context, os.environ.get('FLOW_ASSETS_DIR', str(FLOWS_DIR)))
    if context.get('_context_note'): goal += '\n\n' + context['_context_note']
    config = {"goal": goal, "agent": step.agent, "workspace": str(workspace),
              "autoDone": not step.review, "quietDemo": step.agent == "demo" and not step.review}
    result = await run_session(config, receive, sink)
    return result.get("text") or ""


async def _program(step: Step, context: dict, workspace: Path, emit: Emitter) -> Any:
    flow_dir = os.environ.get('FLOW_ASSETS_DIR', str(FLOWS_DIR))
    # cmd.exe cuts a command at a line break and has no reliable quoting, so on Windows a command
    # that takes values starts its program directly whenever it doesn't need the shell.
    argv = command_argv(step.run, context, flow_dir) if os.name == "nt" and _EXPR.search(step.run) else None
    command = subprocess.list2cmdline(argv) if argv else render(step.run, context, flow_dir, shell=True)
    output_file = workspace / f".flow-output-{step.id}.json"
    output_file.unlink(missing_ok=True)
    context_json = json.dumps(context, ensure_ascii=False)
    env = {**os.environ, "FLOW_CONTEXT": context_json, "FLOW_OUTPUT": str(output_file), "FLOW_STEP": step.id,
           "FLOW_DIR": flow_dir, "FLOW_PYTHON": sys.executable, "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1"}
    # `python` in a command means this worker's interpreter, so steps don't depend on PATH.
    if not argv:
        command = re.sub(r"^\s*(?:python|\"python\"|'python')(?=\s|$)",
                         lambda _: subprocess.list2cmdline([sys.executable]) if os.name == 'nt' else shlex.quote(sys.executable),
                         command, count=1)
    await emit("step", state="running", kind="program", command=command)
    options = dict(cwd=workspace, env=env, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                   stderr=asyncio.subprocess.PIPE, start_new_session=os.name != "nt" and not os.environ.get('FLOW_MANAGED_JOB'))
    process = await (asyncio.create_subprocess_exec(*argv, **options) if argv else asyncio.create_subprocess_shell(command, **options))
    tree = _own(process.pid)
    tail: list[str] = []

    async def pump(stream: asyncio.StreamReader, name: str) -> None:
        while line := await stream.readline():
            text = line.decode("utf-8", "replace").rstrip("\r\n")
            if name == "stdout":
                tail.append(text)
                del tail[:-STDOUT_TAIL]
            await emit("log", stream=name, text=text)

    try:
        process.stdin.write(context_json.encode())
        await process.stdin.drain()
        process.stdin.close()
        await asyncio.gather(pump(process.stdout, "stdout"), pump(process.stderr, "stderr"))
        code = await process.wait()
    except BaseException:
        _kill(process, tree)
        raise
    finally:
        if tree:
            tree.close()
    if code != 0:
        await emit("step", state="failed", kind="program", exit_code=code)
        raise RuntimeError(f"step {step.id!r} exited with code {code}: {' | '.join(tail[-3:])[:500]}")
    try:
        output = json.loads(output_file.read_text(encoding="utf-8")) if output_file.exists() else {"stdout": "\n".join(tail)}
    except ValueError as exc:
        raise RuntimeError(f"step {step.id!r} wrote invalid JSON to FLOW_OUTPUT: {exc}") from None
    return output


def _own(pid: int):
    if os.name != "nt":
        return None
    from agentdriver.process_tree import ProcessTree

    return ProcessTree(pid)


def _kill(process, tree) -> None:
    if tree:
        tree.close()  # kills the whole Windows job
    elif os.name == 'nt' or os.environ.get('FLOW_MANAGED_JOB'):
        if process.returncode is None:
            try: process.kill()
            except ProcessLookupError: pass
    else:
        import signal

        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
