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
import sys
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from agentdriver.events import Emitter, Sink
from .definition import FLOWS_DIR, Flow, Step
from .template import render
from .restart import prepare_workspace, save_result, read_result, descendants

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
    return {"input": {**flow.input, **params}, "_flow_restart": plan,
            "steps": {need: {"output": outputs.get(need)} for need in step.needs}}


async def run_step(flow: Flow, step_id: str, context: dict[str, Any], run_id: str,
                   receive: Callable[[], Awaitable[dict]], sink: Sink) -> Any:
    step = flow.steps[step_id]
    plan = context.get('_flow_restart')
    workspace = prepare_workspace(flow, run_id, plan)
    emit = Emitter(sink)
    if plan and step_id not in descendants(flow, plan['step']):
        output = read_result(workspace, step_id)
        await emit('step', state='done', kind=step.kind, reused=True, output=_bounded(output))
        return output
    if step.kind == "agent":
        try:
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

    config = {"goal": render(step.prompt, context, str(FLOWS_DIR)), "agent": step.agent, "workspace": str(workspace),
              "autoDone": not step.review, "quietDemo": step.agent == "demo" and not step.review}
    result = await run_session(config, receive, sink)
    return result.get("text") or ""


async def _program(step: Step, context: dict, workspace: Path, emit: Emitter) -> Any:
    command = render(step.run, context, str(FLOWS_DIR), shell=True)
    output_file = workspace / f".flow-output-{step.id}.json"
    output_file.unlink(missing_ok=True)
    context_json = json.dumps(context, ensure_ascii=False)
    env = {**os.environ, "FLOW_CONTEXT": context_json, "FLOW_OUTPUT": str(output_file), "FLOW_STEP": step.id,
           "FLOW_DIR": str(FLOWS_DIR), "FLOW_PYTHON": sys.executable, "PYTHONUTF8": "1", "PYTHONUNBUFFERED": "1"}
    # `python` in a command means this worker's interpreter, so steps don't depend on PATH.
    if command.startswith("python "):
        command = f'"{sys.executable}" ' + command[len("python "):]
    await emit("step", state="running", kind="program", command=command)
    process = await asyncio.create_subprocess_shell(
        command, cwd=workspace, env=env, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE, start_new_session=os.name != "nt")
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
    if process.returncode is not None:
        return
    if tree:
        tree.close()  # kills the whole Windows job
    else:
        import signal

        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
