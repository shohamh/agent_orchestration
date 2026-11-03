"""Native Hatchet worker for portable flows: every flows/*.yaml becomes a registered DAG workflow.

Each flow is a Hatchet workflow named after the flow; each step is a task whose `parents` are the
step's `needs`, so Hatchet's dashboard shows the same DAG as the YAML. Step events (program logs,
agent transcripts) are the task's logs as `EVT <json>` lines. Input to an agent step is the event
`agent:input` scoped to that task run's id.

Run from the repository root with the agents virtualenv (flow_host.py does this and restarts it
when a flow changes):

    agents/.venv/Scripts/python.exe -u agents/flow_worker_hatchet.py
"""

import json
import os
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from flowkit.definition import Flow, load_all  # noqa: E402
from hatchet_worker import InputInbox  # noqa: E402


def input_model(flow: Flow) -> Any:
    from pydantic import create_model

    fields = {key: (type(value) if value is not None else str | None, value) for key, value in flow.input.items()}
    fields['flowExecution'] = (dict | None, None)
    return create_model(f"Input_{flow.name.replace('-', '_')}", **fields)


def build_workflow(hatchet: Any, flow: Flow) -> Any:
    from hatchet_sdk import Context

    from flowkit.executor import step_context
    from flowkit.distributed import execute_step as run_step

    workflow = hatchet.workflow(name=flow.name, description=flow.description or None, input_validator=input_model(flow))
    tasks: dict[str, Any] = {}

    for step in flow.steps.values():
        def make(step=step):
            async def run(payload: Any, ctx: Context) -> dict:
                outputs = {need: ctx.task_output(tasks[need])["output"] for need in step.needs}
                inbox = InputInbox(hatchet.event, ctx.step_run_id)

                async def sink(event: dict) -> None:
                    await ctx.aio_log("EVT " + json.dumps(event, ensure_ascii=False), raise_on_error=True)
                    await ctx.aio_put_stream(event.get("type") or "event")

                context = step_context(flow, step, payload.model_dump(), outputs)
                return {"output": await run_step(flow, step.id, context, ctx.workflow_run_id, inbox.receive, sink)}

            run.__name__ = step.id
            return workflow.task(name=step.id, parents=[tasks[n] for n in step.needs], retries=step.retries,
                                 execution_timeout=timedelta(minutes=step.timeout_minutes))(run)

        tasks[step.id] = make()
    return workflow


def main() -> None:
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

    flows, broken = load_all()
    for name, error in broken.items():
        print(f"skipping {name}: {error}", file=sys.stderr)
    if not flows:
        print("no valid flows in flows/; nothing to serve", file=sys.stderr)
        return
    hatchet = Hatchet()
    workflows = [build_workflow(hatchet, flow) for flow in flows.values()]
    print(f"serving flows: {', '.join(flows)}", flush=True)
    hatchet.worker("portable-flows", slots=int(os.environ.get("FLOW_SLOTS", "8")), workflows=workflows).start()


if __name__ == "__main__":
    main()
