"""Native Prefect process for portable flows: every flows/*.yaml becomes a served deployment.

Each flow is a Prefect flow named after the flow, served as deployment `<flow>/flows`, with its
parameters as the flow's parameters. Each step is a task run named after the step; the outputs of
its `needs` are passed in as task inputs, so Prefect records (and its UI draws) the dependencies.
Step events are the task run's logs as `EVT <json>` lines. Input to an agent step is a flow run
input whose key starts with `agentinput-<task run id>-auto`.

Run from the repository root with the agents virtualenv (flow_host.py does this and restarts it
when a flow changes):

    agents/.venv/Scripts/python.exe -u agents/flow_worker_prefect.py
"""

import inspect
import json
import os
import sys
from pathlib import Path
from typing import Any

# Before anything imports Prefect: its settings are read once, and serve() subprocesses re-import us.
os.environ.setdefault("PREFECT_API_URL", "http://localhost:4200/api")
os.environ.setdefault("DO_NOT_TRACK", "1")  # offline: no anonymous usage data
os.environ.setdefault("PREFECT_LOGGING_TO_API_BATCH_INTERVAL", "0.5")  # the board renders logs live
sys.path.insert(0, str(Path(__file__).resolve().parent))
from flowkit.definition import Flow, load_all  # noqa: E402
from prefect_worker import InputInbox  # noqa: E402


def input_key_prefix(task_run_id: Any) -> str:
    return f"agentinput-{task_run_id}-auto"


def attribute_name(flow: Flow) -> str:
    return "flow_" + flow.name.replace("-", "_")


def build_flow(flow: Flow) -> Any:
    from prefect import flow as prefect_flow, get_client, get_run_logger, task
    from prefect.runtime import flow_run, task_run

    from flowkit.executor import step_context
    from flowkit.distributed import execute_step as run_step

    step_tasks: dict[str, Any] = {}
    for step in flow.steps.values():
        def make(step=step):
            async def run(params: dict, upstream: dict) -> Any:
                logger = get_run_logger()
                async with get_client() as client:
                    inbox = InputInbox(client, flow_run.get_id(), input_key_prefix(task_run.get_id()))

                    async def sink(event: dict) -> None:
                        logger.info("EVT " + json.dumps(event, ensure_ascii=False))

                    context = step_context(flow, step, params, upstream)
                    return await run_step(flow, step.id, context, str(flow_run.get_id()), inbox.receive, sink)

            run.__name__ = step.id
            return task(name=step.id, task_run_name=step.id, retries=step.retries, persist_result=False,
                        timeout_seconds=step.timeout_minutes * 60, log_prints=False)(run)

        step_tasks[step.id] = make()

    def run_flow(**params: Any) -> dict:
        futures: dict[str, Any] = {}
        for step in flow.steps.values():
            upstream = {need: futures[need] for need in step.needs}
            futures[step.id] = step_tasks[step.id].submit(params, upstream)
        results = {sid: future.result() for sid, future in futures.items()}
        last = [sid for sid in flow.steps if not flow.children(sid)]
        return {sid: results[sid] for sid in last}

    # Prefect builds the parameter form from the signature: expose the flow's input as parameters.
    run_flow.__signature__ = inspect.Signature([
        inspect.Parameter(key, inspect.Parameter.KEYWORD_ONLY, default=value,
                          annotation=type(value) if value is not None else Any)
        for key, value in flow.input.items()] + [
        inspect.Parameter('flowRestart', inspect.Parameter.KEYWORD_ONLY, default=None, annotation=dict | None),
        inspect.Parameter('flowExecution', inspect.Parameter.KEYWORD_ONLY, default=None, annotation=dict | None)])
    # serve() runs each flow run in a subprocess that re-imports the flow by module and name, so
    # it must be a module-level attribute (see the bottom of this file).
    run_flow.__name__ = run_flow.__qualname__ = attribute_name(flow)
    run_flow.__module__ = __name__
    return prefect_flow(name=flow.name, description=flow.description or None, persist_result=False,
                        log_prints=False)(run_flow)


def main() -> None:
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")
    from prefect import serve

    for name, error in BROKEN.items():
        print(f"skipping {name}: {error}", file=sys.stderr)
    if not FLOWS:
        print("no valid flows in flows/; nothing to serve", file=sys.stderr)
        return
    deployments = [globals()[attribute_name(flow)].to_deployment(name="flows", tags=["portable-flow"])
                   for flow in FLOWS.values()]
    print(f"serving flows: {', '.join(FLOWS)}", flush=True)
    serve(*deployments, limit=int(os.environ.get("FLOW_SLOTS", "8")))


FLOWS, BROKEN = load_all()
for _flow in FLOWS.values():
    globals()[attribute_name(_flow)] = build_flow(_flow)

if __name__ == "__main__":
    main()
