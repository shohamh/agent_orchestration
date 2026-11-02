"""Exercise the requirement scenarios against the running stack:
  docker compose exec example-worker python demo.py
"""

import argparse
import json
import time
from uuid import UUID

from prefect import get_client
from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterFlowRunId
from prefect.client.schemas.objects import StateType
from prefect.deployments import run_deployment
from prefect.exceptions import PrefectHTTPStatusError
from prefect.flow_runs import resume_flow_run
from prefect.input import send_input
from prefect.states import Cancelling

from flows import SteerMessage


def wait_for(client, run_id: UUID, *types: StateType, timeout: float = 120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        run = client.read_flow_run(run_id)
        if run.state and run.state.type in types:
            return run
        time.sleep(0.5)
    raise TimeoutError(f"{run_id} never reached {types}, last state {run.state}")


def result(run) -> object:
    return run.state.result()


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--only", nargs="+", choices=("dag", "examples", "hardware", "steering", "cancel", "approval"),
                    help="Run selected scenarios; omit to run everything.")
selected = set(parser.parse_args().only or ("dag", "examples", "hardware", "steering", "cancel", "approval"))

with get_client(sync_client=True) as client:
    if "dag" in selected:
        # Same uneven DAG and deterministic fixture as Hatchet and Trigger.dev.
        # Missing deployments and incorrect dependency wiring must fail this smoke test.
        run = run_deployment("research-report/example", parameters={"topic": "offline agents"}, timeout=180)
        report = result(run)
        assert report["title"] == "Research report: offline agents", report
        assert len(report["key_points"]) == 3, report
        assert report["entities"] == ["Acme Corp", "Globex", "Initech"], report
        assert report["risk"]["level"] == "high", report
        assert report["risk"]["signals"] == ["outage"], report
        assert report["risk"]["entities_at_risk"] == report["entities"], report
        # Task events are recorded asynchronously; wait for their API view to catch up.
        deadline = time.monotonic() + 60
        while True:
            tasks = client.read_task_runs(task_run_filter=TaskRunFilter(flow_run_id=TaskRunFilterFlowRunId(any_=[run.id])))
            if len(tasks) == 5 and all(t.state and t.state.type == StateType.COMPLETED and t.start_time and t.end_time for t in tasks):
                break
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Task records did not settle for {run.id}: {tasks}")
            time.sleep(0.5)
        by_name = {t.name.rsplit("-", 1)[0]: t for t in tasks}
        assert set(by_name) == {"fetch_sources", "summarize", "extract_entities", "assess_risk", "synthesize_report"}, by_name
        for name, parents in {
            "summarize": {"fetch_sources"},
            "extract_entities": {"fetch_sources"},
            "assess_risk": {"fetch_sources", "extract_entities"},
            "synthesize_report": {"summarize", "extract_entities", "assess_risk"},
        }.items():
            task = by_name[name]
            actual = {str(ref.id) for refs in task.task_inputs.values() for ref in refs if ref.input_type == "task_run"}
            assert actual == {str(by_name[p].id) for p in parents}, (name, actual)
            assert task.state.type == StateType.COMPLETED, task
            assert all(by_name[p].end_time <= task.start_time for p in parents), name
        print("research DAG -> PASS", run.id, report, flush=True)
        empty = result(run_deployment("research-report/example", parameters={"topic": "empty", "sources": []}, timeout=180))
        assert empty == {"title": "Research report: empty", "key_points": [], "entities": [],
                         "risk": {"level": "low", "signals": [], "entities_at_risk": []}}, empty
        print("empty DAG    -> PASS", flush=True)

    if "examples" in selected:
        # 0. Baseline examples
        for name, params in [
            ("uppercase/example", {"message": "hello prefect"}),
            ("agent-pipeline/example", {"message": "offline agents"}),
            ("agent-loop/example", {"message": "durability"}),
        ]:
            output = result(run_deployment(name, parameters=params, timeout=180))
            if name.startswith("uppercase/"):
                assert output == {"upper": "HELLO PREFECT"}, output
            elif name.startswith("agent-pipeline/"):
                assert output == {"final": "first draft about offline agents (reviewed with notes about offline agents)"}, output
            else:
                assert output["history"] == ["turn 0: thinking about durability", "turn 1: thinking about durability",
                                             "turn 2: thinking about durability"], output
            print(f"{name:24} -> PASS", output, flush=True)

    if "hardware" in selected:
        # 1. Hardware: any PC with the device, then pinned to one station's own queue
        reading = result(run_deployment("read-sensor/sensor", timeout=180))
        assert reading["station"] in ("station-a", "station-b"), reading
        assert len(bytes.fromhex(reading["reading"])) == 4, reading
        print("any station  -> PASS", reading, flush=True)
        for station in ("station-a", "station-b"):
            run = run_deployment("read-sensor/sensor", work_queue_name=station, timeout=120)
            reading = result(run)
            assert reading["station"] == station, reading
            assert len(bytes.fromhex(reading["reading"])) == 4, reading
            print(f"{station:12} -> PASS", reading, flush=True)

    if "steering" in selected:
        # 2a. Steer: redirect the goal mid-run, then stop it early
        run = run_deployment("steerable-agent/example", parameters={"goal": "summarize logs", "turns": 30}, timeout=0)
        wait_for(client, run.id, StateType.RUNNING)
        time.sleep(3)
        send_input(SteerMessage(action="redirect", instruction="focus on errors only"), flow_run_id=run.id)
        time.sleep(4)
        send_input(SteerMessage(action="stop"), flow_run_id=run.id)
        done = wait_for(client, run.id, StateType.COMPLETED, StateType.FAILED, StateType.CRASHED)
        output = result(done)
        assert done.state.type == StateType.COMPLETED, done.state
        assert output["goal"] == "focus on errors only" and output["stopped_early"], output
        assert any("human redirected" in line for line in output["transcript"]), output
        print("steered      -> PASS", run.id, output, flush=True)

    if "cancel" in selected:
        # 2b. Hard interrupt: cancel a running agent
        run = run_deployment("steerable-agent/example", parameters={"goal": "long job", "turns": 100}, timeout=0)
        wait_for(client, run.id, StateType.RUNNING)
        time.sleep(3)
        client.set_flow_run_state(run.id, Cancelling())
        print("cancelled    -> PASS", run.id, wait_for(client, run.id, StateType.CANCELLED).state.type.value, flush=True)

    if "approval" in selected:
        # 2c. Approval gate: the run pauses with a typed form; a human (or code) resumes it with input
        for approved in (True, False):
            run = run_deployment("approval-gate/example", parameters={"action": "flash firmware"}, timeout=0)
            paused = wait_for(client, run.id, StateType.PAUSED)
            keys = paused.state.state_details.run_input_keyset
            deadline = time.monotonic() + 60
            while True:
                try:
                    description = json.loads(client.read_flow_run_input(run.id, keys["description"]))
                    schema = json.loads(client.read_flow_run_input(run.id, keys["schema"]))
                    break
                except PrefectHTTPStatusError as exc:
                    if exc.response.status_code != 404 or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.5)
            assert description == "Review the proposed action: flash firmware", description
            assert schema["properties"]["approved"]["type"] == "boolean", schema
            assert schema["properties"]["comment"]["type"] == "string", schema
            resume_flow_run(run.id, run_input={"approved": approved, "comment": "smoke test"})
            done = wait_for(client, run.id, StateType.COMPLETED, StateType.FAILED, StateType.CRASHED)
            output = result(done)
            assert output == {"action": "flash firmware", "approved": approved, "comment": "smoke test"}, output
            print("approval     -> PASS", run.id, output, flush=True)

print("Prefect functional checks passed: " + ", ".join(sorted(selected)), flush=True)
