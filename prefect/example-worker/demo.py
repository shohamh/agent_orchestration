"""Exercise the requirement scenarios against the running stack:
  docker compose exec example-worker python demo.py
"""

import time
from uuid import UUID

from prefect import get_client
from prefect.client.schemas.objects import StateType
from prefect.deployments import run_deployment
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


with get_client(sync_client=True) as client:
    # 0. Baseline examples
    for name, params in [
        ("uppercase/example", {"message": "hello prefect"}),
        ("agent-pipeline/example", {"message": "offline agents"}),
        ("agent-loop/example", {"message": "durability"}),
    ]:
        print(f"{name:24} ->", result(run_deployment(name, parameters=params, timeout=120)))

    # 1. Hardware: any PC with the device, then pinned to one station's own queue
    print("any station  ->", result(run_deployment("read-sensor/sensor", timeout=120)))
    for station in ("station-a", "station-b"):
        run = run_deployment("read-sensor/sensor", work_queue_name=station, timeout=120)
        print(f"{station:12} ->", result(run))

    # 2a. Steer: redirect the goal mid-run, then stop it early
    run = run_deployment("steerable-agent/example", parameters={"goal": "summarize logs", "turns": 30}, timeout=0)
    wait_for(client, run.id, StateType.RUNNING)
    time.sleep(3)
    send_input(SteerMessage(action="redirect", instruction="focus on errors only"), flow_run_id=run.id)
    time.sleep(4)
    send_input(SteerMessage(action="stop"), flow_run_id=run.id)
    done = wait_for(client, run.id, StateType.COMPLETED, StateType.FAILED, StateType.CRASHED)
    print("steered      ->", done.state.type.value, result(done))

    # 2b. Hard interrupt: cancel a running agent
    run = run_deployment("steerable-agent/example", parameters={"goal": "long job", "turns": 100}, timeout=0)
    wait_for(client, run.id, StateType.RUNNING)
    time.sleep(3)
    client.set_flow_run_state(run.id, Cancelling())
    print("cancelled    ->", wait_for(client, run.id, StateType.CANCELLED).state.type.value)

    # 2c. Approval gate: the run pauses with a typed form; a human (or code) resumes it with input
    run = run_deployment("approval-gate/example", parameters={"action": "flash firmware"}, timeout=0)
    wait_for(client, run.id, StateType.PAUSED)
    resume_flow_run(run.id, run_input={"approved": True, "comment": "go ahead"})
    done = wait_for(client, run.id, StateType.COMPLETED, StateType.FAILED, StateType.CRASHED)
    print("approval     ->", done.state.type.value, result(done))
