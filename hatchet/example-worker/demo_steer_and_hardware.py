"""Exercise steering, cancellation and hardware routing against the running stack.

  docker compose --profile worker --profile hardware up -d --build
  docker compose exec example-worker python demo_steer_and_hardware.py
"""

import time

from hardware_worker import ReadSensorInput, read_sensor
from steering import STEER_EVENT, SteerableInput, hatchet, steerable_agent

from hatchet_sdk import DesiredWorkerLabel

# 1. Steer: redirect the goal mid-run, then stop it early.
ref = steerable_agent.run_no_wait(SteerableInput(goal="summarize logs", turns=20))
time.sleep(3)
hatchet.event.push(STEER_EVENT, {"action": "redirect", "instruction": "focus on errors only"}, scope=ref.workflow_run_id)
time.sleep(3)
hatchet.event.push(STEER_EVENT, {"action": "stop"}, scope=ref.workflow_run_id)
print("steered   ->", ref.result())

# 2. Hard interrupt: cancel a running agent.
ref = steerable_agent.run_no_wait(SteerableInput(goal="long job", turns=50))
time.sleep(3)
hatchet.runs.cancel(ref.workflow_run_id)
time.sleep(3)
print("cancelled ->", hatchet.runs.get_status(ref.workflow_run_id))

# 3. Hardware: run on whichever worker has the device, then pin to one named station.
print("any sensor ->", read_sensor.run(ReadSensorInput()))
print(
    "station-a  ->",
    read_sensor.run(
        ReadSensorInput(),
        desired_worker_labels=[DesiredWorkerLabel(key="station", value="station-a", required=True)],
    ),
)
