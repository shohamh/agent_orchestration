"""A flow that needs a physical device attached to the PC it runs on.

Each hardware PC runs a process worker on the "hardware" work pool:
  STATION=lab-pc-2 SENSOR_DEVICE=/dev/ttyUSB0 \\
    prefect worker start --pool hardware --work-queue lab-pc-2 --work-queue any-station --limit 1
Runs go to the "any-station" queue by default (any PC with the device picks it up) or to one PC's own
queue: run_deployment("read-sensor/sensor", work_queue_name="station-a"). `--limit 1` serializes
access to the device.

`python hardware.py deploy` registers the deployment (code is read from /app on the worker).
"""

import os
import socket
import sys

from opentelemetry import trace
from prefect import flow

tracer = trace.get_tracer("agent")


@flow(name="read-sensor")
def read_sensor(num_bytes: int = 4) -> dict:
    station = os.environ.get("STATION", socket.gethostname())
    device = os.environ.get("SENSOR_DEVICE", "/dev/urandom")
    with tracer.start_as_current_span("device.read", attributes={"device.path": device, "station": station}):
        with open(device, "rb") as handle:
            raw = handle.read(num_bytes)
    return {"station": station, "worker_host": socket.gethostname(), "device": device, "reading": raw.hex()}


if __name__ == "__main__" and sys.argv[1:] == ["deploy"]:
    read_sensor.from_source(source="/app", entrypoint="hardware.py:read_sensor").deploy(
        name="sensor",
        work_pool_name="hardware",
        work_queue_name="any-station",
    )
