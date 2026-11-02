"""A worker that owns physical hardware attached to the PC it runs on.

Run one of these on each PC that has a device (natively, or in Docker with `devices:` passthrough).
It advertises worker labels; tasks that need the device are only ever scheduled onto workers whose
labels match, and a caller can target one specific station per run.

  STATION=lab-pc-2 SENSOR_DEVICE=/dev/ttyUSB0 python hardware_worker.py
"""

import os
import socket

from pydantic import BaseModel

from hatchet_sdk import Context, DesiredWorkerLabel, Hatchet
from tracing import setup_tracing, tracer

hatchet = Hatchet()

STATION = os.environ.get("STATION", socket.gethostname())
# Anything readable works for the demo; a real setup opens a serial port, USB device, GPIB, camera...
SENSOR_DEVICE = os.environ.get("SENSOR_DEVICE", "/dev/urandom")


class ReadSensorInput(BaseModel):
    num_bytes: int = 4


@hatchet.task(
    name="read-sensor",
    input_validator=ReadSensorInput,
    # Never schedule onto a worker that doesn't have the device.
    desired_worker_labels=[DesiredWorkerLabel(key="device", value="sensor", required=True)],
)
def read_sensor(input: ReadSensorInput, ctx: Context) -> dict:
    with tracer.start_as_current_span("device.read", attributes={"device.path": SENSOR_DEVICE, "station": STATION}):
        with open(SENSOR_DEVICE, "rb") as device:
            raw = device.read(input.num_bytes)
    return {"station": STATION, "worker_host": socket.gethostname(), "device": SENSOR_DEVICE, "reading": raw.hex()}


def main() -> None:
    setup_tracing(f"hatchet-hardware-{STATION}")
    worker = hatchet.worker(
        f"hardware-{STATION}",
        slots=1,  # serialize access to the device
        labels={"device": "sensor", "station": STATION},
        workflows=[read_sensor],
    )
    worker.start()


if __name__ == "__main__":
    main()
