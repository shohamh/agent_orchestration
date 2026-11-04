"""Isolated worker-service fixture; each root simulates a distinct physical PC."""
import time
from pathlib import Path
from flowkit.capacity import CapacityGate
from flowkit.worker_service import make_app as production_make_app


def make_app(*args, **kwargs):
    root = Path(kwargs['root'])
    gate = CapacityGate(root/'test-capacity.db', slots=kwargs.get('slots') or 4)
    now = time.monotonic()
    for i in range(11): gate.observe(1,1,now-10+i)
    return production_make_app(*args, capacity=gate, **kwargs)
