"""Stable importable Prefect entrypoint for the per-PC native dispatch worker."""
from prefect import flow

from native_engine_worker import PREFECT_FLOW, _local_job_request


@flow(name=PREFECT_FLOW)
def start_admitted_job(job_id: str, nonce: str) -> dict[str, object]:
    return _local_job_request(job_id, nonce)
