"""Serve the example flows as deployments from this process (like a Hatchet worker).

Each flow becomes a deployment named "<flow name>/example"; runs triggered from the UI or with
run_deployment() execute here.
"""

from prefect import serve

from flows import agent_loop, agent_pipeline, approval_gate, steerable_agent, uppercase
from research_dag import research_report

if __name__ == "__main__":
    serve(
        *(f.to_deployment(name="example") for f in (uppercase, agent_pipeline, research_report, agent_loop, steerable_agent, approval_gate)),
        limit=10,
    )
