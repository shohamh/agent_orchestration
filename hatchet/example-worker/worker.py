"""Example Hatchet worker: a single task, a DAG workflow, a durable "agent loop" task, a
human-steerable agent (steering.py) and a research-report DAG (research_dag.py).
Traces are enabled (tracing.py).

Connection is configured purely through env vars:
  HATCHET_CLIENT_TOKEN, HATCHET_CLIENT_HOST_PORT (optional override), HATCHET_CLIENT_TLS_STRATEGY=none
"""

from datetime import timedelta

from pydantic import BaseModel

from hatchet_sdk import Context, DurableContext, Hatchet
from research_dag import research_report
from steering import steerable_agent
from tracing import setup_tracing, tracer

hatchet = Hatchet()


class MessageInput(BaseModel):
    message: str


# --- 1. Plain task -----------------------------------------------------------
@hatchet.task(name="uppercase", input_validator=MessageInput, retries=3)
def uppercase(input: MessageInput, ctx: Context) -> dict:
    ctx.log(f"uppercasing {input.message!r}")
    return {"upper": input.message.upper()}


# --- 2. DAG workflow: plan -> (research, draft in parallel) -> review -------
agent_pipeline = hatchet.workflow(name="agent-pipeline", input_validator=MessageInput)


@agent_pipeline.task()
def plan(input: MessageInput, ctx: Context) -> dict:
    return {"steps": [f"research {input.message}", f"draft {input.message}"]}


@agent_pipeline.task(parents=[plan])
def research(input: MessageInput, ctx: Context) -> dict:
    return {"notes": f"notes about {input.message}"}


@agent_pipeline.task(parents=[plan])
def draft(input: MessageInput, ctx: Context) -> dict:
    return {"draft": f"first draft about {input.message}"}


@agent_pipeline.task(parents=[research, draft])
def review(input: MessageInput, ctx: Context) -> dict:
    notes = ctx.task_output(research)["notes"]
    text = ctx.task_output(draft)["draft"]
    return {"final": f"{text} (reviewed with {notes})"}


# --- 3. Durable task: survives worker restarts, can sleep / wait for events ---
@hatchet.durable_task(name="agent-loop", input_validator=MessageInput)
async def agent_loop(input: MessageInput, ctx: DurableContext) -> dict:
    history = []
    for turn in range(3):
        # In a real agent this is where you'd call your (on-prem) LLM and tools.
        # Custom spans show up nested under the run's trace.
        with tracer.start_as_current_span("llm.call", attributes={"agent.turn": turn}):
            history.append(f"turn {turn}: thinking about {input.message}")
        await ctx.aio_sleep_for(timedelta(seconds=1))
    return {"history": history}


def main() -> None:
    setup_tracing("hatchet-example-worker")
    worker = hatchet.worker(
        "example-worker",
        slots=10,
        workflows=[uppercase, agent_pipeline, agent_loop, steerable_agent, research_report],
    )
    worker.start()


if __name__ == "__main__":
    main()
