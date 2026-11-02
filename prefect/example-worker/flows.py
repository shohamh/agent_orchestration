"""Example Prefect flows: the same scenarios as the Hatchet worker and the Trigger.dev project."""

import time

from opentelemetry import trace
from prefect import flow, get_run_logger, task
from prefect.flow_runs import pause_flow_run
from prefect.input import RunInput, receive_input

tracer = trace.get_tracer("agent")


# --- 1. Plain task with retries ----------------------------------------------
@task(retries=3, retry_delay_seconds=1)
def to_upper(message: str) -> str:
    return message.upper()


@flow(name="uppercase")
def uppercase(message: str) -> dict:
    return {"upper": to_upper(message)}


# --- 2. Fan-out pipeline: plan -> (research, draft in parallel) -> review -------
@task
def plan(topic: str) -> list[str]:
    return [f"research {topic}", f"draft {topic}"]


@task
def research(topic: str) -> str:
    return f"notes about {topic}"


@task
def draft(topic: str) -> str:
    return f"first draft about {topic}"


@flow(name="agent-pipeline")
def agent_pipeline(message: str) -> dict:
    plan(message)
    notes, text = research.submit(message), draft.submit(message)  # run concurrently
    return {"final": f"{text.result()} (reviewed with {notes.result()})"}


# --- 3. Agent loop with sleeps ------------------------------------------------
@flow(name="agent-loop")
def agent_loop(message: str) -> dict:
    history = []
    for turn in range(3):
        with tracer.start_as_current_span("llm.call", attributes={"agent.turn": turn}):
            history.append(f"turn {turn}: thinking about {message}")
        time.sleep(1)
    return {"history": history}


# --- 4. Human-steerable, interruptible agent ----------------------------------
class SteerMessage(RunInput):
    action: str  # "redirect" | "stop"
    instruction: str | None = None


@flow(name="steerable-agent")
def steerable_agent(goal: str, turns: int = 6, step_seconds: float = 2) -> dict:
    """Between steps, wait up to step_seconds for a human message sent with
    `send_input(SteerMessage(...), flow_run_id=...)`. Hard interrupt = cancel the flow run."""
    logger = get_run_logger()
    transcript: list[str] = []
    seen: set[str] = set()

    for turn in range(turns):
        with tracer.start_as_current_span("agent.step", attributes={"agent.turn": turn, "agent.goal": goal}):
            transcript.append(f"turn {turn}: working towards {goal!r}")
            logger.info(transcript[-1])

        message = None
        for received in receive_input(SteerMessage, timeout=step_seconds, poll_interval=0.5, exclude_keys=seen):
            seen.add(received.metadata.key)
            message = received
            break
        if message is None:
            continue

        trace.get_current_span().add_event("human.steer", {"action": message.action})
        if message.action == "stop":
            transcript.append(f"turn {turn}: stopped by human")
            logger.info(transcript[-1])
            return {"goal": goal, "transcript": transcript, "stopped_early": True}
        if message.action == "redirect" and message.instruction:
            goal = message.instruction
            transcript.append(f"turn {turn}: human redirected goal to {goal!r}")
            logger.info(transcript[-1])

    return {"goal": goal, "transcript": transcript, "stopped_early": False}


# --- 5. Approval gate: pause and wait for a human to fill a form in the UI -----
class Approval(RunInput):
    approved: bool
    comment: str = ""


@flow(name="approval-gate")
def approval_gate(action: str) -> dict:
    """Pauses with a typed form; the Prefect UI shows a "Resume" button that renders it.
    From code: resume_flow_run(flow_run_id, run_input={"approved": True, "comment": "..."})."""
    approval = pause_flow_run(
        wait_for_input=Approval.with_initial_data(description=f"Review the proposed action: {action}"),
        timeout=3600,
    )
    return {"action": action, "approved": approval.approved, "comment": approval.comment}
