"""Human-steerable, interruptible agent (durable task).

Between steps the agent durably waits ~2s for a human "steer" event scoped to its own run:
  {"action": "redirect", "instruction": "..."}  -> change the goal and keep going
  {"action": "stop"}                             -> finish early, gracefully
A hard interrupt is a normal cancel (dashboard "Cancel" button or hatchet.runs.cancel(run_id)).
Progress is streamed with put_stream (live) and logged with ctx.log (persisted, so a board can
read the history of any run later via hatchet.logs.list).
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from opentelemetry import trace
from pydantic import BaseModel

from hatchet_sdk import DurableContext, Hatchet, SleepCondition, UserEventCondition, or_
from tracing import tracer

hatchet = Hatchet()

STEER_EVENT = "agent:steer"


class SteerableInput(BaseModel):
    goal: str
    turns: int = 6
    step_seconds: int = 2  # how long each step waits for a human before moving on


def _steer_message(wait_result: dict[str, Any]) -> dict[str, Any] | None:
    """aio_wait_for returns {condition key: payload, ...}; find the steer event's payload, if any."""

    def walk(value: Any) -> dict[str, Any] | None:
        if isinstance(value, dict):
            if "action" in value:
                return value
            for nested in value.values():
                found = walk(nested)
                if found:
                    return found
        if isinstance(value, list):
            for nested in value:
                found = walk(nested)
                if found:
                    return found
        return None

    return walk(wait_result)


@hatchet.durable_task(
    name="steerable-agent",
    input_validator=SteerableInput,
    execution_timeout=timedelta(minutes=30),
)
async def steerable_agent(input: SteerableInput, ctx: DurableContext) -> dict:
    goal = input.goal
    transcript: list[str] = []
    # Steer events sent while the agent is busy (between waits) must not be lost: each wait also
    # considers events that arrived since the previous wait ended.
    listening_since = datetime.now(timezone.utc)

    for turn in range(input.turns):
        # In a real agent: call the (on-prem) LLM / tools here.
        with tracer.start_as_current_span("agent.step", attributes={"agent.turn": turn, "agent.goal": goal}):
            step = f"turn {turn}: working towards {goal!r}"
            transcript.append(step)
        await ctx.aio_put_stream(step + "\n")
        await ctx.aio_log(step)

        # Durable wait: whichever comes first, step_seconds passing or a human steer event for *this* run.
        result = await ctx.aio_wait_for(
            f"steer-{turn}",
            or_(
                SleepCondition(timedelta(seconds=input.step_seconds)),
                UserEventCondition(
                    event_key=STEER_EVENT,
                    scope=ctx.workflow_run_id,
                    consider_events_since=listening_since,
                ),
            ),
        )
        listening_since = datetime.now(timezone.utc)
        message = _steer_message(result)
        if message is None:
            continue
        trace.get_current_span().add_event("human.steer", {"action": message["action"]})
        if message["action"] == "stop":
            transcript.append(f"turn {turn}: stopped by human")
            await ctx.aio_log(transcript[-1])
            return {"goal": goal, "transcript": transcript, "stopped_early": True}
        if message["action"] == "redirect":
            goal = message["instruction"]
            transcript.append(f"turn {turn}: human redirected goal to {goal!r}")
            await ctx.aio_log(transcript[-1])

    return {"goal": goal, "transcript": transcript, "stopped_early": False}
