"""Session events: the transcript a UI renders.

Every event is a flat JSON object with `seq` (monotonic per session), `ts` (unix seconds) and `type`:

  user               {text, queued?}                       a message from the human
  agent              {text}                                 agent prose
  tool               {id, name, summary, status, output?}   status: running | ok | error (same id = update)
  approval           {id, tool, summary, detail}            agent needs permission
  approval_resolved  {id, decision}                         allow | allow_session | deny
  question           {id, questions: [{id, header, question, multi, options: [{label, description}]}]}
  answer             {id, answers: {question_id: [labels]}}
  status             {state, reason?, attempt?, max_attempts?}
  error              {message, will_retry}

`status.state` is the session state machine (see session.py): starting, working, retrying,
approval, question, awaiting_review, awaiting_input, stuck, done.
"""

import time
from collections.abc import Awaitable, Callable
from typing import Any

MAX_TEXT = 6000
MAX_OUTPUT = 2500

Sink = Callable[[dict[str, Any]], Awaitable[None]]


def clip(text: str | None, limit: int) -> str:
    text = text or ""
    return text if len(text) <= limit else text[: limit - 40] + f"\n… [{len(text) - limit + 40} more chars]"


class Emitter:
    def __init__(self, sink: Sink) -> None:
        self._sink = sink
        self._seq = 0

    async def __call__(self, type: str, **fields: Any) -> dict[str, Any]:
        self._seq += 1
        for key, limit in (("text", MAX_TEXT), ("output", MAX_OUTPUT), ("detail", MAX_OUTPUT)):
            if isinstance(fields.get(key), str):
                fields[key] = clip(fields[key], limit)
        event = {"seq": self._seq, "ts": round(time.time(), 3), "type": type, **fields}
        await self._sink(event)
        return event
