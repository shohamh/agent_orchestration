"""Drive Claude Code / Codex coding-agent sessions behind one event + input protocol.

The orchestrators (Hatchet, Trigger.dev) run an AgentSession and only move JSON around:

  events  (session -> UI):  user | agent | tool | approval | approval_resolved | question |
                            answer | status | error            (see events.py)
  inputs  (UI -> session):  message | approval | answer | interrupt | done
"""

__all__: list[str] = []
