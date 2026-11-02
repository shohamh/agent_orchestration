# Agent sessions on the board: design notes (work in progress)

Goal: run real coding agents on this PC through Hatchet and Trigger.dev, and show each session in
the board as a chat. In the chat you can follow the transcript and tool calls, approve or deny
permission prompts, answer the agent's questions, interrupt or steer it, and type "continue". The
agents are Claude Code, Codex, opencode, opencode v2, omp and pi.

## Decision: use ACP (Agent Client Protocol) as the single integration

We write one ACP client (Python, `agent-client-protocol==0.12.1`, already in `.venv`). It spawns
any ACP agent over stdio. Verified on 2026-11-02:

| Agent | ACP entry point | Status |
|---|---|---|
| Claude Code | `node_modules/.bin/claude-agent-acp` (`@agentclientprotocol/claude-agent-acp@0.100.0`, installed here) | The CLI login has **expired**. Run `claude` then `/login` before the demo. |
| Codex | `node_modules/.bin/codex-acp` (`@agentclientprotocol/codex-acp@2.5.0`, installed here) | Logged in. Native `codex app-server` was probed and works (approvals included). |
| opencode | `opencode acp` (native; v1.18.11 installed) | Not probed yet |
| omp | `omp acp` (native; v18.14.2 installed) | Not probed yet |
| pi | `pi-acp` adapter (npm `pi-acp@0.0.34`, svkozak/pi-acp), not installed yet. Fallback is pi's own `--mode rpc`. | Not probed yet |
| opencode v2 | **Unclear what's meant.** No 2.x on npm. `@opencode-ai/sdk` has a `./v2` client for `opencode serve`'s HTTP API. Confirm with the user. | Pending |

## ACP features we rely on

- `session/new`, `session/prompt` and `session/cancel` (interrupt). The prompt response carries a
  `stopReason`: end_turn, max_tokens, max_turn_requests, refusal or cancelled.
- `session/update` notifications: agent message chunks, tool_call, tool_call_update, plan.
- `session/request_permission`: the client picks one of the agent's options (allow_once,
  allow_always, reject_once, ...). These become approval cards in the chat.
- `elicitation/create` (form mode, unstable). Claude's adapter maps **AskUserQuestion** onto this
  **only if the client advertises `elicitation.form`** in initialize. Each question is a property:
  `{type: string, oneOf: [{const,title,description}]}`, with `type: array, items.anyOf` for multi
  select, plus a companion "Other" free-text field. Reply with `{action: "accept", content: {...}}`.
  Codex-acp also uses elicitation.
- Don't advertise `fs` / `terminal`, so agents use their own tools.

## Session state machine (what the board shows)

```
starting -> working -> (approval | question) -> working -> turn ends
  turn ends ok        -> awaiting_review   "agent says it's done": Continue or Mark done (never auto-Done)
  turn cancelled      -> awaiting_input    interrupted: tell it what to do
  turn failed         -> retrying (auto, up to N) -> stuck   "failed N times": Continue to retry
  user Mark done      -> done              session ends, run completes
```

This handles "done for the harness but not for the user" (awaiting_review plus "continue") and
"stuck after too many retries" (stuck plus "continue") the same way. A demo option to simulate
failures can fake errors to show `stuck`.

The event and input schema is in `agentdriver/events.py`; it has been written already. Inputs are
message, approval, answer, interrupt and done.

## Orchestrator wiring (verified pieces)

- **Hatchet:** a native Windows worker (`.venv`, `hatchet-sdk`) runs task `agent-session` with
  `retries=0` and a long timeout. It picks up inputs by polling `hatchet.event.aio_list(keys=["agent:input"],
  scopes=[run_id])`; the board pushes events scoped to the run. Events go out as `ctx.aio_log("EVT " + json)`.
  If you make it a durable task instead, set `eviction_policy=None`. The default evicts after a
  10-minute wait and then replays the code.
- **Trigger.dev:** its runs are containers, so local CLIs and logins need **`trigger dev` on this
  PC** (dev environment). `trigger.dev/example-project/.env` must set
  `TRIGGER_STREAM_URL=http://localhost:8030`, otherwise dev runs get `http://webapp:3000` and
  input streams never arrive. Steering a dev run was verified with this setting. The task spawns
  `python -m agentdriver.stdio` and pipes JSONL: input stream to stdin, stdout to `metadata.events`.
  The board needs the dev env key (`TRIGGER_DEV_SECRET_KEY`) in addition to prod.

## Board UI to build

A chat panel in the card drawer:
- Bubbles for user and agent messages.
- Collapsible tool rows.
- Approval cards with Allow, Allow for session and Deny.
- Question cards with option buttons, multi-select, and "Other".
- State banner and composer: Send, Interrupt while working, Continue and Mark done when waiting.

New card type: "Coding agent session" with an agent picker and a prompt. Add a demo workspace
template that has a failing test, so agents ask for approvals.
