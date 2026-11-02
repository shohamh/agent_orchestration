# Agent chat dashboard implementation plan

Goal: finish the chat dashboard and local coding-agent demo requested in the Claude session.
Spec: `agents/DESIGN.md` and the user's transcript request to build on both backends.
Architecture: one Python ACP session driver; native Hatchet worker and Trigger.dev development task; existing aiohttp board serves the chat without external assets.

## Contracts

- `Card` adds `events: list[dict]`, `session_state: str | None`, `agent: str | None`.
- `POST /api/boards/{system}/{board}/cards` accepts kind `agent-session`, goal, agent (`codex`, `claude`, `opencode`, `omp`, `demo`), workspace (optional; blank creates a disposable demo), simulateFailures (0 default).
- `POST /api/cards/{system}/{run_id}/input` accepts `{type: message, text}`, `{type: interrupt}`, `{type: done}`, `{type: approval, id, optionId}`, `{type: answer, id, answers}`. Answers preserve schema property scalar/array types.
- Event schema stays compatible with `agentdriver/events.py`; approvals add `options: [{id,label,kind}]`; questions add `schema` with the original JSON Schema. Agent chunks use `message_id` to combine streamed text; tools merge by `id`.
- `/api/systems` returns `trigger-dev` separately when its key exists; production tasks stay on `trigger`.
- Only explicit done ends an agent session. Interruption cancels the active turn, resolves pending requests, and leaves the conversation usable. Messages while busy are queued for the next turn.

## Tasks

- [ ] 1. Driver: tests for review/continue/done, retry exhaustion/recovery, permission and question response matching, interruption; implement ACP client, session runner, deterministic demo, JSONL subprocess bridge, demo workspace.
- [ ] 2. Backend: tests for session input validation/state projection; implement Hatchet worker, Trigger.dev task and dev adapter, card fields and input route.
- [ ] 3. UI: chat bubbles, collapsible tools, approvals/forms, composer and state-specific actions; coding-session create form and demo choice. Keep typed answers, draft text, expanded tools, and scroll position across polling. Verify in a real browser.
- [ ] 4. Integrate: run focused tests, TypeScript checks, existing backend smoke test, live sessions on both engines; rebuild running board and document startup and honest demo limitations.

## Review focus

Stale responses after switching boards/runs; canceled pending requests; process cleanup; retry initialization failures; backend terminal errors overriding stale session states; persisted history size; agent availability and login failures.

## Execution notes

The user explicitly requested continuation of the saved design, so implement without another approval round. Work in the current checkout on `feat/agent-chat-dashboard` to preserve existing uncommitted design and running local setup. Do not include the pre-existing comparison/Prefect changes in commits. Offline-bundle regeneration is tracked separately from chat completion.
