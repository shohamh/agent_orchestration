# Agent Board

A small kanban board over **Hatchet** and **Trigger.dev** runs. Each board is a named lane of agent work. You can:
- see cards move through **Scheduled → Queued → Running → Waiting → Done / Stopped**
- see how many runs are ahead of a queued card
- watch intermediate results while a card runs
- **steer**, **stop gracefully**, **cancel** or **replay** a card

http://localhost:8090. There's one UI, with a tab per backend. It has no external assets, so it works offline.

Verified on 2026-11-02 against both stacks:
- `smoke_test.py` passes on both: create, running with progress, steer, graceful stop, cancel, replay, and the DAG of a research-report run.
- Queue positions were checked with the Trigger.dev queue paused, and with the Hatchet worker stopped.
- Creating and steering cards from the UI was checked in the browser.

## Run it

```bash
cd board
cp .env.example .env    # HATCHET_CLIENT_TOKEN (from hatchet/.env) and/or TRIGGER_SECRET_KEY (tr_prod_... from the dashboard)
docker compose up -d --build
python smoke_test.py    # optional: end-to-end check against every configured backend
```

The board reaches both stacks through their published ports (`host.docker.internal`). It doesn't join their networks, so either stack can be down. A backend is enabled only when its credential is set.

## How it maps onto each system

| | Hatchet | Trigger.dev |
|---|---|---|
| Board | `additional_metadata.board` on the run | Run tag `board:<name>` |
| Card title | Run input `goal` | Run metadata `title` (set when the card is created) |
| Intermediate results | The run's persisted logs (`ctx.log` in the agent). History survives board restarts. | Run metadata `transcript` (`metadata.set` in the agent), returned by the runs list |
| Waiting column | Durable task evicted while it waits (holds no worker slot) | `WAITING` (waitpoints, `wait.*`) |
| Queued position | Queued runs of the same task, oldest first | Same, plus queue `running` / `concurrencyLimit` |
| Steer / stop | Event `agent:steer` scoped to the run id | `steer` input stream on the run |
| Cancel | `runs.cancel` | `POST /api/v2/runs/{id}/cancel` |
| Replay | Re-runs **the same run**. A durable task replays from its recorded event history, so it can finish again almost instantly. | Creates **a new run** on the same board |
| Pause a queue | Not available. Hold work with concurrency limits or by stopping the worker. | **Pause / Resume queue** button |

## DAG / run graph view

Click a card to get a live graph of its run in the drawer. Click any node to see its status, timing, output, error and logs. The graph refreshes every 2 seconds while the run is active. Create a **Research report (multi-step DAG)** card to try it.

The two systems produce different kinds of graph:

| | Hatchet | Trigger.dev |
|---|---|---|
| What is drawn | The **declared DAG**: tasks and their `parents=[...]` dependencies, which the engine enforces | The **call tree**: the parent run and the child runs it started in code |
| Ordering | Exact (it's the dependency graph) | Solid lines mean "started child run". Dashed lines mean "ran after", inferred from timing: a child was created only after its earlier siblings finished. For the research report this reconstructs the same graph Hatchet declares. |
| Not-yet-started steps | Shown in grey as soon as the run exists | Only appear once the code triggers them |
| Node logs | That task's `ctx.log` lines | That child run's `metadata.transcript` |

A single-task run, such as the steerable agent, has nothing to draw, so the graph section is hidden for it.

## Making your own agents board-ready

The example agent is `steerable-agent`, in `hatchet/example-worker/steering.py` and `trigger.dev/example-project/src/trigger/steering.ts`. Any agent works if it follows the same small contract:

1. **Report progress.**
   - Hatchet: `await ctx.aio_log(line)` per step. `put_stream` is fine for live-only views.
   - Trigger.dev: keep `metadata.set("transcript", lines)` current.
2. **Listen for steering between steps.** Messages look like `{"action": "redirect", "instruction": "..."}` or `{"action": "stop"}`.
   - Hatchet: `ctx.aio_wait_for(..., UserEventCondition(event_key="agent:steer", scope=ctx.workflow_run_id, consider_events_since=<end of previous wait>))`. Without `consider_events_since`, a message sent while the agent is busy is lost. The smoke test caught this.
   - Trigger.dev: `streams.input({ id: "steer" }).once({ timeoutMs })`. Use `.wait()` for long human pauses.
3. **Cancellation needs nothing.** Both frameworks interrupt the run.

To show other tasks, add their name to `AGENT_TASK` handling in the adapters, or make `steerable` a per-task setting.

## Limits of this prototype

- **No authentication.** Anyone who can reach port 8090 can create, steer and cancel cards. Keep it on localhost or put it behind your reverse proxy or SSO.
- **Polling.** The UI polls every 2 seconds, and the server queries the backend on each poll. That's fine for tens of cards per board. For hundreds, switch Trigger.dev to its Realtime subscriptions (`runs.subscribeToRunsWithTag`) and add a short server-side cache for Hatchet.
- **Window and size.** Each board shows runs from the last 7 days: up to 200 per board on Hatchet and 100 on Trigger.dev.
- **One environment per backend.** Trigger.dev uses one environment (the one whose secret key is configured), and Hatchet uses one tenant.
- **Queue position is computed, not promised.** It's the order of currently queued runs of the same task. Priority, concurrency keys and delays can reorder actual execution, and neither engine exposes an ETA.
- **Board names live in the browser.** The list of boards is kept in `localStorage`. A board exists as soon as one card is tagged with its name.
