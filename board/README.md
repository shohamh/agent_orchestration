# Agent Board

A small kanban board over **Hatchet**, **Trigger.dev** and **Prefect** runs. Each board is a named lane of agent work. You can:
- see cards move through **Scheduled → Queued → Running → Waiting → Done / Stopped**
- see how many runs are ahead of a queued card
- watch intermediate results while a card runs
- **steer**, **stop gracefully**, **cancel** or **replay** a card

http://localhost:8090. There's one UI, with a tab per backend. It has no external assets, so it works offline.

**Coding-agent chat is now available.** Choose Coding agent session for streamed replies, expandable
tools, permissions, question forms, interruption, Continue, and explicit Mark done. Real sessions use
a native Hatchet or Prefect worker, or the **Trigger.dev · local** development tab; see
[local agent setup](../agents/README.md). The production Trigger tab keeps the original examples.

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

## Login and phone access (Tailscale Funnel)

The board can start coding agents that run commands on this PC. Set `BOARD_PASSWORD` in the ignored
`board/.env` (one was generated there) before exposing it beyond this PC. With a password:

- Every page and API route needs the login. Pages redirect to `/login` and the API returns 401.
  There is deliberately **no localhost exception**: Tailscale Serve/Funnel deliver traffic from
  127.0.0.1.
- The session cookie is signed and expires after 30 days. It is `HttpOnly` and `SameSite=Strict`,
  and `Secure` on the `*.ts.net` HTTPS name. Failed logins are slowed down.
- Changing `BOARD_PASSWORD` or `BOARD_SECRET` signs everyone out.
- The scripts in this folder sign in automatically using `board/.env` (`board_http.py`).

On phones the backends become one scrollable tab row and the columns stack, with empty ones
collapsed. The chat drawer goes full-screen.

Published on this PC with:

```powershell
tailscale funnel --bg --https=10000 http://127.0.0.1:8090   # https://<machine>.<tailnet>.ts.net:10000
tailscale funnel --https=10000 off                           # stop publishing
```

Funnel makes the URL reachable from **the whole internet**, protected only by the password. If only
your own devices need access, use `tailscale serve --bg --https=10000 http://127.0.0.1:8090`
instead (tailnet only; the phone needs the Tailscale app).

## How it maps onto each system

| | Hatchet | Trigger.dev | Prefect |
|---|---|---|---|
| Board | `additional_metadata.board` on the run | Run tag `board:<name>` | Flow run tag `board:<name>` |
| Card title | Run input `goal` | Run metadata `title` (set when the card is created) | Parameter `goal` / `topic` |
| Intermediate results | The run's persisted logs (`ctx.log` in the agent). History survives board restarts. | Run metadata `transcript` (`metadata.set` in the agent), returned by the runs list | The flow run's persisted logs |
| Waiting column | Durable task evicted while it waits (holds no worker slot) | `WAITING` (waitpoints, `wait.*`) | `PAUSED` (`pause_flow_run`) |
| Queued position | Queued runs of the same task, oldest first | Same, plus queue `running` / `concurrencyLimit` | Scheduled/pending runs of the same deployment |
| Steer / stop | Event `agent:steer` scoped to the run id | `steer` input stream on the run | Flow run input (`send_input` key format), read with `receive_input` |
| Cancel | `runs.cancel` | `POST /api/v2/runs/{id}/cancel` | Set state `CANCELLING` |
| Replay | Re-runs **the same run**. A durable task replays from its recorded event history, so it can finish again almost instantly. | Creates **a new run** on the same board | Creates **a new run** with the same parameters and tags |
| Pause a queue | Not available. Hold work with concurrency limits or by stopping the worker. | **Pause / Resume queue** button | Not for served deployments (work-pool queues can be paused in Prefect's UI) |

## Drag and drop

Drag a card (on a phone, hold it briefly first) to another column or to a new place in Queued. While
you drag, columns the card can't go to are dimmed and each allowed column says what dropping does.
The rules are in `moves.py`; the server checks them again before acting.

| Drop | Action | Hatchet | Trigger.dev | Prefect |
|---|---|---|---|---|
| Within Queued | Change priority (top runs first) | **Coarse:** the not-started run is re-created at priority 3 (top), 1 (bottom) or the level of the card above; order within a level is by age | The not-started run is cancelled and replayed with a `priority` offset that puts it between its neighbours | **Exact:** a new start time between its neighbours (Prefect runs the earliest first) |
| Within Scheduled | Change start time | — | — | New start time |
| Scheduled → Queued | Start now | — | Reschedule to now | Start time = now |
| Queued/Scheduled/Running/Waiting → Stopped | Cancel | Cancel | Cancel | Cancel |
| Running → Done | Stop gracefully (steerable agent only) | `agent:steer` stop | `steer` stream stop | Flow run input stop |
| Waiting → Done / Running | Mark done / Continue (coding-agent session waiting for you) | Session input | Session input | Session input |
| Running/Waiting/Done/Stopped → Queued | Run again as a new run, **top = high priority, bottom = low** | New run at priority 3 or 1 | New run with a high or no `priority` offset (cancels a running one first) | New run at the front or back of the queue |

- A run that already started can only go to the **top or bottom** of Queued; the middle is dimmed
  while you hold it. Moves that cancel a running run ask first.
- Columns are ordered the way the engine will run them: Queued and Scheduled by priority/start time,
  Running oldest first, Done and Stopped newest first. "N ahead" follows the same order.
- Re-created runs get a new run id: the old one stays in the engine's UI as cancelled.
- The priority the board chose is kept in the run's metadata (`priority`), because neither Hatchet's
  nor Trigger.dev's run list reports it.

## Live updates

The browser holds one server-sent event stream per tab (`GET /api/live?system=&board=&card=`) and
never polls. On the server, one shared watcher per board (`live.py`) listens to the engine's own
live feed. When something changes, it refreshes the board once and pushes only the cards that
changed to every viewer. A viewer with a card open also gets that card's new transcript rows or
log lines.

| | Live feed the board listens to | Notes |
|---|---|---|
| Hatchet | Run events by additional metadata `board` (`SubscribeToWorkflowEvents`): started, stream data, finished | The agent worker sends one `put_stream` marker per transcript event, so new messages wake the board too. The SDK marks this subscription deprecated, but the engine here serves it. |
| Trigger.dev | Realtime (Electric shape) of runs tagged `board:<name>`, long-polled | Needs the header `x-trigger-electric-version: 1.0.0-beta.1`; without it the webapp keeps answering `must-refetch`. |
| Prefect | Websockets `/api/events/out` (flow run state events, matched by the `board:<name>` tag) and `/api/logs/out` (new log lines, matched by flow run) | Log streaming must be enabled on the server: `PREFECT_SERVER_LOGS_STREAM_OUT_ENABLED` and `PREFECT_SERVER_LOGS_STREAM_PUBLISHING_ENABLED` (set in `prefect/docker-compose.yml`). |

- **Fallbacks.** If a feed is quiet or down, the watcher still refreshes the board every 5 s and
  the open card every 2 s. The status line says `live` or `polling`.
- **Board actions.** A new card, an input or an action refreshes that backend's watched boards right away.
- **First paint.** The first refresh sends cards straight from the engine's run list, then fills in
  last lines and session states. A cold board otherwise reads every run's log once before
  showing anything.
- **Lightweight cards.** Cards carry their latest line (`last`) and a `version`, not transcripts or logs.
- **Paging.** A session opens on its newest 100 messages, and scrolling to the top loads 100 more
  (`/transcript?before=<row key>`). An ordinary log opens on its newest 1000 lines, and scrolling
  up loads 1000 more (`/logs?before=<index>`; DAG node logs page the same way). A spinner shows
  while a page loads, and the rows on screen stay in place.
- **Reads.** Engine logs are read incrementally: only lines newer than the last one seen. A
  finished run's log is frozen once its last batch has landed.
- **Hidden tabs.** They close their stream, and coming back reconnects from a fresh snapshot.
- **Scripts.** Use `GET /api/cards/{system}/{id}/events` for a session's raw events.

## DAG / run graph view

Click a card to get a live graph of its run in the drawer. Click any node to see its status, timing,
output, error and logs. Updates arrive live; see [Live updates](#live-updates).

Portable flow nodes open agent chat or a scrolling program log, with **Restart step** and backend
links. Restart uses native Hatchet dependency replay and a flow-host runtime extension for
Prefect/Trigger.dev; see [step restart behavior](../flows/README.md#watching-and-restarting-steps).
**Open in…** links open a separate tab. Set `HATCHET_DASHBOARD_URL`, `TRIGGER_DASHBOARD_URL` and
`PREFECT_DASHBOARD_URL` to roots reachable by the browser (especially when using a phone),
independently of Docker-internal API URLs.
For Trigger queue links, also set `TRIGGER_DASHBOARD_PATH` / `TRIGGER_DEV_DASHBOARD_PATH` to the
environment path copied from its dashboard (`/orgs/.../projects/.../env/prod` or `/env/dev`).

Verified on 2026-11-03: shared watchers and incremental updates, 100-message/1000-line paging,
per-step chat controls on all three local backends, native Hatchet descendant replay with
unchanged outputs, and Prefect/Trigger runtime restart with prerequisite result/workspace reuse.
`step_smoke_test.py` uses simulated agents and a disposable flow; run it only while portable-flow
workers are idle because it activates (and later removes) that fixture, which re-registers them.

Each system produces a different kind of graph:

| | Hatchet | Trigger.dev | Prefect |
|---|---|---|---|
| What is drawn | The **declared DAG**: tasks and their `parents=[...]` dependencies, which the engine enforces | The **call tree**: the parent run and the child runs it started in code | The **task runs** and which earlier task's result each one received (`task_inputs`) |
| Ordering | Exact (it's the dependency graph) | Solid lines mean "started child run". Dashed lines mean "ran after", inferred from timing: a child was created only after its earlier siblings finished. For the research report this reconstructs the same graph Hatchet declares. | Exact data flow as it happened. For the research report it matches Hatchet's graph. |
| Not-yet-started steps | Shown in grey as soon as the run exists | Only appear once the code triggers them | Only appear once the flow submits them |
| Node logs | That task's `ctx.log` lines | That child run's `metadata.transcript` | That task run's logs |

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

- **One shared password.** With `BOARD_PASSWORD` unset there is no login at all, so keep such a board on localhost. There are no per-user accounts.
- **Board refreshes are still full listings.** A feed event triggers one listing of the board's runs (up to 200), shared by every viewer. Logs are read incrementally, but the run list itself is not.
- **Connections per tab.** Each open board tab holds one stream. Over plain HTTP (`http://localhost:8090`), browsers allow six connections per site, so more than about five visible board tabs can stall other requests. The Funnel's HTTPS address doesn't have this limit.
- **Trigger.dev latency comes from Trigger.dev.** The feed wakes the board within about half a second of a run's metadata changing. Delivering an input to a dev-mode run and seeing its new metadata can take several seconds on this setup.
- **Window and size.** Each board shows runs from the last 7 days: up to 200 per board on Hatchet and 100 on Trigger.dev.
- **Environments.** Trigger.dev production and local development use separate keys and tabs; Hatchet uses one tenant.
- **Queue position is computed, not promised.** It's the order of currently queued runs of the same task. Priority, concurrency keys and delays can reorder actual execution, and neither engine exposes an ETA.
- **Board names live in the browser.** The list of boards is kept in `localStorage`. A board exists as soon as one card is tagged with its name.
