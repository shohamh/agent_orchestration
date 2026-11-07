# Agent Board

A small kanban board over **Hatchet**, **Trigger.dev** and **Prefect** runs. Each board is a named lane of agent work. You can:
- see cards move through **Scheduled → Queued → Running → Waiting → Done / Stopped**
- see how many runs are ahead of a queued card
- watch intermediate results while a card runs
- **steer**, **stop gracefully**, **cancel** or **replay** a card
- jump straight to the sessions waiting on you: they're amber on the board, and **N need you** in the toolbar opens each in turn, oldest first

http://localhost:8090. There's one UI, with a tab per backend. It has no external assets, so it works offline.

**New card** starts an agent, a program or a saved flow, now or on a schedule. Repeating
cards can skip a start while the previous run is active, or start another run. Schedules stay
visible in **Scheduled**, even before the board has any runs; opening a worker job loads the
destination board's schedules too.

Agent sessions provide streamed replies, expandable tools, permissions, question forms,
interruption, Continue, and explicit Mark done. Real sessions use a Hatchet or Prefect worker,
or the **Trigger.dev · local** development tab; see [local agent setup](../agents/README.md).
The production Trigger tab keeps the original examples.

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

### Admin: engine URLs and status

Open **Admin** to edit each engine's server-side API URL and browser-visible dashboard URL,
then use **Check now** or leave Admin open to refresh status every 10 seconds. Status distinguishes
disabled engines from reachable, unreachable, and authentication failures. Credentials are still
provided through the board's environment; the UI never reads or stores them. URL settings persist
in the `board_data` Docker volume and apply to the board's adapters without a rebuild. Hatchet's SDK
endpoint is constructed at startup, so changing its API URL takes effect after restarting the board.

For offline deployment, point each API URL at the engine address reachable from the board container;
set dashboard URLs to addresses reachable by the browser (including remote phones). The two URL
types can differ when Docker-internal and browser-visible hostnames differ.

### UI development auto-reload

From the repository root, use the root Compose file and the development override. The root file
connects the board to the flow host and engine network; do not start this override from `board/` alone.

```bash
docker compose -f docker-compose.yml -f board/docker-compose.dev.yml up -d --build board
```

The override mounts `board/` read-only at `/app` (dependencies remain in the image and persistent
data stays in `/data`), enables the static-file watcher, and sets `BOARD_AUTO_RELOAD=1`. Browser tabs
reload within about a second after a file in `board/static/` changes. The backend supervisor watches
Python files under `/app` and restarts the server child after a source change; it uses only the
Python standard library. `BOARD_AUTO_RELOAD` defaults to `0` in ordinary Compose. Set it to `1` to
enable backend restarts; `BOARD_DEV_RELOAD` remains the separate browser/static polling switch.


## Login and phone access (Tailscale Funnel)

The board can start coding agents that run commands on this PC. Set `BOARD_PASSWORD` in the ignored
`board/.env` (one was generated there) before exposing it beyond this PC. With a password:

- Every page and API route needs the login. Pages redirect to `/login` and the API returns 401.
  There is deliberately **no localhost exception**: Tailscale Serve/Funnel deliver traffic from
  127.0.0.1.
- The session cookie is signed and expires after 30 days. It is `HttpOnly` and `SameSite=Strict`,
  and `Secure` on the `*.ts.net` HTTPS name. Failed logins are slowed down.
- Worker `/install/*`, `/update/*` and `/relay/*` endpoints use their install/update code or worker
  token instead of the board cookie. The relay proxy streams binary/runtime downloads and preserves
  their version headers so the worker can verify downloads through the board and Funnel.
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

## Hover and right click

Cards keep to a title, an agent and a time. Hover a card for its full story: the latest update, workers, queue and times.
Explanations elsewhere sit behind tooltips, and touch screens, which have no hover, keep them inline.

Right-click for what you can do with the thing under the pointer. On a card that's open, the same moves as dragging it,
filter by its agent or worker, and copy its title or run ID. Fleet PCs and flows in the list have menus too. In the graph:

- **A step:** edit, add a step after it, duplicate, copy its ID, delete.
- **A dependency:** edit either end, remove it.
- **The canvas:** add a step at the pointer, undo, redo, fit, auto-layout, flow settings.

A run's graph in the drawer is the same viewer as the flow editor's (`static/flow-graph.js`): same layout, including where the
flow's author placed its steps, and the same pan, zoom, fit and hover. Only the actions differ. A run offers its output and
logs, restarting a step, opening it in the engine, and **Edit this flow**; nothing in it changes the flow.

Scroll to pan, Shift+scroll to pan sideways, Ctrl+scroll to zoom. The **?** shows how the graph works on hover, and stays open on click.

Dragging a step's handle onto empty space offers to add the next step there. Hovering a step or dependency lights up what
it connects to. Undo and redo (Ctrl+Z, Ctrl+Y) cover the flow's content, not where steps sit on the canvas. Keyboard:
the menu key or Shift+F10 opens the same menus.

## Which PC work is on

A card's status comes from its engine, which can't tell that the PC doing the work has gone away. So the board checks the fleet too.
Work on an offline PC shows **Worker offline**, stops pulsing and leaves the **need you** count, and its session and steering controls
are disabled until the PC reconnects. **Cancel now** still works, since the engine handles it. A PC the board can't check shows **Connection unknown**. Work on no
worker runs on the coordinator and isn't affected. A worker that a finished step ran on doesn't count.

Cards, the drawer and each step in a run's graph name their worker. Click a worker for its details, what it runs now and what it has
run, from the records the flow host keeps (`GET /api/fleet/workers/{id}/jobs`). A run on a board nobody has open appears by run id and can't be opened from there.

## Finding flows and PCs

The Flows sidebar and the Fleet tab filter with the same words as the board's cards: plain words, `field:value`, `"a phrase"` and
`-word`. Flows have `name:`, `state:`, `step:` and `agent:`. PCs have `id:`, `pool:`, `tag:`, `engine:`, `status:`, `agent:` and `command:`, where
the last two match only what's installed. Clicking a pool or tag on a PC filters by it.

Under **Where each step can run**, the number beside a step is how many workers can take it now. Clicking it opens the Fleet tab
filtered to what that step needs.

The switch beside each flow turns it on or off. Either way you see what it affects first, as with **Activate**. Turning a flow off
stops new runs from starting it and keeps its draft; `POST /api/flow-documents/{name}/deactivate` checks the impact the same way
activation does.

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

## Filtering

The box in the header filters the board as you type (`/` focuses it, Escape clears it). **Hide done /
failed** empties those two columns but keeps them as drop targets; the choice is remembered.

| Query | Matches cards where |
|---|---|
| `gpu report` | every word appears somewhere: title, task, status, agent, flow, run id, latest line, error, execution workers, run input and output |
| `"rust async"` | the phrase appears |
| `worker:gpu-02` | a field contains the value. Fields: `title` `task` `status` `agent` `flow` `id` `last` `error` `system`, and from the execution workers `worker` (id or hostname) `pool` `tag` `step` |
| `goal:vector`, `output.summary:ok` | a key inside the run's input or output contains it. A bare key matches at any depth; `input:` / `output:` search all of one side |
| `-status:failed` | leave matching cards out |
| `pool:` | the card has the field at all |

- Matching is case-insensitive substring search. `status:` also knows the column's everyday names
  (`completed`, `failed`, `cancelled`).
- **Input** is the run's input on Hatchet and its parameters on Prefect. Trigger.dev's runs list
  doesn't return payloads, so Trigger.dev cards search only what the board shows. The drawer now
  shows a card's input next to its output.
- **Workers** are the execution workers that ran a portable flow's steps, read from the flow host
  (`/execution/<run>`) on each refresh; a finished run's workers are read once. Cards show them as
  `on <worker>`.
- Each card is indexed once per version (in idle time after it arrives), and typing further into a
  query only re-checks the previous matches.

## Big boards

Columns initially show 50 cards at a time and load more as you scroll near the end. The **Show more**
button remains available for keyboard use and browsers without intersection observers. Search covers
all cards received from the backend, including cards that have not been rendered yet. Off-screen cards skip layout and paint, unchanged cards keep their elements
between updates, and the glide animation is skipped when more than 300 cards are on screen.

To try it, start a copy of the board with a synthetic backend (`loadtest_adapter.py`) and open the
**Load test** tab. Its cards have generated input, output and workers, and 20 of them change every
2 s through the normal live-update path:

```bash
BOARD_LOADTEST_CARDS=10000 BOARD_HOST=127.0.0.1 BOARD_PORT=8097 python board/server.py
```

`node board/loadtest_ui.cjs 1000,5000,10000,20000` measures the same in headless Chrome at each size;
add `--compare=<git-ref>` to test the older HTML against the same current server and synthetic data.
For example, a 10,000-card comparison against `93c9411` on 2026-11-05 did not observe the old
page's card elements before a 30-second browser-call timeout. The current page reached that state in
3.3 seconds; search keystrokes took a median 107 ms (worst 142 ms) to the next frame, and 20 scroll
loads had a median of 181 ms (worst 323 ms). These are single headless runs on the same machine,
using the current backend for both pages; card-ready time is a DOM measurement, not paint timing.

Subsequent optimizations remove a redundant server sort, skip the initial event debounce, and send
an opt-in compact snapshot to current browser clients. A 10,000-card full run sent 6.3 MB and
reached the first complete render at about 1.06 s on a warmed board; search keystrokes were 71 ms
median (107 ms worst). Later cold-start samples took about 1.5 to 3.2 s, with a 5.5 s outlier under
heavy machine load, so sub-second cold startup has not been established. `--startup-only --cold` measures startup without
warming the board; `--legacy-snapshot` uses the old wire format for comparison.

Earlier measurements on 2026-11-05 (headless Chrome, software rendering, on the worker PC while its stacks ran):

| Cards | Snapshot sent to the browser | Cards ready | Time in long tasks during live updates | Search keystroke, median (worst) | Hide done / failed | JS heap |
|---|---|---|---|---|---|---|
| 1,000 | 0.9 MB | 1.5 s | 1% | 42 ms (85 ms) | 25–31 ms | 4 MB |
| 5,000 | 4.6 MB | 1.7 s | 0% | 46 ms (97 ms) | 51–57 ms | 10 MB |
| 10,000 | 9.2 MB | 2.5 s | 2% | 64 ms (105 ms) | 52–170 ms | 17 MB |
| 20,000 | 18.4 MB | 3.3 s | 1% | 72 ms (384 ms) | 38–44 ms | 37 MB |

Keystroke times run from the input event to the next painted frame. In the page itself, filtering
10,000 cards takes 3–19 ms and rebuilding the columns another 15–30 ms. Before these changes, at
1,000 cards each live update blocked the page for up to 1.2 s, and at 5,000 it stopped responding.

The board scales further than the engines' listings: real backends still return at most 200 runs per
board on Hatchet and Prefect and 100 on Trigger.dev (see [Limits](#limits-of-this-prototype)). The
server's cost per refresh grows with the board: about 0.4 s of CPU for 10,000 cards.

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
- **Window and size.** Each board shows runs from the last 7 days: up to 200 per board on Hatchet and
  Prefect and 100 on Trigger.dev. The UI itself handles 20,000 cards (see [Big boards](#big-boards)).
- **Environments.** Trigger.dev production and local development use separate keys and tabs; Hatchet uses one tenant.
- **Queue position is computed, not promised.** It's the order of currently queued runs of the same task. Priority, concurrency keys and delays can reorder actual execution, and neither engine exposes an ETA.
- **Board names live in the browser.** The list of boards is kept in `localStorage`. A board exists as soon as one card is tagged with its name.
