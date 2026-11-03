# Prefect 3 (self-hosted, Docker Compose)

Prefect `3.9.1`: the open-source API/UI server with Postgres, plus Python workers. Brought up and tested on 2026-11-02 against the same agent requirements as Hatchet and Trigger.dev. Included in the offline bundle (`../offline/bundle.sh`). The bundled worker image builds with networking disabled, and every requirement test passed on a Docker network with no internet route (2026-11-02).

| URL / port | What |
|---|---|
| http://localhost:4200 | Prefect UI + REST API (`/api`). No login by default. |

## Setup

```bash
cd prefect
cp .env.example .env
docker compose up -d --build        # server, postgres, setup (pool + deployment), worker, 2 hardware stations
docker compose exec -T example-worker python -u demo.py
```

| Service | Role |
|---|---|
| `prefect-server` | API, UI, scheduler (`prefect server start`). Analytics are off (`PREFECT_SERVER_ANALYTICS_ENABLED=false`). |
| `postgres` | State store |
| `setup` | One-shot: creates the `hardware` process work pool and its queues, then registers the `read-sensor/sensor` deployment |
| `example-worker` | `serve()`s the agent flows as deployments: `uppercase`, `agent-pipeline`, `research-report`, `agent-loop`, `steerable-agent`, `approval-gate` |
| `hardware-station-a/-b` | Process workers on the `hardware` pool. Each listens on its own queue plus `any-station`, with `--limit 1`. |

## Try the UI

Open http://localhost:4200 (no login by default). Under **Deployments**, select
`research-report/example`, choose **Run**, and enter a topic. Open the resulting run's
graph to inspect the five tasks, dependencies, logs and timings. The deterministic task
bodies match the Hatchet/Trigger.dev fixtures; they do not call a real LLM.

For the approval form, run `approval-gate/example` with an action such as `inspect sensor`.
When it pauses, choose **Resume** and fill `approved` and `comment`. This demo returns the
decision; it does not perform a hardware operation. Run `steerable-agent/example` with
`turns: 100` to inspect live logs and try **Cancel**. Redirect/stop messages are exercised
by `demo.py` using `send_input`; the stock UI's Resume form applies to paused runs.

The Agent Board (port 8090, `../board`) also supports Prefect. Cards are flow runs tagged `board:<name>`; its graph shows
task runs with the data dependencies Prefect recorded; steering uses flow run inputs; and coding-agent
sessions run in `../agents/prefect_worker.py` on this PC. Prefect's own UI stays on port 4200.

## The requirements

| Requirement | How | Result |
|---|---|---|
| **Hardware on the run's PC** | Run a process worker (`prefect worker start --pool hardware --work-queue <pc> --work-queue any-station --limit 1`) natively on each PC, or in Docker with `devices:`. `run_deployment(..., work_queue_name="station-a")` pins a run to one PC. | Simulated stations read `/dev/urandom`. Pinned runs landed on station-a and station-b; real USB/serial hardware has not been tested. Containers need explicit device mappings; native workers use OS permissions. |
| **Steer** | `send_input(SteerMessage(...), flow_run_id=...)` from outside. Inside the flow, `receive_input(SteerMessage, timeout=..., poll_interval=0.5)` runs between steps. | ✅ Goal redirected mid-run, then a graceful stop |
| **Approve / pause for a human** | `pause_flow_run(wait_for_input=Approval)`. The UI shows **Resume** with a generated form. From code, use `resume_flow_run(id, run_input={...})`. | ✅ Paused, resumed with input |
| **Interrupt** | UI **Cancel**, or set the state to `Cancelling`. The worker or runner kills the run's process. | ✅ `CANCELLED` |
| **Traces** | Prefect creates OTel spans for flow and task runs, and custom spans work with the normal OTel API. **The server has no trace store or viewer.** Export with `opentelemetry-instrument` (in `prefect[otel]`) to an OTLP backend, using the `docker-compose.otel.yml` overlay to send to `../observability` (Jaeger). | ✅ Flow-run, task-run, `agent.step`, `llm.call` and `device.read` spans reached Jaeger from all three workers. |

## Gotchas found while setting up

- **A worker with an unknown work queue silently does nothing.** `prefect worker start --work-queue station-a` did not create `station-a`. The worker then picked up no runs at all, including runs on its *other*, existing queue, and logged no error. Create queues up front, as `setup` does.
- **Results aren't readable remotely by default.** Set `PREFECT_RESULTS_PERSIST_BY_DEFAULT=true` and put the storage path on storage every worker shares (here the `results` volume). Otherwise `state.result()` fails for runs that executed in another container.
- **Start latency.** Runs initially measured **3–20 s** after being triggered: workers and runners poll, and each run is a fresh Python subprocess. This is informational; startup speed is not a selection criterion for this setup.
- **Steering polls too.** `receive_input` polls the API (`poll_interval`). Messages arrive within that interval, not instantly.
- **Spans are named after the run.** Prefect names flow-run spans after the run (`deft-zebu`), so search Jaeger by service or by attributes.
- **Human waits.** `pause_flow_run` keeps the flow process alive and preserves its in-memory state, which is acceptable for this use case. This does not provide recovery from process or machine crashes. `suspend_flow_run` exits and reruns the flow from the start after resuming; use explicit caching/idempotency if choosing that model. Persisting results alone does not make every task replay-safe.
- **Approval form description.** In this server's V1 UI, an input without a saved description showed an empty Resume dialog and logged `Flow run input description not found`, even though API resume worked. The example supplies `Approval.with_initial_data(description=...)` so both the description and schema are saved. `demo.py` checks both before resuming.
- **Busy-host database connections.** With all three stacks running and the Windows host low on memory, the default 5-second database connection timeout produced HTTP 500s and failed a paused run. The server now allows 30 seconds (`PREFECT_DATABASE_CONNECTION_TIMEOUT`). This tolerates slower connections; it does not guarantee operation under resource exhaustion.

## Trace collection

From the repository root (PowerShell, Bash or another shell):

```sh
docker compose -f observability/docker-compose.yml up -d
docker compose -f prefect/docker-compose.yml -f prefect/docker-compose.otel.yml up -d --build
docker compose -f prefect/docker-compose.yml -f prefect/docker-compose.otel.yml exec -T example-worker python -u demo.py
```

Open http://localhost:16686 and select `prefect-example-worker`,
`prefect-hardware-station-a` or `prefect-hardware-station-b`. Without this overlay the
base stack runs without exporting traces. The overlay needs the shared `otel` network
created by the observability stack.

## Validation

`demo.py` asserts the report output and API-recorded task dependencies, an empty-source
report, baseline examples, station routing/device reads, redirect plus graceful stop,
hard cancellation, and both approval and denial. An assertion, deployment error or timeout
exits nonzero. Run it after upgrades; a healthy API alone does not prove worker readiness.
Use `python -u demo.py --only hardware steering cancel approval` inside the worker to run
selected scenarios after an interruption. Choices are `dag`, `examples`, `hardware`,
`steering`, `cancel` and `approval`; omitting `--only` runs all of them.

For workers on separate physical PCs, replace the local shared `results` Docker volume
with storage all workers and result readers can reach. That volume only shares data on
this Docker host. The example sensor path is Linux-specific; set `SENSOR_DEVICE` and
adapt the device driver for your real hardware/OS.

## Upgrading

Bump `PREFECT_VERSION` in `.env`, then run `docker compose up -d --build`. The same
setting selects the server image, worker base image and worker SDK with OTel extras.
The server migrates its database on start. Back up Postgres first and rerun `demo.py`
afterwards; future releases still need their own compatibility check. Offline bundling
remains deferred.
