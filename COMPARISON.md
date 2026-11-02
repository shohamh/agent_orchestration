# Hatchet vs Trigger.dev for agent orchestration

This compares Hatchet `v0.114.2` and Trigger.dev `v4.8.3`, both self-hosted with Docker Compose on the same machine on 2026-11-02.

- **Tested:** rows marked *tested* were run against this setup with the example code in this repo.
- **Docs/source:** the rest come from the official docs and the SDK/server source at those versions.

## TL;DR

- **Hatchet** is a durable task queue and workflow engine. Your workers are ordinary long-running processes (Python, TypeScript, Go, Ruby) that connect over gRPC.
  - It's light and simple to self-host.
  - It's **easy to run offline**.
  - It's a natural fit for **agents that must run on a specific PC with specific hardware**.
  - You own packaging and deploying your worker code.
- **Trigger.dev** is a "serverless background jobs" platform for **TypeScript**. You `deploy` code, and it builds an image and runs **each run in its own container**.
  - It has a richer developer experience: versioned deployments, a local dev mode, realtime streaming to frontends, input streams for steering, and AI-chat helpers.
  - It's **heavier to self-host and harder to run offline**.
  - The self-hosted Docker worker **can't pass devices into run containers**.
  - The self-hosted version lacks checkpoints, warm starts and autoscaling.
- **Recommendation:** for agents in an offline network, especially ones that talk to local hardware or are written in Python, **Hatchet is the better fit**. Trigger.dev is attractive for TypeScript-first teams building web-facing agents that stream to a UI, where per-run isolation and deploy versioning matter more than hardware access.

## Your three requirements, side by side

### 1. Agents that talk to hardware attached to the PC they run on

| | Hatchet | Trigger.dev (self-hosted, Docker) |
|---|---|---|
| Where agent code runs | **Your worker process, on whatever machine you start it.** Run `hardware_worker.py` natively on the PC, or in Docker with `devices: [/dev/ttyUSB0]`. It has full access to serial, USB, GPIB, cameras, GPUs and drivers. | In a **fresh container per run**, started by the supervisor on its Docker host |
| Device passthrough | n/a. It's your process, so it's whatever the OS allows. | **None.** The Docker supervisor creates run containers with only network, CPU and RAM settings: no devices, volumes or privileged mode (checked in `apps/supervisor/src/workloadManager/docker.ts`). You'd need to patch the supervisor or use Kubernetes with a device plugin. |
| Routing a task to "the PC with the device" | ✅ **Worker labels plus `desired_worker_labels`**: `required=True` means only workers with `device=sensor`, or pin a run to `station=station-a` at trigger time. *Tested:* `read-sensor` only ran on labelled workers, and the per-run pin landed on station-a. | ➖ **Regions / worker groups**: run a supervisor on each PC and trigger with `{ region: "..." }`. That picks the machine, but the code still can't reach the device from inside the container. |
| Serialized access to one device | ✅ `slots=1` on that worker. Concurrency keys are also available. | ✅ a queue with `concurrencyLimit: 1` |
| Workarounds on Trigger.dev | — | (a) Run a small **hardware bridge service** on each PC (HTTP/gRPC) and have tasks call it. (b) Use `trigger dev` on the PC, where runs execute as local processes with host access. That's dev-mode only, with no deploy versioning. |
| Verdict | **Natural fit** | **Indirect.** It needs a bridge service or a patched supervisor. |

### 2. Human-interruptible and steerable agents

| | Hatchet | Trigger.dev |
|---|---|---|
| Hard interrupt (stop now) | ✅ Cancel from the dashboard or `hatchet.runs.cancel(id)`. The task sees `ctx.is_cancelled` / `CancelledError`. Bulk cancel and **replay** are also available. *Tested:* status went to `CANCELLED`. | ✅ Cancel from the dashboard or `runs.cancel(id)`. Also bulk actions and replay. *Tested:* status went to `CANCELED`. |
| Steer a running agent (new instructions mid-run) | ✅ Push an event scoped to the run (`hatchet.event.push("agent:steer", {...}, scope=run_id)`). The durable task waits with `ctx.aio_wait_for(or_(SleepCondition(2s), UserEventCondition(...)))`. *Tested:* redirected the goal mid-run, then a graceful stop. | ✅ **Input streams**: `steer.send(runId, {...})` from outside, and `steer.once()`, `.on()`, `.peek()` or `.wait()` inside. *Tested:* redirect plus graceful stop. Self-hosted needed `STREAM_ORIGIN=http://webapp:3000` (found while testing), or a real DNS name for the server. |
| Approve / pause for a human (long waits) | ✅ durable `wait_for` on an event. The wait is durable: it survives worker restarts, and the worker slot is evicted while waiting. | ✅ waitpoint tokens (`wait.forToken`, completed through the API or a callback URL) and `steer.wait()`. **Self-hosted has no checkpoints**, so the run container stays alive and holds its CPU/RAM while it waits. |
| Live view of what the agent is doing | ✅ `ctx.put_stream(...)` with stream subscriptions, plus logs and the run timeline in the dashboard | ✅ run metadata (live), realtime streams (`streams.pipe`), React hooks, and the trace view |
| Built-in chat/agent UI helpers | ➖ none; you build the UI | ✅ `chat.*` / AI SDK helpers with steering of in-flight LLM steps (TypeScript) |
| Verdict | **Strong.** Steering is a durable event wait, and the waits survive restarts. | **Strong and more UI-ready**, but long human waits hold a container when self-hosted. |

### 3. Trace collection from agents (enabled in this setup)

| | Hatchet | Trigger.dev |
|---|---|---|
| Built-in trace store and UI | ✅ The engine has an OTel collector, enabled here with `SERVER_OBSERVABILITY_ENABLED=true`. Workers send spans through `HatchetInstrumentor` (`hatchet-sdk[otel]`). Spans are stored in Postgres and shown per run in the dashboard. | ✅ Always on. Every run has a trace view with spans and logs. Stored in Postgres here; ClickHouse is recommended for scale. |
| Custom spans (LLM call, tool call, device I/O) | `tracer.start_as_current_span("llm.call")`. *Tested:* `agent.step`, `llm.call`, `device.read` and `hatchet.durable.wait_for` were recorded. | `logger.trace("agent.step", async (span) => ...)`. *Tested:* `agent.step`, `inputStream.once()` and the `human steer` log were recorded. |
| Export to your own backend (OTLP) | ✅ add any exporter to the tracer provider. Here: `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318` | ✅ `telemetry.exporters` in `trigger.config.ts`. Here: the `AGENT_TRACES_OTLP_ENDPOINT` project env var |
| Shared view across both | ✅ *Tested:* both appear in the bundled **Jaeger** (`observability/`, UI on :16686). Services: `hatchet-example-worker`, `hatchet-hardware-station-a`, `trigger.dev`. | ✅ same |
| Auto-instrumentation | OTel Python instrumentations (httpx, requests, DB...) can be added to the same provider | `telemetry.instrumentations` (OpenAI, Prisma, HTTP...) |

## Operations (measured here)

| | Hatchet | Trigger.dev |
|---|---|---|
| Containers (idle) | 4 running (postgres, engine, dashboard, worker) | 11 running |
| RAM idle | **~0.7 GB** (incl. example worker) | **~2.0 GB** (webapp 750 MB, ClickHouse 390 MB, supervisor 280 MB, ...) |
| Upstream sizing advice | none strict (Postgres is the main cost) | 3+ vCPU / 6 GB webapp, 4+ vCPU / 8 GB worker host |
| Time to first successful run | ~2 min, 4 commands | ~10 min: secrets, magic-link login, org/project, PAT, docker login, image build+deploy |
| Datastores | Postgres only (RabbitMQ optional) | Postgres, Redis, ClickHouse, MinIO (S3), Docker registry, s2, Electric |
| Getting code to run | run your worker anywhere (`python worker.py`, a container, k8s) | `trigger deploy` → BuildKit image → registry → supervisor starts a container per run |
| Offline | easy: `docker save/load` | moderate: also needs the buildkit image, mirrored base images by digest, an npm mirror and `--network host` builds |
| Upgrades | bump `HATCHET_VERSION`; migrations run on `up` | `scripts/update-upstream.sh <tag>`; keep CLI/SDK version == server version |
| Gotchas hit while setting up | Git-Bash path rewriting; REST URL inside containers (`HATCHET_CLIENT_SERVER_URL`); trace collector off by default | `API_ORIGIN` and `STREAM_ORIGIN` unreachable from run containers (needed `INTERNAL_API_ORIGIN` and `STREAM_ORIGIN`), bind mounts on Docker Desktop, reserved port 9090, supervisor/bootstrap race, `TASK_DID_CONCURRENT_WAIT` |
| License | MIT | Apache-2.0 |

## General capabilities

| Capability | Hatchet | Trigger.dev |
|---|---|---|
| Languages | Python, TypeScript, Go, Ruby SDKs | TypeScript/JavaScript. Python scripts can run *inside* a TS task via a build extension. |
| Basic task + retries/backoff/timeouts | ✅ | ✅ |
| DAG workflows (declared dependencies) | ✅ first-class (`parents=[...]`), shown as a graph in the UI | ➖ written as code: `triggerAndWait` / `batch.triggerByTaskAndWait`, shown as a run tree |
| Child tasks / fan-out / fan-in | ✅ | ✅ (parallel waits must use `batch.*AndWait`) |
| Durable execution (survive restarts, long sleeps) | ✅ durable tasks: `sleep_for`, `wait_for` events, durable event log | ✅ on Cloud (checkpoints). **Self-hosted: no checkpoints**, so a waiting run keeps its container alive. |
| Event-triggered runs | ✅ push events → trigger workflows (with filters) | ➖ trigger via SDK/API; webhooks handled in your code |
| Cron / scheduled / delayed runs | ✅ | ✅ (declarative and imperative schedules, `delay`, TTL) |
| Concurrency control | ✅ per-workflow/per-key (group keys, strategies such as cancel-in-progress and round-robin) | ✅ queues + `concurrencyKey` (per-tenant/user queues) |
| Rate limiting | ✅ static and dynamic (per-key) rate limits | ➖ via queue concurrency, no token-bucket primitive |
| Priority | ✅ | ✅ |
| Worker routing (sticky / labels / affinity) | ✅ sticky sessions, worker labels, per-run desired labels | ➖ regions/worker groups (machine-level), machine presets (CPU/RAM size) |
| Per-run isolation and resource limits | ➖ runs share the worker process (slots) | ✅ container per run with enforced machine presets |
| Idempotency | ✅ (dedupe keys) | ✅ `idempotencyKey` with TTL |
| Streaming results / LLM tokens to a UI | ✅ `put_stream` / stream subscriptions | ✅ Realtime API + React hooks + streams v2 (s2), built for AI token streaming |
| Versioned deployments / rollbacks | ➖ you version your worker images | ✅ atomic versioned deploys, runs pinned to their version, promote/rollback |
| Local dev loop | run the worker locally against the server | ✅ `trigger dev`: hot reload, runs on your laptop against the server |
| Preview environments | ➖ (tenants/environments set up by hand) | ✅ dev / staging / prod / preview branches |
| Alerts | ✅ tenant alerting (email/Slack) | ✅ alerts (email/Slack/webhook) |
| Multi-tenancy | ✅ tenants, API tokens per tenant | ✅ orgs → projects → environments |
| Scaling out | run more workers anywhere; the engine scales with Postgres (RabbitMQ optional) | more supervisors/worker groups; **no autoscaling** self-hosted; Kubernetes via Helm for scale |
| Self-hosted parity with Cloud | close (same engine) | missing warm starts, checkpoints, autoscaling |

## Try it yourself

The same scenarios exist in both example projects:

| Scenario | Hatchet (`hatchet/example-worker/`) | Trigger.dev (`trigger.dev/example-project/`) |
|---|---|---|
| Single task with retries | `uppercase` | `uppercase` |
| Fan-out pipeline | `agent-pipeline` (DAG) | `agent-pipeline` (batch of child runs) |
| Uneven DAG (fan-out, chain, fan-in) | `research-report` (`research_dag.py`): 5 tasks with `parents=[...]` | `research-report` (`src/trigger/research-dag.ts`): parent awaits children; the entities -> risk chain becomes its own `entity-risk-branch` child so it can run beside `summarize` |
| Agent loop with waits | `agent-loop` (durable) | `agent-loop` (`wait.for`) |
| Steerable / interruptible agent | `steerable-agent` (`steering.py`) | `steerable-agent` (`src/trigger/steering.ts`) |
| Hardware-bound task | `read-sensor` on `hardware-station-a` / `-b` (`hardware_worker.py`) | not possible in-container; see the workarounds in section 1 |
| Demo script | `docker compose exec example-worker python demo_steer_and_hardware.py` | `npx tsx scripts/demo-steer.ts` (with `TRIGGER_SECRET_KEY`) |

Traces from both appear in each tool's own dashboard and in Jaeger at http://localhost:16686.
