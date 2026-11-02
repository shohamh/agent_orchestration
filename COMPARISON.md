# Hatchet vs Trigger.dev vs Prefect for agent orchestration

This compares Hatchet `v0.114.2`, Trigger.dev `v4.8.3` and Prefect `3.9.1`, all self-hosted with Docker Compose on the same machine on 2026-11-02.

- **Tested:** rows marked *tested* were run against this setup with the example code in this repo.
- **Docs/source:** the rest come from the official docs and the SDK/server source at those versions.
- **Offline:** Prefect has not been offline-bundled yet.

## TL;DR

- **Hatchet** is a durable task queue and workflow engine. Workers are ordinary long-running processes (Python, TypeScript, Go, Ruby) that connect over gRPC.
  - It's light, starts runs fast and runs **easily offline**.
  - It has the most complete support for **durable**, steerable agents.
  - It has a **built-in trace viewer**.
  - Worker labels route work to **the PC with the hardware**.
- **Prefect** is a Python workflow orchestrator with a mature UI.
  - Workers and served flows run as **local processes on the PC**, so hardware access is natural.
  - Human input is first-class, including **typed forms rendered in the UI**.
  - It's the lightest server.
  - Tradeoffs: there is **no trace viewer in the server**, so use the shared Jaeger. A flow that's mid-run doesn't resume after a worker crash unless you design for it (`suspend_flow_run` + explicit task caching). Startup latency and retaining a process during human waits are acceptable for this use case.
- **Trigger.dev** is a "serverless background jobs" platform for **TypeScript**. `deploy` builds an image, and each run gets its own container.
  - It has the richest developer and web-UI tooling: versioned deploys, dev mode, realtime streams and AI-chat helpers.
  - Self-hosted, it **can't pass devices into run containers**.
  - It's the heaviest to run and the hardest to run offline.
- **Recommendation for your requirements** (offline network, hardware-bound agents, human steering, traces): **Prefect and Hatchet are both strong fits.** Startup time and keeping a process alive during waits do not decide this comparison, per your clarification.
  - Choose Prefect for Python workflows, the run graph, and built-in approval forms. Keeping the flow process alive is the straightforward supported way to preserve its live state while a human replies.
  - Choose Hatchet when durable recovery after worker restarts, multiple worker languages, or its integrated trace viewer matters. Crash recovery is a separate concern from whether a process stays up during a wait.
  - Trigger.dev fits TypeScript web-facing agents that don't touch local devices.

## Your three requirements, side by side

### 1. Agents that talk to hardware attached to the PC they run on

| | Hatchet | Prefect | Trigger.dev (self-hosted, Docker) |
|---|---|---|---|
| Where agent code runs | Your worker process on that PC (natively, or Docker with `devices:`) | A process worker or a `serve()` process on that PC. Each run is a local subprocess. | A fresh container per run, started by the supervisor |
| Device access | ✅ Whatever the OS allows | ✅ Whatever the OS allows | ❌ None. Run containers get no devices, volumes or privileged mode (`apps/supervisor/src/workloadManager/docker.ts`). |
| Routing to "the PC with the device" | ✅ Worker labels + `desired_worker_labels`, with an optional per-run station pin. *Tested.* | ✅ Work pool + one work queue per PC, plus a shared `any-station` queue. Pin with `run_deployment(..., work_queue_name="station-a")`. *Tested.* | ➖ Regions/worker groups pick the machine, but the run still can't reach the device |
| One operation at a time per device | ✅ `slots=1` | ✅ `--limit 1` on the worker, or a queue concurrency limit | ✅ queue `concurrencyLimit: 1` |
| Gotcha | — | A worker started with a queue that doesn't exist silently picks up **nothing**. Create queues first. | Workarounds: a hardware bridge service on each PC, or `trigger dev` (dev mode only) |
| Verdict | **Natural fit** | **Natural fit** | **Indirect** |

### 2. Human-interruptible and steerable agents

| | Hatchet | Prefect | Trigger.dev |
|---|---|---|---|
| Hard interrupt | ✅ Cancel from UI/API. *Tested:* `CANCELLED`. | ✅ Cancel from UI, or set state `Cancelling`. *Tested:* `CANCELLED`. | ✅ Cancel from UI/API. *Tested:* `CANCELED`. |
| Steer mid-run | ✅ Event scoped to the run, plus a durable `wait_for`. *Tested:* redirect, then stop. | ✅ `send_input(...)` / `receive_input(...)`, which polls (0.5 s here). *Tested:* redirect, then stop. | ✅ Input streams `steer.send()` / `steer.once()`. *Tested:* redirect, then stop. Needed `STREAM_ORIGIN` when self-hosted. |
| Approval / pause with input | ✅ Durable event wait. You build the form. | ✅ **`pause_flow_run(wait_for_input=Model)`**: the UI renders a typed form with a Resume button. *Tested:* resumed with input. | ✅ Waitpoint tokens / `steer.wait()`. You build the form. |
| Long human waits | Durable. The worker slot is released, and the wait survives worker restarts. | `pause` keeps the process alive, which is acceptable here. `suspend_flow_run` frees it, but the flow **re-runs from the start** on resume; explicitly cached tasks can reuse results. Persistence alone does not guarantee reuse. | Self-hosted: the container stays alive and holds CPU/RAM |
| Live view | `put_stream`, logs, run timeline | Logs, run timeline graph, artifacts | Live metadata, realtime streams, React hooks, trace view |
| Verdict | **Strongest durability** | **Best human-input UX** out of the box; polling and a retained process are acceptable here | **Most UI-ready for web apps** |

### 3. Trace collection from agents (enabled for all three)

| | Hatchet | Prefect | Trigger.dev |
|---|---|---|---|
| Built-in trace store and viewer | ✅ The engine's OTel collector (`SERVER_OBSERVABILITY_ENABLED=true`). Spans are shown per run. | ❌ None. The server stores logs and run state only. Spans are created by the SDK and must be exported. | ✅ Always on: a trace view per run |
| Custom spans | OTel `tracer.start_as_current_span` | OTel API (`prefect[otel]`) | `logger.trace(...)` |
| Export to OTLP backend | ✅ *Tested* (Jaeger) | ✅ via `opentelemetry-instrument`. Covers flow subprocesses too. *Tested* (Jaeger). | ✅ `telemetry.exporters`. *Tested* (Jaeger). |
| Spans seen in testing | `agent.step`, `llm.call`, `device.read`, durable waits | flow-run spans (named after the run), task spans, `agent.step`, `llm.call`, `device.read` | `agent.step`, `inputStream.once()`, steer logs |

All three report into the bundled Jaeger in `observability/` (http://localhost:16686).

## Operations (measured here)

| | Hatchet | Prefect | Trigger.dev |
|---|---|---|---|
| Containers (idle) | 4 | 5 (server, postgres, worker, 2 stations) | 11 |
| RAM idle | ~0.7 GB | **~0.2 GB** | ~2.0 GB |
| Time to first successful run | ~2 min | ~3 min | ~10 min (login, project, token, image build) |
| Datastores | Postgres (RabbitMQ optional) | Postgres (Redis for multi-server HA) | Postgres, Redis, ClickHouse, MinIO, registry, s2, Electric |
| Run start latency | ~1–5 s end-to-end incl. result fetch | **~3–20 s** (worker poll + new Python process per run) | ~0.3–0.4 s queue→start |
| Getting code to run | Start a worker anywhere | `serve()` or a worker + deployment (code from local path, git or image) | `trigger deploy` builds an image per version |
| Offline | Easy (done) | Likely easy: images + wheels, same shape as Hatchet. Not done yet. | Moderate (done): base images, npm mirror, builder |
| Upgrades | Bump `HATCHET_VERSION` | Bump `PREFECT_VERSION` (server and worker SDK follow it) | `scripts/update-upstream.sh <tag>` |
| Auth on the UI | Built-in users | None by default (basic auth via `PREFECT_SERVER_API_AUTH_STRING`) | Magic link / GitHub, email whitelist |
| License | MIT | Apache-2.0 | Apache-2.0 |

## General capabilities

| Capability | Hatchet | Prefect | Trigger.dev |
|---|---|---|---|
| Languages | Python, TypeScript, Go, Ruby | Python | TypeScript/JavaScript |
| DAG / fan-out | ✅ Declared `parents=[...]` | ✅ Tasks + `.submit()` futures (dynamic, in code) | ➖ In code with batch waits |
| Durable execution | ✅ Durable tasks, event log | ➖ Retries, caching and transactions. No mid-flow resume. | ✅ Cloud only (checkpoints) |
| Event-triggered runs | ✅ | ✅ Automations and event triggers | ➖ SDK/API |
| Schedules | ✅ | ✅ (very mature) | ✅ |
| Concurrency / rate limits | ✅ Keys, strategies, rate limits | ✅ Global and tag concurrency limits, rate limits | ✅ Queues and concurrency keys |
| Worker routing | ✅ Labels, sticky | ✅ Work pools and queues | ➖ Regions |
| Per-run isolation | ➖ Shared worker process | ✅ Process per run (Docker/k8s pools for containers) | ✅ Container per run |
| Streaming to a UI | ✅ | ➖ Logs and artifacts, no token streaming API | ✅ Best in class |
| Versioned deploys | ➖ Your images | ✅ Deployments with versions | ✅ Atomic versions |
| UI-driven human input forms | ➖ | ✅ | ➖ |

## Try it yourself

| Scenario | Hatchet (`hatchet/example-worker/`) | Prefect (`prefect/example-worker/`) | Trigger.dev (`trigger.dev/example-project/`) |
|---|---|---|---|
| Single task with retries | `uppercase` | `uppercase` | `uppercase` |
| Fan-out pipeline | `agent-pipeline` (DAG) | `agent-pipeline` (`.submit()`) | `agent-pipeline` (batch of child runs) |
| Uneven DAG (fan-out, chain, fan-in) | `research-report` (`research_dag.py`): 5 tasks with `parents=[...]` | `research-report/example` (`research_dag.py`): 5 tasks connected by futures, with a run graph in the Prefect UI | `research-report` (`src/trigger/research-dag.ts`): parent awaits children; the entities -> risk chain becomes its own `entity-risk-branch` child so it can run beside `summarize` |
| Agent loop with waits | `agent-loop` (durable) | `agent-loop` | `agent-loop` (`wait.for`) |
| Steerable / interruptible agent | `steerable-agent` (`steering.py`) | `steerable-agent` (`flows.py`) | `steerable-agent` (`src/trigger/steering.ts`) |
| Approval form | — | `approval-gate` (`pause_flow_run`) | — |
| Hardware-bound task | `read-sensor` on `hardware-station-a` / `-b` | `read-sensor/sensor` on the `hardware` pool, stations a/b | not possible in-container |
| Demo script | `docker compose exec example-worker python demo_steer_and_hardware.py` | `docker compose exec example-worker python demo.py` | `npx tsx scripts/demo-steer.ts` (with `TRIGGER_SECRET_KEY`) |
