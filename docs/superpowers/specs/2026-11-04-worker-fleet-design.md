# Declarative worker fleet design

## Intent and scope

Make machine registration, capability requirements and placement understandable and manageable in the board for standalone cards and portable flows. Each step may run on a different machine. When Hatchet is selected, a dedicated per-PC worker-capable token authenticates a real Hatchet SDK worker on that PC; compatible routed work executes there. When Prefect is selected, an idempotently created or reused work pool/queue is served by a per-PC Prefect process worker executing compatible routed work. Trigger.dev remains a shared Fleet relay bridge; deployed workers remain isolated containers, not per-PC native workers. Preserve the common per-PC admission gate, native orchestration, cancellation, retries, step restart and workspace-transfer guarantees.

## Fleet and registration

One registered execution worker represents a PC and may serve multiple selected engines and pools. The Fleet view reports per-engine readiness separately. When Hatchet is selected, a dedicated per-PC worker-capable token is delivered only to that PC's valid protected claim; it must not appear in the install command, public registration/status data or logs. Native readiness must be established before registration completes.

The Fleet registry is authoritative for settings. Claim/install/update is idempotent for the same identity; updates occur in place and restore the previous working setup on failure. Unregister revokes routing and prevents new work; repeated unregister and replacement registration are safe. Keep the Hatchet secret in a separately protected local file.

## Capability and placement

Tags are manually declared attributes, distinct from detected software. A task may select one pool and require multiple tags; a match requires membership of that pool and every required tag, plus the relevant software. Worker-ID selection and existing flow-level defaults and step overrides remain supported. Explain mismatches in the card and flow creation interfaces; allow saving definitions even when nothing matches.

Detect supported agents at startup, daily and on manual refresh; run any overdue daily refresh when the worker reconnects. Present installed, absent and unknown/error states with timestamps. Installation means discoverable in the execution environment, accounting for agent-specific adapters and dependencies; it does not imply authentication. Report authentication readiness separately only where it can be checked reliably, otherwise unknown. Do not expose secrets. Automatically infer a straightforward program's first executable; allow an explicit declaration when the executable cannot be determined safely. Probe declared commands as needed and recheck the required software immediately before execution, using the same resolution and environment as execution (including the existing Python interpreter behavior).

Before launch, block when no registered worker can satisfy the requirements and explain how to resolve the mismatch. If a potentially compatible worker is offline, busy, or has unknown or stale capability results, allow launch and show that the task is waiting; start automatically once fresh checks and capacity permit. An ambiguous command requires an explicit executable requirement before launch. If software disappears after launch, wait for another suitable worker; show intervention needed if no registered worker can satisfy the requirement. A worker setting change affects future allocations, not running work.

## Capacity and live metrics

Show live machine-wide CPU and RAM usage for every connected worker. Each worker has configurable maximum CPU and RAM thresholds, both defaulting to 90%. Calculate each as a rolling average of the last 10 seconds. A worker is ineligible for new work if either average is at or above its limit. Missing, stale or insufficient measurements mean unknown capacity: wait for a fresh complete 10-second window. Do not interrupt running jobs when thresholds are exceeded. These are admission thresholds, not resource caps or predictions.

Use time-weighted samples against a monotonic clock. RAM usage is physical memory in use, using the operating system's available-memory measure so readily reclaimable memory is not treated as consumed; document and display the basis. Specify the sampling interval and freshness deadline during implementation and test gaps, reconnects and restarts.

Enforce the worker's slot limit across all engines sharing a PC. Serialize allocation per worker, and after starting work wait for a fresh 10-second measurement window before another allocation on that worker. Recheck requirements and capacity atomically at admission to prevent competing engines from overallocating slots. Show waiting reasons, including high CPU, high RAM, missing metrics, full slots, offline and missing or unknown software. Scheduling must remain cancellable and recover from worker or coordinator restarts; do not occupy engine execution slots indefinitely merely to wait for eligibility where the engine permits a queued alternative.

Engine assignment is not itself admission: a single per-PC gate must enforce reservations across native and shared paths. Define lease ownership, cleanup and fencing after crashes, and preserve the cooldown across reconnects or conservatively restart it. Queued cancellation and board state must remain correct without falsely reporting engine failure.

## Engine integration and limits

Selected Hatchet means a real Hatchet SDK worker on the registered PC, authenticated by that PC's dedicated worker-capable token; compatible routed work executes there. Admin configures the worker-reachable gRPC host/port and TLS strategy separately from coordinator REST/dashboard URLs.

Selected Prefect means a per-PC Prefect process worker polling a work queue in an idempotently created or reused work pool. Compatible routed work executes there. Admin configures the API URL reachable from the PC, not merely a coordinator-internal Docker URL. Live endpoint reachability and failure reporting still require validation.

Trigger.dev remains a shared Fleet relay bridge. Its deployed workers remain isolated containers; do not advertise per-PC Trigger.dev native workers.

Native Hatchet and Prefect work and relay-dispatched work share one per-PC capacity gate. Reservations, leases, start authorization, cancellation and fencing must prevent over-allocation, stale starts and stale publication across engines. Coordinator-owned workspaces must be transferred through authenticated artifact access or an equivalent mechanism, preserving isolation, publication locks, transfer limits and conflict detection; never assume a shared filesystem.

Offline installation has release and load paths for `worker/native-runtime-windows.tar.gz` and `worker/native-runtime-linux.tar.gz`, each bundling CPython and the combined Hatchet/Prefect SDK requirements; `requirements-worker.txt` alone omits the engine SDKs. The Windows archive build passed locally. Linux archive generation remains unverified because the upstream CPython asset fetch returned HTTP 504. Native runtime packaging does not currently target macOS.

### Superseded historical feasibility probes

The 2026-11-04 Hatchet probes used SDK 1.44.0 and server image v0.114.2. Selector composition, run-level worker-ID routing, queued cancellation, long scheduling waits and restart lifecycle were inconclusive or inconsistent; an eight-minute timeout probe also logged an unexplained slot-replenishment timeout after a 310-second wait. These are historical risk evidence, not proof that the current design is impossible or tested.

The current local tests exercise service dispatch, local result ownership, cancellation and capacity fencing using the Python service and real Rust worker. Still unverified: live worker-reachable Hatchet gRPC/TLS and Prefect API endpoints, actual cross-PC execution, Prefect pool/queue reuse against the target server, combined native/relay contention with real engines, native engine-side cancellation/restart races, and artifact transfer between physical PCs. Do not describe those behaviors as live-tested.

## Acceptance and validation

- Register an offline worker, connect it, configure multiple pools, reuse the same PC for an additional engine, and verify per-engine readiness precedes registration.
- Confirm the valid PC claim alone receives the Hatchet token; verify command, public view, status and logs do not reveal it and the local secret file is protected.
- Verify idempotent update, rollback after partial native setup, repeated unregister and replacement registration. Missing or unreachable engine endpoints must produce actionable errors and leave the worker not ready.
- Verify Hatchet SDK execution from a physical PC over worker-reachable gRPC/TLS and Prefect process execution through the reused/created pool and queue; record exact versions and all untested modes.
- Test per-step placement, capability checks, cancellation, restart, retry and queue-timeout behavior rather than inferring it from historical probes.
- Exercise shared capacity admission with concurrent native and relay requests, CPU/RAM thresholds, cooldown, leases, cancellation and fencing. Verify transfer isolation, publication locks, conflicts and transfer limits across PCs.
- Verify offline installation from the complete bundle on Windows and Linux without preinstalled Python; document missing artifacts or scenarios not actually tested.
