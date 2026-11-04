# Declarative worker fleet design

## Intent and scope

Make machine registration, capability requirements and placement understandable and manageable in the board for both standalone cards and portable flows. Each step may run on a different machine. Use each engine's native placement where it satisfies these requirements; use the shared execution service for an engine where it does not. Hatchet native placement is the preferred direction, subject to the feasibility checks below. Preserve native engine orchestration, existing interactive sessions, cancellation, retries, step restart and workspace transfer.

## Fleet and registration

One registered execution worker represents a PC; it may serve multiple engines and belong to multiple named pools. The fleet tab lists workers, per-engine connection status, online state, pools, tags, slots and usage, and supports editing settings and refreshing capabilities. Registration starts with engine- and operating-system-specific instructions, followed by worker details, tag autocomplete with inline creation, and selection of multiple pools. A registration can be saved as awaiting connection; verify identity when it connects. Keep credentials out of the general fleet view.

The fleet registry is authoritative for editable settings (including pools, tags, slot count and resource thresholds). Workers report identity, health and capabilities and receive settings required for enforcement. Import existing YAML registrations, preserving IDs, tags, their existing pool, endpoint and credential configuration; the old pool becomes one membership. Preserve existing flows and selectors when migrating. Define an explicit migration and rollback procedure before implementation, including the default local worker and secrets; avoid duplicate registrations or dual mutable sources of truth.

## Capability and placement

Tags are manually declared attributes, distinct from detected software. A task may select one pool and require multiple tags; a match requires membership of that pool and every required tag, plus the relevant software. Worker-ID selection and existing flow-level defaults and step overrides remain supported. Explain mismatches in the card and flow creation interfaces; allow saving definitions even when nothing matches.

Detect supported agents at startup, daily and on manual refresh; run any overdue daily refresh when the worker reconnects. Present installed, absent and unknown/error states with timestamps. Installation means discoverable in the execution environment, accounting for agent-specific adapters and dependencies; it does not imply authentication. Report authentication readiness separately only where it can be checked reliably, otherwise unknown. Do not expose secrets. Automatically infer a straightforward program's first executable; allow an explicit declaration when the executable cannot be determined safely. Probe declared commands as needed and recheck the required software immediately before execution, using the same resolution and environment as execution (including the existing Python interpreter behavior).

Before launch, block when no registered worker can satisfy the requirements and explain how to resolve the mismatch. If a potentially compatible worker is offline, busy, or has unknown or stale capability results, allow launch and show that the task is waiting; start automatically once fresh checks and capacity permit. An ambiguous command requires an explicit executable requirement before launch. If software disappears after launch, wait for another suitable worker; show intervention needed if no registered worker can satisfy the requirement. A worker setting change affects future allocations, not running work.

## Capacity and live metrics

Show live machine-wide CPU and RAM usage for every connected worker. Each worker has configurable maximum CPU and RAM thresholds, both defaulting to 90%. Calculate each as a rolling average of the last 10 seconds. A worker is ineligible for new work if either average is at or above its limit. Missing, stale or insufficient measurements mean unknown capacity: wait for a fresh complete 10-second window. Do not interrupt running jobs when thresholds are exceeded. These are admission thresholds, not resource caps or predictions.

Use time-weighted samples against a monotonic clock. RAM usage is physical memory in use, using the operating system's available-memory measure so readily reclaimable memory is not treated as consumed; document and display the basis. Specify the sampling interval and freshness deadline during implementation and test gaps, reconnects and restarts.

Enforce the worker's slot limit across all engines sharing a PC. Serialize allocation per worker, and after starting work wait for a fresh 10-second measurement window before another allocation on that worker. Recheck requirements and capacity atomically at admission to prevent competing engines from overallocating slots. Show waiting reasons, including high CPU, high RAM, missing metrics, full slots, offline and missing or unknown software. Scheduling must remain cancellable and recover from worker or coordinator restarts; do not occupy engine execution slots indefinitely merely to wait for eligibility where the engine permits a queued alternative.

Engine assignment is not itself admission: a single per-PC gate must enforce reservations across native and shared paths. Define lease ownership, cleanup and fencing after crashes, and preserve the cooldown across reconnects or conservatively restart it. Queued cancellation and board state must remain correct without falsely reporting engine failure.

## Engine integration and feasibility gates

For Hatchet, prefer native worker assignment on each PC using native labels and slot management, while enforcing the fleet's cross-engine admission policy. Before committing to the detailed design, prove with the repository's pinned Hatchet SDK/server that per-step and per-run selectors, required rather than preferred labels, dynamic availability, cancellation, waiting and restarts work together. Native slots alone are insufficient to coordinate with other engines. If they cannot meet the contract, document the concrete limitation and use the shared execution service for the affected route; do not claim native placement succeeded.

The pinned Hatchet SDK has a five-minute default scheduling timeout; verify and configure its lifecycle so intended waiting does not silently turn into a failure. Design immutable flow-revision distribution and registration for remote native workers, including failed updates and old runs still in flight.

For Prefect and Trigger.dev, evaluate native mechanisms against per-step PC affinity, dynamic eligibility, mixed-engine slots and current deployment modes. Prefer them where the full contract is feasible; otherwise use the shared execution service. The current Trigger flow integration is limited to its local development worker; any production expansion is a separate explicit design decision. Keep the engine-neutral fleet UI and requirement semantics consistent regardless of adapter. A worker may need both a native engine connection and a local coordination component.

Existing workspace publications are stored on the coordinator; direct remote native execution must preserve the current isolated workspace, transfer, conflict handling, fencing, cancellation and restart guarantees rather than assuming the filesystem is shared. Engine credentials and remote registration require engine-specific setup instructions. Keep tokens and executable endpoints out of browser-visible run parameters and apply existing network authentication requirements.

Provide authenticated cross-PC artifact access or an equivalent mechanism, preserving the current publication locks, fencing and transfer limits. Make migration idempotent and protect ignored registry and credential files through migration and rollback.

## Acceptance and validation

- Register an offline worker, connect it, configure multiple pools and create/reuse tags in the UI. Add a second engine to the same PC without creating a duplicate.
- Migrate a representative existing registry, preserve secrets and selectors, and verify rollback. Show per-engine connectivity and actual executing worker for cards and flow steps.
- Verify supported agent detection, adapter prerequisites, unknown and absent results, startup/daily/manual refresh, offline timestamps, pre-execution rechecks and command resolution, including ambiguous commands.
- Exercise a flow with steps on different PCs and a standalone card, across supported engine modes. Prove engine-specific placement where claimed and shared-service fallback otherwise. Test registration, online/offline transitions, mismatches, queueing, cancellation and retries.
- Feed controlled CPU/RAM samples across both limits, unknown and stale windows; verify admission resumes only after a complete eligible window. Concurrent engine requests must honor one worker-wide slot limit and the 10-second post-start cooldown. Existing jobs must continue above limits.
- Verify file transfer, independent branches, restart, cancellation, authentication and no secret exposure. Document any engine mode or physical multi-PC scenario not actually tested.
