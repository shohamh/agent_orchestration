# Declarative Worker Fleet Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Manage a multi-engine worker fleet declaratively and place cards and flow steps only on compatible workers with available shared capacity.

**Architecture:** A host-side registry owns worker settings and credentials references; an authenticated per-machine agent reports capabilities and enforces a shared admission gate. Engine adapters translate the same requirements into native placement where proven viable, falling back to the existing shared execution service otherwise. The board presents registration, status, resource metrics, placement explanations and waiting state.

**Tech Stack:** Python 3, aiohttp, YAML, JavaScript, Hatchet SDK 1.44.0, Prefect 3.9.1, Trigger.dev 4.8.3; existing unittest and Node browser tests.

**Spec:** `docs/superpowers/specs/2026-11-04-worker-fleet-design.md`

## Global Constraints

- Existing cards, flow selectors, restarts, artifacts, authentication, interactive inputs and native DAG behavior remain supported.
- One registered PC can serve multiple engines and pools; settings have one authoritative fleet registry.
- Requirements match a selected pool, all requested tags and detected executable or agent requirements.
- CPU and RAM defaults are 90% each, time-weighted over 10 seconds; either limit or unknown metrics blocks admission; never stop running work.
- Slots are shared across engines; serialize admission and require a fresh 10-second window after each start.
- Capability refresh runs at startup, daily, manually and on reconnect if overdue; software rechecks before execution.
- Block launch for a confirmed absence of any matching worker; save drafts and wait for potentially matching offline, busy or unknown workers.
- Do not expose credentials or worker endpoints in public catalogs or task payloads.

## Review Focus

- Duplicate IDs and interrupted migration must not lose the existing registry or secrets (Task 2).
- CPU sample gaps, process restart and clock changes cannot make stale measurements eligible (Task 4).
- Two engines competing for the final slot cannot both start work (Task 4).
- A queued task cancelled while its worker is offline cannot later start (Task 5).
- A flow revision or worker update while old work runs cannot mix artifacts or revisions (Task 6).

## File map

- `agents/flowkit/workers.py`: existing registry, catalog and selection compatibility facade.
- `agents/flowkit/fleet_registry.py`, `capabilities.py`, `capacity.py`: registry migration, probes and admission respectively.
- `agents/flowkit/worker_service.py`, `distributed.py`, `flow_host.py`: authenticated worker protocol, routing and host APIs.
- `agents/flow_worker_hatchet.py`, `flow_worker_prefect.py`, `agents/hatchet_worker.py`, `agents/prefect_worker.py`, `trigger.dev/example-project/src/flows-runtime.ts`: engine integration for flows and standalone cards.
- `board/server.py`, `board/flows_client.py`, `board/static/index.html`, `board/static/flow-builder.js`, new `board/static/fleet.js` and `fleet.css`: fleet UI and launch feedback.
- `agents/tests/`, `board/static/*.test.cjs`, `board/workers_smoke_test.py`: focused tests and end-to-end checks.
- `docs/workers-and-context.md`, `workers.example.yaml`: operator guidance and migration instructions.

### Task 1: Verify native routing and select adapters

**Files:** Create `agents/tests/test_fleet_engine_feasibility.py`; update `docs/workers-and-context.md` with results.

**Interfaces:** Produces a documented per-engine decision: native placement or shared-service fallback, including exact supported engine modes. Later tasks consume this decision; no unsupported native feature may be assumed.

- [ ] Add disposable integration checks for pinned Hatchet strict label matching, per-step and runtime overrides, worker disconnection, queued cancellation, label updates and waiting beyond its default five-minute scheduling timeout. Record installed server and SDK versions.
- [ ] Run `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_fleet_engine_feasibility -v` with an isolated Hatchet stack; expected: each supported assertion passes or the output identifies the unsupported operation. Do not represent skipped or unavailable live checks as passed.
- [ ] Check Prefect and Trigger current configured modes for per-step PC selection and document evidence; adopt native placement only where every spec guarantee can be preserved. Document fallback, artifact access, flow revision distribution, shared admission and engine-queue timeout implications.

### Task 2: Fleet registry and migration

**Files:** Create `agents/flowkit/fleet_registry.py`, `agents/tests/test_fleet_registry.py`; modify `agents/flowkit/workers.py`, `agents/flow_host.py`, `workers.example.yaml`.

**Interfaces:** `load_fleet() -> list[dict]`; `save_worker(worker: dict) -> dict`; `migrate_legacy_registry() -> dict`. Public worker objects include `id`, `pools`, `tags`, `slots`, `cpu_limit`, `ram_limit`, engine connections and online state, but no URL or credentials.

- [ ] Add unittest cases for idempotent migration of one legacy pool into a list, the implicit local worker, custom registry paths, duplicate IDs and interrupted writes; verify the original file and credential references survive rollback and no token appears in public output.
- [ ] Run `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_fleet_registry -v`; expect failure until the module exists.
- [ ] Implement atomic persistent registry writes, default limits 90/90, validation and versioned migration; keep old `{pool: ...}` selectors working. Add authenticated host CRUD and sanitized catalog routes and preserve secret storage on the coordinator.
- [ ] Rerun the focused test and `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_distributed -v`; expect both to pass.

### Task 3: Software inventory and requirements

**Files:** Create `agents/flowkit/capabilities.py`, `agents/tests/test_fleet_capabilities.py`; modify `agents/flowkit/definition.py`, `agents/flowkit/worker_service.py`, `agents/flowkit/executor.py`.

**Interfaces:** `probe_agent(name: str) -> dict`; `probe_command(name: str) -> dict`; `infer_command(run: str) -> str | None`; each probe reports `status` (`installed`, `absent`, `unknown`), `checked_at`, and separately `auth_status`. Extend a program step with an explicit `requires_command` when inference is ambiguous.

- [ ] Test supported agents including demo, Codex/Claude adapters, `opencode2`, missing Node/adapter and shell-specific launch behavior; test `python` interpreter resolution, a missing executable, an ambiguous shell expression and offline/stale results without exposing credentials.
- [ ] Run `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_fleet_capabilities -v`; expect initial failure.
- [ ] Implement probes without executing arbitrary user commands, persisted timestamped inventory, startup/daily/manual refresh and overdue refresh on reconnect. Reuse actual executable resolution and keep authentication unknown unless reliably verified.
- [ ] Rerun the focused test plus `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_flowkit agents.tests.test_acp -v`; expect all to pass.

### Task 4: Resource sampling and atomic admission

**Files:** Create `agents/flowkit/capacity.py`, `agents/tests/test_fleet_capacity.py`; modify `agents/flowkit/worker_service.py`.

**Interfaces:** `CapacityGate.observe(cpu_percent: float, ram_percent: float, monotonic_time: float) -> None`; `CapacityGate.try_acquire(job_id: str) -> dict`; `CapacityGate.release(job_id: str) -> None`. Admission returns an acquired lease or a structured waiting reason; all engines on a PC use the same gate.

- [ ] Test 10-second time-weighted samples, 60% CPU/70% RAM limits, equality at a threshold, unknown or stale data, clock and restart gaps, 90/90 defaults, CPU/RAM recovery and running-job continuity.
- [ ] Test simultaneous admissions through distinct engine adapters with one remaining slot, job cancellation, lease expiry/fencing and the 10-second cooldown across reconnects. Assert at most one job starts and no stale lease publishes.
- [ ] Run `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_fleet_capacity -v`; expect initial failure.
- [ ] Implement machine-wide sampling, available-memory-based physical RAM usage, monotonic time weighting and one crash-safe local admission gate; explicitly document sampling frequency, stale deadline and cooldown persistence. Enforce gate at actual start, not only an earlier health check.
- [ ] Rerun the focused test; expect all to pass. Verify the UI-facing metrics include timestamps and unknown states.

### Task 5: Compatibility, queueing and cancellation

**Files:** Create `agents/flowkit/fleet_matching.py`, `agents/tests/test_fleet_matching.py`; modify `agents/flowkit/workers.py`, `agents/flowkit/distributed.py`, `agents/flow_host.py`, `board/server.py`.

**Interfaces:** `match_worker(worker: dict, requirements: dict) -> dict` returns `compatible`, `unknown` or `incompatible` with reasons; `validate_launch(requirements: dict, workers: list[dict]) -> dict` returns `ready`, `wait` or `blocked`.

- [ ] Test pool plus all tags, ID selection and step overrides; verified absence versus stale/offline/unknown; busy/high-load waiting; missing explicit requirement; disappearance between launch and start; cancel while offline and ensure no later execution.
- [ ] Run `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_fleet_matching -v`; expect initial failure.
- [ ] Implement launch validation and a cancellable, restart-safe waiting lifecycle with visible reasons and pre-execution rechecks. Queue without consuming engine execution capacity indefinitely when the adapter supports it; document any unavoidable fallback constraint.
- [ ] Rerun the focused test and `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_distributed agents.tests.test_restart_runtime -v`; expect all to pass.

### Task 6: Engine adapters and workspace integrity

**Files:** Modify `agents/flow_worker_hatchet.py`, `agents/flow_worker_prefect.py`, `agents/hatchet_worker.py`, `agents/prefect_worker.py`, `agents/flowkit/distributed.py`, `agents/flowkit/restart.py`, `trigger.dev/example-project/src/flows-runtime.ts`; add `agents/tests/test_fleet_adapters.py`.

**Interfaces:** Every engine adapter accepts the Task 5 requirement object and submits execution through the Task 4 admission gate; reports the actual PC, engine connection and waiting reason to the board. Preserve the current flow output and event schemas.

- [ ] Test standalone cards and multi-PC flow steps for each supported engine mode, required labels, overrides, retries, interactive input, cancellation and waiting past the Hatchet default timeout. Test simultaneous engines against a single worker gate.
- [ ] Test authenticated remote artifact access, immutable flow revisions, an update failing while an old run is in flight, branch conflicts and restart fencing. Assert no stale attempt publishes and no remote worker assumes the host's local workspace path.
- [ ] Run `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_fleet_adapters -v`; expect initial failure.
- [ ] Implement the Task 1 proven adapter choices, retaining fallback for unsupported native semantics. If native remote execution is selected, implement authenticated artifact and immutable revision access with the existing transfer limits and publication guarantees.
- [ ] Rerun the focused test, `agents/.venv/Scripts/python.exe -m unittest agents.tests.test_distributed agents.tests.test_restart_runtime -v`, and the existing engine-specific smoke tests on isolated services; distinguish unavailable engine tests from passing tests.

### Task 7: Fleet management and registration UI

**Files:** Create `board/static/fleet.js`, `board/static/fleet.css`, `board/static/fleet.browser.test.cjs`; modify `board/static/index.html`, `board/server.py`, `board/flows_client.py`.

**Interfaces:** Board routes proxy the authenticated host fleet API. Fleet UI consumes sanitized worker objects from Task 2, inventory from Task 3 and capacity state from Task 4.

- [ ] Add browser tests for registering an offline PC with multiple engines and pools, engine/OS instructions, tag autocomplete and inline creation, connecting the same PC to a second engine, settings edits, manual refresh, per-engine status, 90/90 defaults, live metrics and unknown/stale values. Assert no credentials appear in fleet listings.
- [ ] Run `node --test board/static/fleet.browser.test.cjs`; expect initial failure.
- [ ] Implement the fleet tab, registration form, settings and accessible live CPU/RAM presentation; preserve startup instructions and protected token provisioning and show refresh timestamps.
- [ ] Rerun the focused browser test and `node --test board/static/chat.browser.test.cjs`; expect both to pass.

### Task 8: Card and flow UX, documentation and verification

**Files:** Modify `board/static/index.html`, `board/static/flow-builder.js`, `board/static/flow-builder.browser.test.cjs`, `board/workers_smoke_test.py`, `docs/workers-and-context.md`, `agents/README.md`.

**Interfaces:** Card and flow creation consume Task 5 launch validation; program steps expose `requires_command`; worker selectors preserve current default/override behavior.

- [ ] Add browser tests for both standalone cards and flows: matching pool/tags/software, clear incompatibility reasons, save-but-block, offline/unknown/busy waiting, ambiguous command declaration, step overrides and last detected timestamps.
- [ ] Run `node --test board/static/flow-builder.browser.test.cjs`; expect the new cases to fail before implementation.
- [ ] Implement compatibility explanations and live waiting status, then update setup, migration/rollback, per-engine registration, metric semantics, limits, and fallback documentation.
- [ ] Run `agents/.venv/Scripts/python.exe -m unittest discover -s agents/tests -v`, `python -m unittest discover -s board -p "test_*.py" -v`, and `node --test board/static/*.test.cjs`; expect normal exit and all passing. Run `board/workers_smoke_test.py` against isolated services for each available engine and test two real PCs when available; record exact commands and distinguish missing infrastructure from passed validation.
