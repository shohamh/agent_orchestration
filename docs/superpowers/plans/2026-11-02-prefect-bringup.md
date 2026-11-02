# Prefect bringup continuation

Goal: finish the functional Prefect comparison already requested in the Claude transcripts.
Startup latency and keeping a process alive during human waits are acceptable to the user.
Keep the existing Python/Compose architecture, upstream APIs, and unrelated ACP work intact.
Offline packaging remains deferred. Work in this checkout because it owns the running stack.

- [x] Extend `prefect/example-worker/demo.py` with assertions and the missing research DAG
  scenario. Run against the current stack and confirm the missing deployment fails.
- [x] Add `research_dag.py`, matching the five-task Hatchet graph and deterministic fixture.
  Register it in `serve.py`. Check real task dependencies and outputs through the API.
- [x] Pass `PREFECT_VERSION` into every worker build and install the matching SDK/OTel extras.
  Render Compose with an alternate version to verify propagation without upgrading the stack.
- [x] Rebuild and rerun all scenarios: task, fan-out, uneven DAG, loop, shared/pinned
  hardware queues, redirect/stop, cancel, and approval/denial. Verify current-run Jaeger spans.
- [x] Update Prefect instructions and both comparisons to reflect the user's priorities.
  Record verification evidence, simulated hardware limits, and remaining scope.

Validation uses real deployments, shared result storage, API task-run records and Jaeger;
no LLM credentials or physical devices are needed. Review timeouts, worker readiness,
empty source input, deny approval, and version consistency. Do not alter other stacks.

Evidence and the initial failures/corrections are recorded in `prefect/VALIDATION.md`.
All scenario groups were verified across resumed batches; no claim that the first full
invocation exited successfully. Fixed the UI input-description issue with the supported
RunInput API, and made the database connection deadline configurable after failures on
this busy host. Those changes remain within functional bringup; no upstream forks.
