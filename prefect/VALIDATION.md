# Prefect functional validation — 2026-11-02

Stack: Prefect 3.9.1 / Python 3.12 / Postgres 16, with the OTel overlay and shared Jaeger.
This continuation read the three Claude transcripts from this project's local session
directory before making changes. Offline packaging and the separate ACP chat work were
outside this task.

## Functional evidence

| Check | Evidence |
|---|---|
| Research DAG | Run `183cfbae-543c-42db-8bfb-a505cba6ee84`: five completed tasks, eight expected direct dependency edges, valid parent/child timing, expected report. Visually inspected the run graph. |
| Empty source list | Run `b4e00e33-2ff0-45d6-9980-2cf7a4c1d2fb`: empty report, no entities or risk signals, low risk. |
| Uppercase and fan-out examples | Runs `bf7179ce-ea23-40dc-8d5b-0e6b255a5d7e` and `8bf6f54e-bfd9-4622-bde8-d93a79cb8d59`: expected outputs asserted. |
| Agent loop | Run `e7de56ad-694d-47b7-906e-b3a965bd5f9c`: completed; all three expected history entries checked by reading its persisted result after a client read failed. |
| Any-station routing | Run `c9aa9a59-38d8-494a-b4fa-98a81a7b423f`: station-a read four bytes from `/dev/urandom`. |
| Pinned stations | Runs `8b7441da-ca0e-4c39-b664-50053cda0e8b` and `13e1661c-43d6-42a1-9311-52bd00f808b2`: station-a and station-b respectively, with four-byte reads. |
| Redirect and stop | Run `e691bbee-b7ed-458f-96c2-fe99a79f2e0b`: redirected to `focus on errors only`, then stopped early; transcript and result asserted. |
| Hard interrupt | Run `a96eed82-7900-432a-86da-f1e64718ef96`: reached `CANCELLED`. |
| Approval and denial | Runs `e37e3ebe-f4c7-459a-8223-7f8ba43e4aa9` and `65624192-12d3-4eab-88cf-c7275e8ddcc0`: resumed with true/false and returned the decisions. |
| Traces | Queried Jaeger for spans starting after 17:58 UTC. Verified all five research task span names, `agent.step`, `llm.call`, and `device.read` from both hardware workers. First research trace: `b7d574a8064ced10e7a990343cf21cff`. |
| Version selection | Rendered Compose with `PREFECT_VERSION=3.9.2` without deploying it: server image and all four build consumers followed that version. Running worker SDK remains 3.9.1. |

## Corrections and verification boundaries

- The original test failed with a missing `research-report/example` deployment before
  the new DAG was added. The updated DAG passed again as run
  `ececa00f-f1ee-4524-ab3c-122f72114f8e`, using bounded polling for task records.
- An initial full demo stopped on an API HTTP 500 while fetching the loop run. The
  run itself completed. Server logs traced failures to asyncpg connection timeouts;
  neither Postgres nor the server had restarted or been OOM-killed. Windows reported
  about 210 MB free physical memory while all stacks were running. Direct database
  connections later succeeded. The server's default five-second connection deadline
  was increased to a configurable 30 seconds. The live setting was checked, and 15
  consecutive API reads passed afterwards. This is not a capacity/stress guarantee.
- The first paused UI demo failed later on an API 500. API-only approval tests had
  also missed a separate V1 UI issue: the saved input schema existed, but the optional
  description was absent and the Resume form was empty. The flow now calls
  `Approval.with_initial_data(description=...)`. The corrected paused demo
  `398e5bae-a974-496f-b0d2-da9eee579a30` saved both inputs and displayed the description
  and Approved control in the UI. It was left for the user to resume.
- The final approval checks also verify the saved schema and description before
  resuming. Functional groups can be selected with `demo.py --only ...`, allowing
  unfinished checks to resume without retriggering scenarios already verified.
  The first metadata check exposed a transient 404 and initially caught the wrong
  exception type. After fixing the bounded 404 retry, `--only approval` exited zero:
  `400cca49-babc-41fe-a30d-cbe5aba9b879` approved and
  `bc8516d7-3ed1-4cd0-bcd5-3eb6a49010d7` denied, both with schema/description assertions.
  The interrupted check run `03c248a7-6dce-4c89-9337-4af070ffb715` was resumed with a
  denial and completed, so it was not left holding a paused process.
- Physical hardware, cross-PC result storage, process-crash recovery and offline
  transfer were not tested. Hardware stations are containers on one Docker host.
- Independent read-only review found the task-recorder race corrected above; it also
  confirmed the Compose inheritance, version argument and DAG wiring.

Commands and raw local evidence are in the ignored `dist/` folder:
`prefect-demo.log` (contains the initial API failure), `prefect-remaining.log`,
`prefect-final.log` (includes the metadata-read test failure), `prefect-approval-final.log`,
`prefect-trace-evidence.json`, `prefect-api-probe.json`, and
`prefect-run-evidence.json`. The reusable test is `prefect/example-worker/demo.py`.

Upstream references: [interactive flow API](https://reference.prefect.io/prefect/flow_runs/),
[input descriptions in 3.9.1](https://github.com/PrefectHQ/prefect/blob/3.9.1/src/prefect/input/run_input.py),
and [batched task recording in 3.9.1](https://github.com/PrefectHQ/prefect/blob/3.9.1/src/prefect/server/services/task_run_recorder.py).
