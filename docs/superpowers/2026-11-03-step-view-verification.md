# Live board and step view verification (2026-11-03)

Continues the two Claude sessions that exhausted their usage allowance. Preserves their shared
uncommitted board/live-update changes and portable-flow integration.

## Automated checks

- `agents/.venv/Scripts/python.exe -m unittest discover -s board -p 'test_*.py'`: 34 passed.
- `agents/.venv/Scripts/python.exe -m unittest discover -s agents/tests -p 'test_*.py'`: 34 passed.
- `agents/.venv/Scripts/python.exe -m unittest discover -s agents -p 'test_*.py'`: 4 passed.
- `node --test board/static/chat.test.cjs board/static/chat.browser.test.cjs`: 11 passed.
- Trigger project: `npx --no-install tsc --noEmit -p .` passed.

Chromium checks permissions, typed questions, Continue/Mark done, per-step input routing, scrolling
program logs, latest-100/1000 paging, patched older tool results, completed-to-completed DAG replay,
and disabled unsupported backend capability handling. A 390px screenshot was inspected: the step
chat composer is visible. The physical Android keyboard remains untested.

## Native Hatchet dependency replay

Run `40ff9aee-f793-4e80-bc7e-09c2ec94a52e` (`research-flow` on `native-restart`). Replayed only the
`summarize` task with `hatchet.runs.aio_replay(task_external_id)`. Engine timestamps changed only
for `summarize` and its dependent `write_report`; fetch_sources, extract_entities and assess_risk
kept their timestamps. Outputs were constant across attempts.

The installed engine's [recursive replay SQL](https://github.com/hatchet-dev/hatchet/blob/v0.114.2/pkg/repository/sqlcv1/tasks.sql#L804-L858)
walks its persisted dependency edges. Its [regression test](https://github.com/hatchet-dev/hatchet/blob/v0.114.2/pkg/operator/dagoperator/dag_test.go#L465-L503)
also covers unchanged outputs and preserved independent branches.

## Runtime extension on Prefect and Trigger.dev

`board/step_smoke_test.py` used a disposable program → interactive demo agent → program flow.
Both source and restarted runs exercised approval, question, Continue, Mark done and streamed rows.
The restarted flow reused the prerequisite's exact result and preserved its `context.txt` file.
The final program reran and read that copied file.

- Prefect source `741efcea-7fd2-4dbf-ad2d-2b3f2eb99032`, restart
  `666f1472-2e24-4434-90d3-2ec430c255bb`.
- Trigger source `run_cmw0at58d00b34go2ooakl6jg`, restart `run_cmw0atohg00bn4go2mu19pxrw`.

The board only forwards IDs to the flow host. Runtime tests cover exact/null/large saved outputs,
definition mismatch, absent workspace/results, active siblings, unrecorded Trigger children,
latest Prefect task attempts, and honoring the worker's Prefect endpoint.

The disposable flow definition was removed after completion. Standalone agent sessions belonging
to prior work were not cancelled. The native flow host now runs in an Orca terminal.

After reloading the final runtime service, quiet five-step research-flow restarts passed again:
Prefect `250c2ebd-be36-4954-af09-57e6224ba0bb` → `1c98cb75-3045-4ae4-9c30-85c17b3ce8e4`;
Trigger `run_cmw0b621500c84go2g1v6j9a9` → `run_cmw0b70tb00d24go2keszdrli`.

## Deployment

The verified image is running on the main board at port 8090. Root redirects to login (302),
unauthenticated API access returns 401 locally and through the existing HTTPS Funnel, and
authenticated systems/UI requests return 200. Login configuration and password were preserved.
The 8092 test copy uses the same image and is bound only to loopback.
