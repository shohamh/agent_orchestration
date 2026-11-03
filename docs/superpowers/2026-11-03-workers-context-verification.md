# Worker and context verification (2026-11-03)

Implemented the remaining worker-selection and environment-setup requests, including the user's
explicit choice to run different steps of one DAG on different PCs. Scheduling and dependency
restart remain outside the board.

## Native integration

`agents/.venv/Scripts/python.exe -u board/workers_smoke_test.py --base http://127.0.0.1:8093`
finished normally with exit code 0 against the dedicated `agent-board-workers-test` copy.

| Engine | Source run | Selective restart |
|---|---|---|
| Hatchet | `4dbfb4d8-d960-4c72-8c14-2a1bd14bc224` | Same run, native replay |
| Prefect | `1a79a893-ac82-434a-b832-3b978ce59158` | `46cde6f7-1560-43a4-bda5-6d941ddd668d` |
| Trigger.dev | `run_cmw0qlj0f00ft4go2qseq0ijd` | `run_cmw0qm1jx00gd4go29xrn6juy` |

Every run cloned a disposable git repository, copied a source file and exported a Confluence API
fixture tree on `smoke-a`. The native setup agent waited for human review while dependent tasks
remained pending. A command on independent service `smoke-b` checked all three sources and wrote
an output file; a subsequent command on `smoke-a` read that file. Actual worker identities in the
board DAG matched those services. Restart moved the selected command from `smoke-b` to `smoke-a`
and reused setup without another review. The test removed its flow and temporary worker registry
and left no active disposable runs.

These are two independently configured local services, not two physical PCs. Physical remote
networking and a real Confluence account were not tested. Confluence pagination was separately
verified against a local multi-page fixture; production API credentials stay on the setup worker.

## Regression checks

- Agents suite: 46 tests, 45 passed and one POSIX-specific test skipped on Windows.
- Board suite: 36 tests passed.
- Native Windows startup/input tests: four passed.
- JavaScript and real Chromium suite: 11 tests passed, including worker pool/id/tag launch controls.
- Trigger TypeScript: `npx.cmd --no-install tsc --noEmit -p .` in the example project passed.
- POSIX cancellation test passed separately in a disposable Linux Docker container. A spawned
  command's grandchild could not write its delayed marker after cancellation. Test container removed.
- `git diff --check` passed for the feature changes.

Focused tests cover conflicting/identical parallel file changes, deletions, unsafe paths, changed
definitions, immutable run assets, copied-file transfer, authenticated services, queued cancellation,
missing tracked git files and incomplete Confluence exports. A failed setup was marked done,
remained interactive after validation failed, was repaired, and completed only after a second review.
A 6000-line command retained actual worker identity despite the bounded event history.

## Review and limits

Fresh review identified mutable per-step script snapshots, evicted worker identity and incomplete
git worktree validation. Red/green regression tests and fixes address all three. The reviewer
spot-checked those changes and reported no remaining critical or important findings.

Environment sources are configured in the Flows YAML editor. File paths refer to the setup PC;
browser uploads are not implemented. Workspace/asset transfers each allow 64 MiB and 10,000 files,
with symlinks and incompatible paths rejected. Native coordinators currently share the flow-host
PC's artifact store; execution services may live on other PCs. The prior offline bundle was not
rebuilt as part of this feature.

Unrelated pre-existing edits to `board/README.md`, `prefect/README.md`, and removal of
`board/LIVE_UPDATES_PLAN.md` were preserved and excluded from this feature commit.

## Deployment

Feature commit `3ce47d6` was pushed to `origin/feat/agent-chat-dashboard`. The main board on 8090
and the isolated worker test copy on 8093 run the rebuilt image
`sha256:975209278f3db6ce351ba5c479887e14f9e4a6d54c091bd08044e502f5df0b16`.
The flow host was restarted in Orca terminal `term_84644a90-9193-4ccb-8d42-917e54873f26`, with
the local execution service online, four slots and zero active jobs after verification.

Local and Funnel checks both passed: unauthenticated `/` = 302, `/api/systems` = 401;
authenticated worker catalog and the updated UI = 200. The flow host listed the original three
definitions and no disposable smoke flow. Password and backend credentials were preserved.
