# Workers and environment context implementation plan

> **For agentic workers:** Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Route individual native DAG steps to real workers, carry their files across PCs and prepare interactive environment context.
**Architecture:** Native engine coordinators keep dependency and retry ownership. A Python execution service runs isolated steps; shared flowkit code resolves workers and publishes ancestor file deltas. Environment expansion inserts a native setup agent task.
**Tech Stack:** Existing Python/aiohttp/YAML, native Hatchet/Prefect/Trigger APIs, vanilla JavaScript board.
**Spec:** docs/superpowers/specs/2026-11-03-workers-context-design.md

## Global constraints

- Different steps in one DAG may execute on different PCs.
- Board contains no dependency scheduling or restart descendant business logic.
- Native coordinators share the flow-host PC artifact store; remote execution services need no engine credentials.
- Bound workspace transfers to 64 MiB and 10,000 files; fail explicitly on limits/conflicts.
- Credentials live in worker environment variables; network execution services require authentication.

## Review focus

- Concurrent branches editing/deleting one path must fail before downstream execution unless maximal changes match.
- A cancelled or superseded job must not publish files or an exact result.
- A partial clone/export must not release dependent steps after Mark done.
- Remote commands need the correct flow assets and executable metadata.
- Native replay must see current ancestor artifacts and preserve unrelated results and worker selections.

### Task 1: Worker execution and portable artifacts

Files: create `agents/flowkit/artifacts.py`, `workers.py`, `distributed.py`, `worker_service.py`, `worker_stdio.py`; modify definition/executor/stdio and native workers; test `agents/tests/test_distributed.py`.

Interfaces: `snapshot(Path) -> dict`, `restore(dict, Path)`, `ancestor_files(Flow, step, Path) -> dict`; `execute_step(flow, step, context, run_id, receive, sink)` matches the local executor interface; service `/health`, `/jobs`, `/jobs/{id}`, `/jobs/{id}/input`, DELETE `/jobs/{id}`.

- [x] Write tests for dependency merge, conflicts, deletions, unsafe paths and two independently configured services transferring a real file and reporting distinct identities.
- [x] Run focused unittest module and observe missing-module/behavior failures.
- [x] Implement immutable deltas, execution fencing, authenticated service, queue/cancel and input relay. Add worker selectors to flow YAML and `flowExecution` native parameters; preserve legacy local execution for runs without routing.
- [x] Run focused tests plus existing agents tests; regenerate Trigger code and type-check.
- [x] Commit working runtime and evidence.

### Task 2: Board worker controls and actual identity

Files: `agents/flow_host.py`, `board/flows_client.py`, `server.py`, `cards.py`, `static/index.html`, board tests and worker registry example/docs.

Interfaces: host `/workers`, `/execution/{run}` and POST `/execution/{run}/selection`; sanitized worker catalog plus actual step identity. Board forwards launch `flowExecution` and optional selected-step restart override.

- [x] Add failing route tests for launch override validation and worker information.
- [x] Implement selectors for default and each step, including worker id/pool/tags and actual worker node display. Selection updates belong in runtime; restart still uses existing engine path.
- [x] Verify board Python tests, Chromium tests and actual two-worker native flows on all three engines.
- [x] Commit working controls and verification.

### Task 3: Interactive setup environment

Files: create `agents/flowkit/environment.py`, tests and example flow; modify definition/executor, docs and UI environment summary.

Interfaces: validate environment mapping, expand into `setup_environment` ordinary agent step, `prepare_sources(config, workspace)` and `validate_sources(config, workspace)`; setup routes through existing native chat inputs.

- [x] Write failing tests using a disposable git repository, copied file and paginated local Confluence server; test incomplete export/clone and repair validation.
- [x] Implement deterministic source preparation, safe destinations, credential-env lookup and interactive reviewed agent repair loop; validate every target before returning success.
- [x] Verify real setup chat and downstream file availability on a different service, existing restart and native smoke suites.
- [x] Update documentation and ledger, request fresh whole-change review, fix material findings and rerun relevant checks.
- [x] Commit/push and deploy the authorized feature branch.

## Execution ledger

Ruling: Continue inline under the user's existing 'go' and 'continue the rest' authorization. Use the confirmed per-step worker requirement and builder decisions in the spec; no further design approval round is needed.
Builder check completed: restart reconstruction, fencing, assets, completion validation and filesystem metadata decisions incorporated in the spec.

Ruling: Deliver the shared runtime, board and setup changes in one integrated feature commit; native parameter and workspace changes span these tasks. Preserve unrelated concurrent documentation edits outside the commit.
Verification: See ../2026-11-03-workers-context-verification.md. Fresh review is clear; Windows and Linux regression checks and all three native smoke flows passed.

Delivery: Feature commit `3ce47d6` pushed to `origin/feat/agent-chat-dashboard`. Main board rebuilt and flow host restarted with the local execution service online; authenticated local/Funnel worker catalog and UI both returned 200, while unauthenticated root/API returned 302/401.
