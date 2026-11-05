# Execution workers and environment setup

Start `agents/flow_host.py` as before. It also supervises a loopback execution service on 8096
(the implicit `local` worker); this stays up when flow definitions re-register. Other PCs run
the same execution service and are registered in the coordinator's fleet registry, which the
board's **Fleet** tab edits. Older engine runs without worker selections keep their local
executor and workspace behavior.

Each native task is still scheduled, retried and cancelled by its engine. Its Python coordinator
dispatches the command or agent to an execution service chosen from the fleet. The node panel
reports the **actual execution worker**, hostname, pool and tags after the job is admitted. This
is distinct from the native engine's coordinator process. Prefect serve and Trigger development
execution are supported by the shared runtime extension; choosing a native queue is not a
PC-affinity guarantee.

## Register a PC

One registration is one PC, however many engines and pools it serves. In the board, open
**Fleet**, then **Register PC**:

1. Choose the engines and the operating system. The form shows setup steps generated for that
   choice (the same steps as below).
2. Enter the worker ID, the endpoint (`http(s)://<pc host>:8096`) and the name of the environment
   variable that holds its token (default `FLOW_WORKER_TOKEN_<ID>`). The board never asks for the
   token itself. Leave the endpoint blank to save the PC as **awaiting connection** and add the
   endpoint later.
3. Choose one or more pools (or add one) and tags. Tags autocomplete from the tag catalog, and a
   new tag can be created inline. Tags are declared attributes, separate from detected software.
4. Set the slots and the CPU and RAM limits (defaults 1, 90% and 90%).

On the PC, install the agents environment and the coding agent CLIs it should run (see the
[agent instructions](../agents/README.md); for an offline PC the fleet tab lists the wheelhouse
and npm mirror steps), then start the service:

```powershell
$env:FLOW_WORKER_ID = 'build-pc'          # must equal the registered worker ID
$env:FLOW_WORKER_BIND = '0.0.0.0'
$env:FLOW_WORKER_PORT = '8096'
$env:FLOW_WORKER_SLOTS = '2'              # per-process cap: keep it at least the fleet slot count
$env:FLOW_WORKER_TOKEN = (Get-Content '<protected token file>' -TotalCount 1).Trim()
agents/.venv/Scripts/python.exe -u agents/flow_execution_worker.py
```

On Linux use `agents/.venv/bin/python` and `export`. Generate the token on the coordinator
(`python -c "import secrets; print(secrets.token_urlsafe(32))"`), store it as `NAME=value` in the
coordinator's ignored `workers.local.env` (or the flow host's environment), and copy it to a file
on the PC that only the service account can read. The service refuses a network bind without a
token. It can execute commands with that PC's account: allow inbound TCP 8096 only from the
coordinator, over a private network or HTTPS termination. No native engine credentials are needed
on execution PCs. Agent credentials, Confluence tokens and source file paths belong on the PC that
runs the step. `FLOW_WORKER_POOL` and `FLOW_WORKER_TAGS` on the PC are legacy and informational
only: the registry owns pools, tags and limits.

When the PC connects, the coordinator checks that `/health` reports the registered ID (otherwise
the PC is shown as an identity mismatch and nothing is placed on it), then pushes the registry's
slots and CPU/RAM limits to its capacity gate. The fleet tab shows each PC's state (online,
awaiting connection, credentials unavailable, unauthorized, identity mismatch or unreachable),
live CPU and RAM, slot use, per-engine status, pools, tags, limits and their sync state, and the
detected agents and commands with their last check time. The board receives only this sanitized
view, never endpoints, token variable names or tokens.

To let a registered PC serve another engine, edit it and add the engine. Registering the same ID
again is refused with a pointer to the existing entry, so a PC is never registered twice.

### Per-engine registration

Native engine placement is not enabled (see [Engine adapters](#engine-adapters-and-workspace-integrity)),
so every engine reaches a PC through its execution service and the PC needs no engine connection:

| Engine | On the coordinator | On the PC | Fleet tab engine status |
|---|---|---|---|
| Hatchet | The flow host runs the Hatchet flow worker (`FLOW_BACKENDS` includes `hatchet`). Placed standalone cards go through `agents/hatchet_worker.py`. | Execution service only; no Hatchet token. | Connected while the PC is online and the coordinator's Hatchet flow worker runs. |
| Prefect | The flow host runs `flow_worker_prefect.py` (served deployments stay in the coordinator's serve process). Placed cards go through `agents/prefect_worker.py`. | Execution service only; no Prefect API access. | As for Hatchet. |
| Trigger.dev | The local development worker (`trigger dev`, `scripts/dev-worker.ps1`) runs on the coordinator and sends flow steps to the PC. | Execution service only. | Unconfirmed: the flow host cannot check `trigger dev`. |

Standalone Trigger.dev cards always run on the coordinator, and Trigger.dev production deployment
to other PCs is not supported. A worker's `engines` list (default: all three) limits which engines
may place work on it; a step or card on another engine treats that PC as not compatible.

## Registry, migration and rollback

The registry is `workers.local.yaml` on the coordinator (`FLOW_WORKERS_FILE` overrides it); token
values stay in the ignored `workers.local.env` or the flow host's environment and are never
written to the registry. [workers.example.yaml](../workers.example.yaml) shows the format. Without
a registry file the coordinator uses one implicit worker, `local` (`FLOW_WORKER_ID`), in pool
`default`, at `http://127.0.0.1:8096` with token variable `FLOW_WORKER_TOKEN`.

A legacy (version 1) registry keeps working as it is: its single `pool` becomes the worker's only
pool membership, and IDs, tags, endpoints and `token_env` references are kept. Flow and step
selectors keep their meaning; `pool` matches any of a worker's pools. The file is rewritten as
version 2 (with `pools`, `engines`, `slots`, `cpu_limit`, `ram_limit` and the tag catalog) on the
first save from the fleet tab or the legacy `/workers` routes, or explicitly:

1. Stop everything that edits the registry (the flow host, or at least registry saves).
2. Copy the registry file, with its permissions, to a protected folder outside the repository.
   `workers.local.env` is never rewritten by migration; keep it as it is.
3. Start the flow host and call `POST /workers/migrate` on it (with `Authorization: Bearer
   <FLOW_HOST_TOKEN>` when that token is set). It answers `{migrated: true}` and writes version 2
   through a temporary file and an atomic replace, so an interruption leaves the original file.
   Running it again answers `{migrated: false}` and changes nothing.
4. In the fleet tab, check the IDs, pools, tags and that each PC with a token still shows its
   connection as configured and comes online.

To roll back, stop the registry editors again, copy the backup into the registry's folder under a
temporary name with the same protections, and atomically replace the registry with it. Restore
the backup before running an older version of this repository: older versions read only the
legacy `pool` field. Edits made after the backup are lost, so note them first. The last known
capability results of offline PCs are kept in `<registry>.inventory.json`; it holds no secrets and
can be deleted at any time.

## Choosing workers in the board

**Starting a flow (New card).** Choose a default worker or pool and required tags, and expand
**Choose workers for individual steps** to override placement per step. A step's own worker ID
chooses that worker regardless of the default pool; a step pool replaces the default pool; step
tags replace the default's tags and keep its pool. A default chosen at launch replaces the flow's
default worker, and a step chosen at launch replaces that step's own setting; otherwise the flow
definition's default and step settings apply as before. While you choose, the dialog asks the flow host
(`POST /api/launch/validate`, with the selected backend) where each step can run and shows, per step:

- **Ready**: the workers that can start it now.
- **Will wait**: a possible match is offline, busy or not freshly checked; the run starts and the
  step waits for it (see [waiting reasons](#waiting-reasons-and-states)).
- **Blocked**: no registered worker can run it, with the reason and **How to fix**. **Start flow**
  is disabled, and the board refuses the launch as well. The flow itself stays saved.

Under each step every registered worker is listed as Compatible, Might match (waiting) or Not
compatible, with the reasons, and the required agent or command with its last detected status and
check time (for an offline PC, its last known result, marked as such).

**Standalone coding agent cards.** **Run on (fleet worker)** places the card on a pool, a worker
or tags, on Hatchet and Prefect. It is disabled, with the reason, on Trigger.dev (cards run on the
coordinator there) and when a workspace or resume ID is filled in: a placed card runs in a new
workspace on that PC. The same compatibility panel is shown, and a blocked card cannot be created.
Without a selection the card runs on the coordinator as before and is not checked.

**Editing a flow (Flows).** Each step's form shows **Where this step can run** for the unsaved
draft, placed the way **New card** starts it, and where its selection comes from (its own worker
setting or the default in Flow settings). The Steps list shows Ready, Waits or Blocked per step,
and Flow settings summarizes all steps. Saving is never blocked: a blocked step says "You can save
this draft; launch is blocked until this is resolved". Program steps have an **Executable
(requires_command)** field. Leave it blank when the executable is the first word of a simple
command; when it cannot be inferred (pipes, variables, scripts) the field is marked required and
launch stays blocked until it is filled in.

**While it runs.** The engine shows a placed card or step as running while it waits for a worker:
the wait happens inside the engine task, which occupies one engine execution slot meanwhile and
counts toward the step's `timeout_minutes`. This is an unavoidable constraint of the shared-service
fallback (see [Placement, waiting and cancellation](#placement-waiting-and-cancellation)). The card
drawer's **Fleet placement** section polls the execution records every 2 seconds while the drawer
is open and shows each step's state and reason; the graph labels a waiting step "waiting for a
worker" (or "intervention needed"), and its step panel explains it.

**Restarting a step.** A finished step's node panel can choose a new execution worker; the choice
is checked like a launch (a confirmed mismatch is refused, an offline or busy match is accepted and
waits). Descendants retain their own selections and restart through the engine dependency
mechanism. Hatchet replays its native DAG; the Prefect/Trigger extension creates a new native run
and reuses unaffected results. Clearing a restart override returns to the flow's selection.

### Waiting reasons and states

| `waiting_reason` | Shown as |
|---|---|
| `offline` | a matching worker is offline (or awaiting connection) |
| `credentials_unavailable` | the coordinator has no usable credentials for a matching worker |
| `full_slots` | every matching worker is busy (no free slot) |
| `high_cpu` / `high_ram` | CPU or RAM on the matching workers is at or above their limit |
| `missing_metrics` | waiting for a complete 10-second CPU and RAM measurement |
| `cooldown` | the worker waits for a fresh 10-second measurement after its last start |
| `software_unknown` / `software_stale` | the required software has not been checked, or was last checked over a day ago; it is probed |
| `software_missing` | the software was missing on the chosen worker at admission; another one is sought |
| `awaiting_authorization` | the worker admitted the job and waits for the coordinator to confirm the start |
| `coordinator_unreachable` | the worker cannot reach the coordinator |
| `intervention_needed` | no registered worker can run it now; the record carries the message and resolution |

Record states: `queued` (being placed, or queued on a worker), `waiting`, `running`, `done`,
`failed`, `cancelled` (the board's cancel), `timed_out` (the step's `timeout_minutes` elapsed) and
`interrupted` (any other engine-side cancellation). The last three keep the last waiting reason,
so the board can say why the work never ran.

## How files move

The coordinator PC holds each run's published files in `FLOW_WORKSPACES` (by default
`%TEMP%/agent-flows` on Windows). Before starting a step it merges the completed ancestors' file
changes, then sends the complete input tree to the chosen service as JSON: relative paths,
base64-encoded file bytes and permission bits. The service restores it into a new job folder.
After successful execution the service sends its complete output tree back. The coordinator
records that step's changed files and deletions, and downstream steps receive a newly merged tree.

This is coordinator-mediated copying over HTTP/HTTPS, according to the registry URL. Non-loopback
worker services require a bearer token; the default loopback service may run without one. Workers
do not copy directly to one another or mount a common folder. Files become available
to dependents only when the producing step succeeds; edits are not streamed while it runs. The
network currently carries full snapshots rather than binary diffs or compressed archives. The
coordinator's saved publications are deltas. Parallel branches changing one path differently
produce an explicit conflict; identical changes are allowed, and later dependent edits win.

## Flow YAML

```yaml
name: context-example
worker: {pool: builds}
environment:
  agent: codex
  worker: {id: setup-pc}
  repositories:
    - url: https://example.com/team/project.git
      ref: main
      target: repos/project
  files:
    - source: C:/Context/requirements.pdf
      target: context/requirements.pdf
  confluence:
    - base_url: https://company.atlassian.net/wiki
      page_id: '123456'
      target: context/wiki
      token_env: CONFLUENCE_TOKEN
      username_env: CONFLUENCE_EMAIL  # Cloud basic auth; omit for bearer/PAT
      include_descendants: true     # default; false exports just the root page
      attachments: referenced       # default; all or disabled also supported
steps:
  build:
    kind: program
    worker: {tags: [gpu]}
    run: python -c "from pathlib import Path; print(list(Path('repos/project').iterdir()))"
  review:
    kind: agent
    needs: [build]
    worker: {id: review-pc}
    agent: codex
    review: true
    prompt: Review the project using context/wiki and context/requirements.pdf.
```

Configure context in the existing **Flows** YAML editor. Files are source paths on the setup PC,
including directories; browser uploads are not implemented. Git uses that PC's credential helper
or SSH, with interactive terminal prompting disabled so failures appear in setup chat. HTTP URLs
must not contain credentials. Confluence imports use
[Spenhouet's Confluence Markdown Exporter](https://github.com/Spenhouet/confluence-markdown-exporter),
pinned to **5.6.0** in `agents/requirements.txt`. Install that requirements file on every setup
worker. Public sites work anonymously when credential fields are omitted. Each source runs in
an isolated exporter process; credentials/configuration remain outside the transferable workspace.

The default imports the root and all descendants, preserving Markdown formatting and downloading
referenced attachments. Each page is named `<page-id>.md`; `index.json` maps IDs to titles,
versions, paths and attachment versions. Root-relative links with exported page IDs are rewritten
to local Markdown files; other root-relative links become absolute Confluence URLs. `attachments/`
contains referenced files. The exporter lockfile and `confluence-export.json`
record completion and file hashes. Missing/modified files, failed pages or attachments, and a
changed remote page tree or page/attachment version block setup completion and require re-import.
A failed refresh preserves the previous complete import. `include_descendants: false` limits the
import to one page; `attachments: all` downloads every attachment, while `disabled` skips downloads.
External linked pages are not crawled.

To reproduce the public-site check without credentials:

```powershell
agents/.venv/Scripts/python.exe -u agents/confluence_smoke_test.py
```

It imports small public Apache Tika, Atlassian and Shibboleth roots, including a six-page Tika
subtree, into ignored `agents/runtime/confluence-public`, then validates page completeness and
content hashes. Results are recorded in `verification.json`. See the
[verification record](superpowers/2026-11-03-confluence-markdown-verification.md).

The same live imports, plus a public-page transfer between two local workers, are part of the
automated suite. Run `agents/.venv/Scripts/python.exe -m unittest discover -s agents/integration_tests -v`.
The [Tests workflow](../.github/workflows/tests.yml) runs them on pushes, pull requests and daily on
the default branch. Separate jobs start disposable Hatchet and Prefect stacks and test a real
public-page import, worker transfer and native step restart. Fixture tests run separately so
public-site failures are clear; Trigger.dev native-engine smoke remains a local-stack test.

The runtime expands `environment` into a real native `setup_environment` agent node before all
root steps. It attempts each source, shows preparation errors, and opens the selected agent for
interactive repairs. It always waits for **Mark done**. Validation checks git origin/revision,
copied content and the complete Confluence Markdown export before releasing dependencies. Missing or
partial sources keep setup open. The demo agent is explicitly simulated and cannot repair files.

Each step gets an isolated workspace containing its ancestors' file changes, even on another PC.
Flow scripts referenced with `{flow_dir}` travel with the job, captured once per run and retained
for selective restart. Later sequential edits supersede
earlier edits; differing edits/deletions on parallel branches cause a merge error before downstream
execution. Identical parallel edits are allowed. Unrelated branches do not see each other's files.
Transfers preserve executable bits, reject symlinks, case collisions and unsafe paths, and are
limited to **64 MiB and 10,000 files** per tree (including repository `.git`). Large contexts need
smaller clones or a future external artifact store. Steps must put durable outputs in their
workspace; worker-local absolute paths are not portable artifacts.

Cancelled or superseded jobs cannot publish new results. Remote jobs expire after 45 seconds
without coordinator polling, including when an engine kills the coordinator process abruptly.
Finished service workspaces expire after five minutes without polling. Canonical publications
remain in `FLOW_WORKSPACES` on the coordinator PC for selective restart.

## Fleet placement feasibility decision (2026-11-04)

For the current deployment modes, retain the shared execution service for Hatchet portable
flows, Prefect served deployments, and Trigger.dev's local development worker. This is a
conservative adapter decision, not proof that native placement cannot work. The existing
service supports the documented flow and step selectors, but does not yet implement the new
fleet contract: in particular, offline selections currently fail, and cross-engine admission,
metrics, cooldown, leases and fencing still need implementation.

The installed Hatchet SDK is 1.44.0; the isolated Compose stack used server image v0.114.2. The SDK
surface exposes worker labels, task desired labels, and run desired worker ID/labels. Labels
default to `required=False`, so merely supplying labels is insufficient for strict matching.
The task scheduling timeout defaults to five minutes and is configurable. In a disposable stack, a run-level required station selector caused a task to execute on a
worker lacking the task's required kind label: task and runtime selectors cannot be assumed to
combine. A two-step probe placed distinct task selectors on the correct workers, but a run-level
station selector sent both steps to that station. A separate run-level desired worker ID probe
executed on a different worker. The SDK's public worker update request exposes pause only, and
the runtime labels property has no setter; no in-place label-update facility was found in this
pinned SDK. Without a run-level override, a queued task waited through disconnection and a worker label
change, then executed on the re-registered matching worker. The queued cancellation probe did
not execute after an eligible worker joined. A task configured with an eight-minute scheduling
timeout did not execute within 90 seconds after an eligible worker joined following a 310-second
wait; the engine logged a slot-replenishment timeout, so the cause is undetermined. These
limited observations do not establish all cancellation and restart behavior. Native placement
must not be enabled without resolving selector composition and verifying the complete lifecycle,
including a timeout strategy that supports the intended indefinite wait.

Prefect's current `serve()` process registers deployments and executes the flow and its tasks
on the serving host; the repository dispatches each step through the shared service. Trigger
currently generates flow and task code for the local `trigger dev` worker and uses the same
service for step execution. Neither configured mode establishes direct per-step remote PC
placement or the required cross-engine admission guarantees. Trigger production deployment is
outside the current supported mode and requires a separate design and verification.

For any future native route, engine assignment must still pass a single per-PC admission gate
shared with service routes, with crash-safe leases and fencing. A queued engine task must remain
cancellable and must not silently fail at an engine scheduling timeout; waiting inside an engine
slot can consume capacity, so queue behavior must be measured. Remote native workers also need
immutable, versioned flow definitions, safe registration and update failure handling while old
runs remain in flight. The coordinator owns workspace publications: remote execution needs
authenticated artifact access or equivalent transfer preserving locks, limits, conflict handling
and fencing; do not assume a shared filesystem. Existing coordinator-mediated HTTP transfer and
its authentication remain the fallback path described above.

## Placement, waiting and cancellation

A step placed by the fleet needs a worker in its selected pool (if any) with every required tag
(or the selected worker ID), plus its software: the agent of an agent step, or the program's
`requires_command`, otherwise the first executable inferred from `run`. A leading shell builtin
such as `echo` needs no executable. A program whose executable cannot be inferred safely (pipes,
variable executables) must declare `requires_command` before launch.

Launch is blocked only when no registered worker can satisfy a step: a selection mismatch or a
fresh `absent` capability result on every candidate. The error names the mismatch and how to
resolve it; drafts can still be saved. If a potentially matching worker is offline, awaiting
connection, has no usable credentials, has unknown or stale (older than one day) capability
results, no free slot, high CPU or RAM, or missing metrics, the run launches and the step waits.
Its execution record (`/execution/<run>`) shows `state: waiting`, a `waiting_reason` and the
per-worker reasons; it never contains endpoints or credentials. Missing or unchecked software is
probed on demand, at most every 15 seconds per worker while waiting. If the software disappears
after launch, the worker rejects the job before it starts and the step waits for another suitable
worker; when no registered worker can satisfy it, the reason becomes `intervention_needed` with a
resolution, and the step resumes automatically once the fleet or software is repaired.

The coordinator submits a job with start authorization: after the worker's capacity gate admits
the job and immediately before launch, the job waits up to 10 seconds for the coordinator to
confirm the attempt is still current, then rechecks its software. Without that confirmation it
releases its reservation, and it only seeks admission again while its coordinator keeps polling.
A step cancelled while its worker is unreachable therefore never starts later, even if the worker
returns with free capacity; the unpolled job is cancelled after 45 seconds. A job lost before
authorization (worker restart or disconnection) is placed again; after authorization a lost
worker fails the step as before, and the engine's retries apply. A restarted coordinator starts a
new attempt with a new fence, so an older attempt can neither start nor publish.

Fallback constraint: the current Hatchet, Prefect and Trigger.dev adapters run each portable-flow
step as an engine task that calls the shared execution service, and none of them offers a queued
alternative that preserves the native DAG, step restart and the cross-engine admission gate.
Waiting therefore happens inside the step's engine task: it occupies one engine execution slot of
the flow worker and counts toward the step's `timeout_minutes`, so it is bounded rather than
indefinite. Raise `timeout_minutes` for steps expected to wait long. Engine scheduling timeouts do
not apply because the task is already running. Native engine queueing remains a separate, unverified option.

The default wait bound is therefore the step's `timeout_minutes`, 60 minutes unless the flow sets
it (at most 1440). When it expires, the engine reports the step as timed out and the step's
execution record shows `state: timed_out`, keeping the last `waiting_reason` and `message` (for
example `offline`) so the board can show why it never ran. A cancellation through the board's
cancel action records `state: cancelled`; any other engine-side cancellation of the task (from the
engine's own UI or a worker shutdown) records `state: interrupted`. If the flow host is unreachable
when the board cancels, the cancellation still happens but may be recorded as `interrupted`. A
restart onto a selected worker is checked like a launch, including the step's software: a confirmed
absence is refused with the explanation, while an offline, busy or unchecked worker is accepted and
the restarted step waits for it. A worker that reports a job as started (running state, a worker
identity or step events) is never replaced by another placement, even if it ignored start
authorization; losing it then fails the step and the engine's retries apply.

## Engine adapters and workspace integrity

Native engine placement is not enabled for any engine. Hatchet's run-level selectors did not
compose with task-level ones in the feasibility probes, routing by worker ID and waits longer than
five minutes were not proven, and Prefect `serve()` and the Trigger.dev local development worker
offer no per-step remote PC placement (see the feasibility decision above). Every supported mode
therefore uses the shared execution service: Hatchet portable flows, Prefect served deployments,
the Trigger.dev local development worker, and standalone agent cards on Hatchet and Prefect.

Each adapter computes the same requirement object (selection plus software), waits with the same
lifecycle, and submits the job to the chosen worker's service, whose capacity gate admits it. So
one PC's slots, CPU/RAM limits and start cooldown apply to work from every engine at once. The
execution record shows `engine` (`hatchet`, `prefect` or `trigger`), the executing `worker`
(the identity reported by the service that started the job: id, hostname, pool, tags), `state`,
and `waiting_reason`. For a flow step, the record is under `/execution/<run>` (`steps.<step>`).
For a card, it is under `steps.card`, which the board serves at
`GET /api/cards/<system>/<run>/execution`. The record never contains endpoints or credentials.

A standalone card is placed only when it has a `worker` selection (`id`, `pool`, `tags`); its
requirement adds the card's agent. Without a selection it runs on the engine worker exactly as
before. A placed card runs in a new workspace on the executing PC, as a blank-workspace card does.
A host workspace path or a resumed session (bound to its original workspace) is refused for a
placed card, both when it is created and by the worker service, rather than assumed to exist on
another PC. A card transcript keeps its event schema: the executing PC is in the record, not the
transcript. Placed cards on Trigger.dev are not supported; that board backend refuses a selection.

Queue timeouts: work waits for a fleet worker inside its engine task, which counts toward the
step's `timeout_minutes`. Steps queued behind waiting ones at the engine must not expire first, so
Hatchet flow tasks and the card task set a 24-hour `schedule_timeout` (the SDK default is five
minutes). Trigger.dev flow and step tasks set a one-day `ttl` (development runs otherwise leave
the queue after ten minutes). Neither setting has been verified against a live server here.
Trigger.dev's configuration disables retries in development (`enabledInDev: false`), so a step's
`retries` applies only on Hatchet and Prefect.

Revisions: each job carries the exact flow definition and the run's asset snapshot, so a worker
never reads flows or assets from its own disk, and a worker update cannot change a running job. The
first step of a run pins the definition hash, for placed and local flows alike. If the flow is
updated while an old run is in flight, an engine worker serving the new revision may still receive
the old run's remaining or parallel steps. They fail with "The flow definition has changed; start
a new flow run" before anything is placed or published, so revisions never mix. If activating
the new revision failed for an engine, that engine keeps serving the old revision and old runs
finish on it.

Workspaces and artifacts: the coordinator owns the run workspace. Each job receives its files
over the authenticated service API, within the existing transfer limits, and runs in its own
job directory on the executing PC, never at the coordinator's path. Results are published only
while the attempt's fence is current. An engine retry or replay starts a new attempt, after which
the superseded attempt can neither publish nor update the record. Parallel branches that change
the same file on different PCs make the joining step fail with a workspace conflict before it is
placed. A restart reuses the source run's exact results and places only the restarted subtree.

Limits: the registry's `slots`, `cpu_limit` and `ram_limit` are pushed to the worker's gate
(authenticated `POST /capacity/settings`, limits only) when the registry entry is saved and
whenever the coordinator finds a connected worker whose gate reports different limits, which
covers connecting and reconnecting. The gate reads them at each admission, so a change affects
new work only and never interrupts running jobs. Until the push succeeds (the PC is offline, or
its service predates the route) the gate keeps its previous limits and the fleet tab says so.
A failed push is retried after 30 seconds (5 minutes when the service predates the route), or at
once when the registry value changes. Slots are at most 100, the gate's maximum: a save above it
is refused, and an older registry entry above it is enforced as 100 with a warning in the fleet
tab until the next save stores 100. `FLOW_WORKER_SLOTS` still caps each service process. A
worker's `engines` list (default: all three) limits which engines may place work on it.

## Capability detection

Each execution service detects the supported agents (including their ACP adapters) and the
commands steps need, in its own execution environment: at startup, whenever its results are more
than a day old (checked hourly, and when the coordinator reads them, which covers a reconnect),
on **Refresh capabilities** in the fleet tab, on demand while a step waits for a command or agent
that is not freshly installed (at most every 15 seconds per worker), and once more immediately
before a job starts. Results are `installed`, `absent`, `unknown` or `error`, each with its check
time. Installed does not mean signed in: authentication readiness is reported as unknown. A result
older than a day counts as unknown, never as absent; only a fresh `absent` makes a worker not
compatible. The coordinator keeps the last known results of each PC so an offline PC still shows
when it was last checked; they are shown, never used as a verified answer.

## Capacity: CPU, RAM, slots and cooldown

Each execution service samples the whole machine every second into the PC's shared capacity store:
CPU from the OS's idle and busy counters between samples (`GetSystemTimes` on Windows,
`/proc/stat` on Linux), and RAM as physical memory in use, `(total - available) / total`, using
the OS's available-memory figure (`GlobalMemoryStatusEx` on Windows, `MemAvailable` on Linux), so
readily reclaimable cache counts as free. macOS is not sampled: a macOS PC always reports unknown
capacity and its gate never admits work.

The admitted value is a time-weighted average over the last 10 seconds of a monotonic clock. It is
unknown (waiting reason `missing_metrics`) until samples cover a full 10-second window, when the
newest sample is more than 3 seconds old (stale), and after a gap of more than 3 seconds between
samples, which restarts the window. A PC is not eligible for new work while either average is at
or above its limit (`high_cpu`, `high_ram`; default 90%). These are admission thresholds, not caps
or predictions: running jobs are never interrupted when a limit is exceeded or changed.

Admission is serialized per PC in one SQLite store shared by every service on it
(`FLOW_CAPACITY_DB`, default `%PROGRAMDATA%\agent-orchestration\capacity.db` on Windows and
`/var/tmp/agent-orchestration/capacity.db` on Linux; never a network drive). In order, a new job
waits while the PC's leases fill its slots (`full_slots`, counted across all engines), while
another admission is still starting, during the cooldown (`cooldown`: for 10 seconds after the last
confirmed start, and until a sample taken at least 10 seconds after it exists), while metrics are
unknown, and while CPU or RAM is at or above its limit. The cooldown and samples survive service
restarts and reconnects. A lease lasts 30 seconds and is renewed every 5 seconds while the job runs.

## Crash recovery and its limits

An expired lease is not reused while its work may still run: the reservation stays blocked until
the execution is known to have stopped. A reservation whose owner died before the job started is
reclaimed. A started job's reservation is reclaimed after its process tree is verified dead
(a Windows named Job in the same OS session is terminated and checked; on Linux the session or
process group must be gone) or after a verified reboot (a changed OS boot identity).

**Limitation (POSIX):** live orphan process groups on Linux and other POSIX systems are not
terminated automatically. If an execution service or its coordinator crashes while a job's process
group keeps running, that PC's reservation stays blocked (the PC shows a used slot and new work
waits with `full_slots`) until the group is verified dead or the PC reboots. Unattended POSIX crash
recovery is not supported: an operator must stop the orphaned processes, or reboot the PC. On
Windows, a Job owned by another OS session or account cannot be opened, and its reservation stays
blocked in the same way.
