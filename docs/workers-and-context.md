# Execution workers and environment setup

Start `agents/flow_host.py` as before. It now also supervises a loopback execution service on
8096; this stays up when flow definitions re-register. New flows launched from the board use it.
Older engine runs without worker selections retain their local executor and workspace behavior.

Each native task is still scheduled, retried and cancelled by its engine. Its Python coordinator
dispatches the command or agent to the selected execution service. The node panel reports the
**actual execution worker**, hostname, pool and tags after the job acquires a slot. This is distinct
from the native engine's coordinator process. Prefect serve and Trigger development execution are
supported by the shared runtime extension; choosing a native queue is not a PC-affinity guarantee.

## Add a PC

Clone this repository on the execution PC, create the agents virtualenv, install
`agents/requirements.txt` and `npm ci --prefix agents`, and install/log in to the desired agent
CLIs using the existing [agent instructions](../agents/README.md). Then run:

```powershell
$env:FLOW_WORKER_ID = 'build-pc'
$env:FLOW_WORKER_POOL = 'builds'
$env:FLOW_WORKER_TAGS = 'windows,gpu'
$env:FLOW_WORKER_SLOTS = '2'
$env:FLOW_WORKER_BIND = '0.0.0.0'
$env:FLOW_WORKER_TOKEN = '<shared service token>'
agents/.venv/Scripts/python.exe -u agents/flow_execution_worker.py
```

On Linux use `agents/.venv/bin/python` and the corresponding shell environment syntax. Configure
private networking or HTTPS termination for a service beyond loopback. This endpoint can execute
commands with that PC's account; use trusted clients and restrict network access accordingly.
The service refuses network binding without a token. No native engine credentials are required
on execution PCs. Agent credentials, Confluence tokens and source file paths belong on the PC
performing setup/execution.

On the coordinator PC copy [workers.example.yaml](../workers.example.yaml) to ignored
`workers.local.yaml`, adding that PC's URL, id, pool, tags and `token_env`. Put the matching secret
in that environment variable or ignored `workers.local.env` (`NAME=value`). All native coordinators
read that registry; they currently run on the same flow-host PC and share its `FLOW_WORKSPACES`
artifact store. Worker services can be on other PCs. The board receives a sanitized health catalog,
never registry URLs or tokens. `FLOW_WORKERS_FILE` can override the registry location.

Choose a default worker/pool and required tags when starting a flow. Expand individual steps to
override placement per step. Offline or unmatched selections fail explicitly. When restarting a
finished step, its node panel can choose a new execution worker; descendants retain their own
selections and restart through the engine dependency mechanism. Hatchet replays its native DAG;
the Prefect/Trigger extension creates a new native run and reuses unaffected results.
Per-step tags keep the default pool; an explicit worker id chooses that worker independently of
the default pool. Clearing a restart override returns to the flow's selection.

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
