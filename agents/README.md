# Local coding sessions

For per-step execution on different PCs, worker pools/tags, transferred workspaces and an
interactive context setup agent, see [Workers and environment context](../docs/workers-and-context.md).

The [Agent Board](http://localhost:8090) can now run coding agents through ACP on this PC.
Choose **Coding agent session**, an agent, and a prompt. Leave Workspace blank to create a separate
temporary Python project with a failing test, or enter an existing absolute directory on the worker PC.
The default demo prompt asks the agent to fix that test. Agents run with your local CLI login and tools.

The chat shows the model, mode, permission and other configuration choices advertised by the
agent, along with context usage and cost when provided. Type `/` for advertised commands.
Supported agents can receive image and file attachments (up to five, 96 KiB total). ACP terminal
commands require confirmation and run within the session workspace; their output is shown live.
To resume a session after a restart, copy its ACP session ID from the chat and supply it when
creating a session with the same agent and workspace. Loading depends on agent support; if it
is unavailable, a transport retry reconstructs recent conversation context instead.

## Start the local workers (Windows / PowerShell)

From the repository root:

```powershell
python -m venv agents/.venv
agents/.venv/Scripts/python.exe -m pip install -r agents/requirements.txt
npm.cmd ci --prefix agents
agents/.venv/Scripts/python.exe -u agents/hatchet_worker.py
```

The Hatchet worker reads the ignored `hatchet/.env` and uses localhost:7077 / localhost:8080 by default.
Environment variables override these defaults. Keep this terminal running. This native worker serves
only `agent-session`; the Docker example workers continue serving the original examples.
Hatchet SDK 1.44.0 has two Unix signal assumptions; `hatchet_windows.py` adapts this worker only.
The shim checks the exact SDK version and must be reviewed when upgrading it.

If this virtualenv was created without pip, install the same worker requirements with
`uv pip install --python agents/.venv/Scripts/python.exe -r agents/requirements.txt` instead.
The requirements include the pinned Confluence Markdown Exporter used by environment setup.

For Prefect, in another terminal (serves `agent-session/local`; PREFECT_API_URL defaults to localhost:4200):

```powershell
agents/.venv/Scripts/python.exe -u agents/prefect_worker.py
```

In another terminal:

```powershell
cd trigger.dev/example-project
.\scripts\dev-worker.ps1
```

The script authenticates with `TRIGGER_API_URL` / `TRIGGER_ACCESS_TOKEN` from the ignored
`.env.local` (a personal access token), so no interactive `trigger login` is needed. It pipes the
CLI's output to keep it non-interactive: trigger.dev 4.8.3 ignores `--skip-mcp-install` (it reads
`options.skipMCPInstall`), and its first-run MCP question would otherwise block the worker.
Run it in a terminal that stays open. A worker whose console is closed keeps picking up runs and
crashes each one at startup (`0xC0000142`). The project's `.env` needs:

```dotenv
TRIGGER_STREAM_URL=http://localhost:8030
```

The Trigger task discovers this repository's `agents/` directory. If moving it, configure
`AGENT_ROOT` (absolute agents directory) and `AGENT_PYTHON` (absolute Python executable) in the
development task's environment. Production container runs deliberately reject local coding sessions.

Add the development environment's secret key to ignored `board/.env`:

```dotenv
TRIGGER_DEV_SECRET_KEY=your_development_environment_secret_key
```

Then rebuild the board:

```powershell
cd board
docker compose up -d --build
```

Select **Hatchet** or **Trigger.dev · local**. The existing production tab retains the original examples.

## Portable flows (flow host)

Flows in [`../flows`](../flows/README.md) run natively on all three backends. Start the flow host in
another terminal; it runs the Hatchet and Prefect flow workers, regenerates the Trigger.dev flow tasks
(picked up by the `trigger dev` worker above) when a flow is activated, and serves the board's Flows
API on 127.0.0.1:8095. Saved files are drafts; the workers serve the active snapshots in
`FLOW_STATE_DIR` (default `.flow-state/`), see [Drafts and activation](../flows/README.md#drafts-and-activation):

```powershell
agents/.venv/Scripts/python.exe -u agents/flow_host.py
```

The board reaches it at `FLOW_HOST_URL` (default `http://host.docker.internal:8095`, which Docker
Desktop forwards to this PC's loopback). On Linux, bind it with `FLOW_HOST_BIND`, set
`FLOW_HOST_TOKEN` on both sides, and point `FLOW_HOST_URL` at it. `FLOW_BACKENDS` (default
`hatchet,prefect,trigger`) picks which backends to keep in sync.

The flow host also provides step-restart support for Prefect and local Trigger.dev, where native
dependent-step replay is absent. Its engine endpoints default to this PC's localhost ports.
`FLOW_PREFECT_API_URL` overrides `PREFECT_API_URL`/localhost:4200/api;
`FLOW_TRIGGER_API_URL` overrides `TRIGGER_API_URL`/localhost:8030. Set these in the host's
environment or `board/.env`. The Trigger extension reads `TRIGGER_DEV_SECRET_KEY` from that file
or the host's environment. The board only forwards backend/run/step IDs to this control plane.
Exact outputs and definition fingerprints live in the worker's run workspace; see
[restart behavior and limits](../flows/README.md#watching-and-restarting-steps).

## Execution PCs (fleet)

Flow steps and coding agent cards can run on other PCs. In the board's **Fleet** tab, **Register
PC** takes a short form (name, operating system, engines, pool, tags, slots and limits, and the
address the PC uses to reach the server). When it is valid you get one command per shell
(PowerShell, cmd, or bash) to paste on the PC **as a normal user, with no Administrator or sudo**.
It downloads the worker from the server, installs it from PyPI and npm, creates the PC's own token,
starts a per-user startup entry and registers the PC. Nothing on the PC is opened to the network:
the PC connects out to the server and the server's relay carries the coordinator's calls over that
connection (see [Why the PC needs no open port](../docs/workers-and-context.md#why-the-pc-needs-no-open-port-the-relay)).
The coordinator's registry (`workers.local.yaml`, token values in `workers.local.env`) owns pools,
tags, slots and the CPU and RAM limits (90% by default); **Register by URL (advanced)** and
legacy registries keep working for PCs the coordinator can reach directly, and a legacy registry is
migrated idempotently, with a backup-and-restore rollback. Native engine placement is not enabled:
Hatchet, Prefect and Trigger.dev all reach a PC through its execution service, and Trigger.dev
cards always run on the coordinator.

When you start a flow or a placed card, the New card dialog shows where each step can run: ready,
will wait (a matching PC is offline, busy or not freshly checked) or blocked (no registered PC can
run it, with how to fix it). Only a blocked launch is refused; a flow draft can always be saved.
While a placed step or card waits, it occupies its engine task (the engine shows it as running)
and the card drawer shows the waiting reason live; cancelling, the step's timeout and engine-side
interruptions are recorded as `cancelled`, `timed_out` and `interrupted` with the last reason.

Admission uses machine-wide CPU and RAM averaged over 10 seconds (sampled every second; unknown
after a gap or a sample older than 3 seconds; RAM is physical memory in use, total minus
available), one slot count shared by every engine on the PC, and a 10-second cooldown after each
start. Thresholds only gate new work. **POSIX limitation:** if a service crashes on Linux while a
job's process group keeps running, its slot stays reserved until that group is verified dead or the
PC reboots; unattended POSIX crash recovery is not supported, so stop the orphaned processes by hand.

Setup, migration and rollback, per-engine registration, metric semantics, limits, the fallback and
every waiting reason are described in [Workers and environment context](../docs/workers-and-context.md).

## Chat controls

- **Send message** while busy queues the message for the next turn. **Interrupt turn** cancels
  the current turn; send a new instruction to steer immediately. It keeps the conversation open.
- Permission cards display the exact options offered by the agent. Allow for session appears
  only when the agent supports that permission scope. The board never auto-approves tools.
- Question forms preserve JSON Schema field types, single/multiple choices and free text.
  Agents that ask questions as ordinary prose can be answered through the composer instead.
- A successful turn goes to **Awaiting your review**, in the Waiting column. **Continue** sends
  another turn; only **Mark done** completes the session.
- Three consecutive failures go to **Stuck**. Continue retries. **Simulated failures** under
  **Advanced** in New card exercises this without needing an actual provider error.
- **Demo (simulated)** exercises permissions and questions without a model or file changes.
  Codex and Claude are real agents; their availability depends on their CLI login/provider.

## Agents (all through ACP)

| Agent | Entry point | Live status on this PC (2026-11-02) |
|---|---|---|
| opencode v2 | `@opencode/cli` 2.2.5, installed in `agents/node_modules` (`opencode acp`). Your global v1 `opencode` is unchanged. | **Verified end-to-end through Hatchet**: 3 approvals, fix, review, follow-up, done. Uses v2's shared background service. |
| opencode | `opencode acp` (native) | **Verified end-to-end through Trigger.dev, Prefect and the chat UI.** It fixed the test, asked 3–4 permissions per run, went to review, took a follow-up, and was marked done. |
| Codex | `@agentclientprotocol/codex-acp` | Verified through Trigger.dev earlier: 2 approvals, fix, review. Then the account hit its usage limit. The Hatchet session shows that as a real **Stuck** after 3 retries. |
| Claude Code | `@agentclientprotocol/claude-agent-acp` | The CLI login has expired: run `claude`, then `/login`. |
| omp | `omp acp` (native) | Starts. It asked a real question through the question form ("spend a saved Codex rate-limit reset?"), because it uses the same rate-limited Codex account. |
| pi | `pi-acp` adapter (spawns `pi --mode rpc`) | Starts and takes turns, but pi's configured models fail: `nim/z-ai/glm-5.1` returns 410 and `groq` returns 404. Pick a working model in pi. pi-acp reports such failed turns as finished, so the board shows **review** rather than **stuck**. |

Permission prompts come from each agent's own permission settings, not from ACP:

- **opencode** allows every tool by default. Board sessions pass `OPENCODE_CONFIG_CONTENT` with
  `edit`, `bash` and `webfetch` set to `ask`. This is merged over your own opencode config, which
  stays unchanged.
- **Codex** starts in its read-only mode, so edits and commands ask.
- **Claude Code** uses its default permission mode.

## Verify

```powershell
agents/.venv/Scripts/python.exe -m unittest discover -s agents/tests -v
agents/.venv/Scripts/python.exe -m unittest discover -s agents/integration_tests -v
agents/.venv/Scripts/python.exe -m unittest discover -s agents -p 'test_hatchet*.py' -v
agents/.venv/Scripts/python.exe -m unittest discover -s board -p 'test_*.py' -v
node --test board/static/*.test.cjs   # unit tests and headless-Chrome browser tests (chat, flows, fleet, launch)
$env:AGENT_PYTHON = (Resolve-Path agents/.venv/Scripts/python.exe).Path
trigger.dev/example-project/node_modules/.bin/tsx.cmd --test trigger.dev/example-project/src/agent-session.test.ts
trigger.dev/example-project/node_modules/.bin/tsc.cmd --project trigger.dev/example-project/tsconfig.json
python board/session_smoke_test.py hatchet trigger-dev
python board/smoke_test.py hatchet trigger
agents/.venv/Scripts/python.exe board/workers_smoke_test.py --base http://127.0.0.1:8093  # needs idle engines and a dedicated board
```

The agent unit/fixture tests and public Confluence integration tests also run in the
[Tests workflow](../.github/workflows/tests.yml) on pushes, pull requests and a daily schedule
(scheduled workflows run on the default branch). The public suite needs internet access and
anonymous access to the selected third-party sites. It imports four public page trees and checks
Markdown, attachments, links and transfer between two local worker services. CI also starts
disposable Hatchet and Prefect stacks and runs the public-page setup, worker transfer and native
step restart on each engine. Public-site outages or content changes can fail those separate jobs.
To run only the deterministic/offline suite, omit the `agents/integration_tests` command. Trigger.dev
native-engine smoke still requires a local stack and is not run by this workflow.

The Chrome integration test uses a deterministic API fixture and requires installed Chrome
(override `CHROME_PATH`). The live session smoke test uses simulated agents through both real engines.
For a real agent without the orchestrator, run:

```powershell
agents/.venv/Scripts/python.exe agents/demo_smoke.py --agent codex --approve-demo
```

That last opt-in script automatically approves only the disposable demo's allow-once requests and
marks its result done. Normal dashboard sessions always leave those decisions to you.

## Prototype limits

- The local workers and CLI logins are required. Restart the workers after restarting the PC.
  Engine cancellation/worker failure is terminal; use a new card or replay. Worker restarts do not
  transparently reload a live ACP conversation. Turn-level connection retries reconstruct the
  original goal and recent conversation, with a 48,000-character history window.
- Trigger metadata keeps a rolling transcript (200 compacted events / 24 KiB UTF-8), preserving
  pending requests and the latest status. Older resolved history can be discarded. Oversized active
  requests fail explicitly. Hatchet stores events in persisted task logs.
- Windows job objects clean up adapter descendants, including after adapter exit. The job is assigned
  immediately after process creation; children created before assignment are outside that guarantee.
- Blank-workspace projects are retained in the OS temporary directory for inspection.
- The dashboard has no authentication and can launch local tools. Keep it on a trusted local network.
- The offline distribution bundle has not been regenerated for this UI/driver. Cloud-backed models
  still need their provider; an offline deployment needs a local provider and separately bundled
  adapters, CLIs, Python dependencies and authentication/configuration.
