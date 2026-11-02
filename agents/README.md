# Local coding sessions

The [Agent Board](http://localhost:8090) can now run coding agents through ACP on this PC.
Choose **Coding agent session**, an agent, and a prompt. Leave Workspace blank to create a separate
temporary Python project with a failing test, or enter an existing absolute directory on the worker PC.
The default demo prompt asks the agent to fix that test. Agents run with your local CLI login and tools.

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

In another terminal:

```powershell
cd trigger.dev/example-project
npm.cmd run dev
```

Use the existing `self-hosted` Trigger CLI profile. Its `.env` needs:

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

## Chat controls

- **Send message** while busy queues the message for the next turn. **Interrupt turn** cancels
  the current turn; send a new instruction to steer immediately. It keeps the conversation open.
- Permission cards display the exact options offered by the agent. Allow for session appears
  only when the agent supports that permission scope. The board never auto-approves tools.
- Question forms preserve JSON Schema field types, single/multiple choices and free text.
  Agents that ask questions as ordinary prose can be answered through the composer instead.
- A successful turn goes to **Awaiting your review**, in the Waiting column. **Continue** sends
  another turn; only **Mark done** completes the session.
- Three consecutive failures go to **Stuck**. Continue retries. **Simulated failures** in the
  create form exercises this without needing an actual provider error.
- **Demo (simulated)** exercises permissions and questions without a model or file changes.
  Codex and Claude are real agents; their availability depends on their CLI login/provider.

Claude was signed out when checked on 2026-11-02: run `claude`, then `/login`, to enable its live demo.
Codex was exercised against a disposable failing-test project. opencode and omp entry points are
supported but a provider-authenticated live session has not been verified for them.
There is no separate opencode-v2 integration: the prior session did not establish what that name meant.

## Verify

```powershell
agents/.venv/Scripts/python.exe -m unittest discover -s agents/tests -v
agents/.venv/Scripts/python.exe -m unittest discover -s agents -p 'test_hatchet*.py' -v
agents/.venv/Scripts/python.exe -m unittest discover -s board -p 'test_*.py' -v
node --test board/static/chat.test.cjs board/static/chat.browser.test.cjs
$env:AGENT_PYTHON = (Resolve-Path agents/.venv/Scripts/python.exe).Path
trigger.dev/example-project/node_modules/.bin/tsx.cmd --test trigger.dev/example-project/src/agent-session.test.ts
trigger.dev/example-project/node_modules/.bin/tsc.cmd --project trigger.dev/example-project/tsconfig.json
python board/session_smoke_test.py hatchet trigger-dev
python board/smoke_test.py hatchet trigger
```

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
  still need their provider; an air-gapped deployment needs a local provider and separately bundled
  adapters, CLIs, Python dependencies and authentication/configuration.
