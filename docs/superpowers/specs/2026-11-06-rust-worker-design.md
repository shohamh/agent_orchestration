# The execution worker as a Rust binary (Windows and Linux x86_64)

Date: 2026-11-06. Status: in progress (see "Stages and what is done").

## Why

The worker on each PC is Python: an execution service, a relay connector, a runner and (new) a management command.
Installing it means a virtual environment, `pip install` from PyPI and Python 3.10+ on every PC. That is the packaging
problem: the wrong Python version, a missing `venv` module, a pip that cannot reach PyPI, packages that drift. A single
static binary removes it: the installer downloads one file from this server and runs it. Node.js stays optional, only
for the coding-agent adapters (npm packages that are themselves Node programs).

## Goal

One binary, `agent-worker` (`agent-worker.exe`), for Windows x86_64 and Linux x86_64 (static musl, no libc needed):

- `agent-worker run`: the runner. The execution service on `127.0.0.1`, the relay connector (outbound only), and
  supervision of extra components (any engine worker) with crash restarts.
- `agent-worker status | start | stop | restart | update | uninstall | logs | version | component ...`: the command the
  installer puts on the PATH. `update` fetches the server's binary, waits for running jobs, swaps itself and restarts,
  with rollback. A second install on a PC that already has a worker fails.
- The installer scripts download this binary from the server (`/install/<code>/agent-worker`), write a token, install the
  per-user startup entry (Windows Scheduled Task at logon with restart on failure, `systemd --user` with `Restart=always`)
  and register. No Python, no pip, no virtual environment on the PC.
- The server serves the binaries (`dist/worker/`), computes the worker version (SHA-256 of the binary), runs fleet
  updates (connected PCs update within seconds, PCs that are down update when they connect, and an update link or
  `agent-worker update` works on a PC that was down).

## The contract is the existing protocol (nothing changes for the coordinator)

The coordinator code (`distributed.py`, `workers.py`, the relay, the board) keeps working unchanged. The Rust service
implements the execution service's HTTP protocol as the Python one does:

`GET /health`, `GET /capabilities`, `POST /capabilities/refresh`, `POST /capacity/settings`, `POST /drain`,
`POST /jobs` (create, with `authorize`, `gate_id`, `settings_revision`), `GET /jobs/{id}?after=` (state, events, cursor,
authorization nonce, output, files), `POST /jobs/{id}/start` (nonce), `POST /jobs/{id}/input`, `DELETE /jobs/{id}`
(cancel; replies with proof of termination for the gate), bearer-token auth, 190 MB request bodies.

Behaviour kept: admission through the capacity gate (slots, CPU/RAM 10-second averages against limits, cooldown after a
start, settings revisions that refuse stale pushes, a gate identity), the start-authorization handshake (a nonce per
admission, a coordinator that stopped polling never gets capacity), cancellation and termination records, drain for
updates, artifact transfer (workspace files in, changed files out), capability inventory (agents and commands, installed
or absent, with timestamps).

**Oracle.** The Python coordinator and its test suites are the specification. A test harness starts the Rust binary as
the PC and runs the same scenarios (placement, waiting, cancel, restart, relay) against it. A behaviour is done when the
harness passes.

## Stages and what is done

1. **Core worker (program steps).** Crate, gate, service routes, jobs for `program` steps (templates, environment,
   output file, workspace, restart results), artifacts, capabilities, relay connector, runner with components,
   `agent-worker` commands, update with drain and rollback, uninstall, installer scripts, server serving binaries and
   fleet updates. Verified on Windows and in Linux containers, including crash restarts and the real startup entries.
2. **Agent steps.** The ACP session driver (`agentdriver`): drive a coding agent's adapter over stdio (JSON-RPC),
   permissions, questions, interrupt, usage, attachments, plus the built-in `demo` agent.
3. **Environment setup.** Git sources. Confluence export needs the `confluence-markdown-exporter` Python package; it
   stays a Python-only feature (a PC that needs it keeps Python), and the Rust worker refuses such a step with a clear
   error instead of failing in the middle.

## Non-goals

macOS and arm64 binaries (the code avoids OS-specific shortcuts, so they are a build target away, but they are not
built or tested here). Replacing the coordinator, the engines or the board with Rust. Changing the protocol.

## Decisions I made (change any)

- Single process: the gate is in memory with its settings persisted to JSON, instead of SQLite shared by several
  processes. One service per PC is already the design.
- Steps run in the service process (a child process per program step), not in a Python child with a handshake.
- `python` in a step command resolves from the PC's PATH (the Rust worker has no interpreter of its own to substitute).
- Static linking: Windows MSVC with the CRT linked statically; Linux musl. rustls instead of OpenSSL, so TLS (https
  servers) works with no system libraries.
- The Python worker stays in the repository until stages 2 and 3 are done, and is used by the test oracle.
