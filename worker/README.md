# agent-worker

The execution worker for a PC, as one static program for Windows x86_64 and Linux x86_64 (musl).
It runs the execution service, the outbound relay connector and any registered components, and
carries its own `install`, `update`, `uninstall` and control commands. How to register a PC, how
it starts and restarts, and how a fleet update works: [docs/workers-and-context.md](../docs/workers-and-context.md#register-a-pc).

Dependencies are vendored in `vendor/` (`.cargo/config.toml` points cargo at them), so every build works offline.
After changing `Cargo.toml` run `ci/vendor.sh` (needs network once) and commit the result.

Iterate with a debug build (`cargo build`, incremental, tens of seconds); the tests pick the newest of `target/debug` and
`target/release`. Release with LTO (`ci/build.sh`, `ci/build.ps1`) is for CI and final versions only.

```
cargo test                                # unit tests (gate, templates, artifacts, capabilities)
ci/build.sh                               # Linux musl build  -> ../dist/worker/
ci/build.ps1                              # Windows MSVC build -> ../dist/worker/
ci/fetch-release.sh github OWNER/REPO worker-v0.1.0   # download a release instead of building
```

Releases: push a tag `worker-vX.Y.Z` (it must match `version` in `Cargo.toml`). GitHub Actions
(`.github/workflows/worker-release.yml`) and GitLab CI (`.gitlab-ci.yml`) call the same
scripts and publish `agent-worker-x86_64-unknown-linux-musl`, `agent-worker-x86_64-pc-windows-msvc.exe`
and `SHA256SUMS`. On GitLab set `WINDOWS_RUNNER_TAG` to your Windows runner's tag.

The version a worker reports is the first 16 hex characters of the SHA-256 of its program, so
the server and a PC always agree on what "the same version" means.

The worker runs what a Python execution worker runs, with nothing else to install but the
agents themselves: program steps and cards, agent sessions over ACP (permissions, questions,
terminals, interrupts, retries), the built-in demo agent, environment setup and files,
repositories and Confluence page trees attached to a run. `git` is needed for repositories.

Agents: `agent-worker agents install [--registry URL]` puts the pinned npm adapters for Claude
Code, Codex, opencode v2 and pi next to the worker (needs Node.js). `opencode` and `omp` are found
on the PATH. `agent-worker agents status` says what this PC can run; set
`AGENT_WORKER_ACP_<AGENT>` to a JSON list to start an agent another way.
Confluence pages go through the pinned `confluence-markdown-exporter`, the same code a Python worker
runs, with `CONFLUENCE_TOKEN` and `CONFLUENCE_EMAIL` when set. The PC needs no Python: the first run
that needs it fetches a runtime from the server (a relocatable CPython and the pinned exporter)
into `confluence-runtime/` next to the worker. Build it with
`python worker/ci/build-confluence-runtime.py --os windows` and `--os linux` from the repository
root, using a Python with pip installed; `ci/fetch-release.sh` can fetch published archives instead.
Both paths put `confluence-runtime-<os>.tar.gz` in `dist/worker/` on the server. The board relay
preserves its version header, and the worker verifies the archive before using it.
The Linux runtime requires glibc 2.26 or newer; the worker binary itself remains static.
For an offline PC, set `AGENT_WORKER_CONFLUENCE_RUNTIME` to an archive or an unpacked runtime
directory. Neither path installs Python globally or uses a Python already installed on the PC.
