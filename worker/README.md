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

Releases are unsigned Linux and Windows binaries plus `SHA256SUMS`. The checksum detects transfer
corruption; it does not identify the publisher or prevent SmartScreen/Defender reputation warnings.
This project does not currently sign Windows releases.


Worker commands resolve the installed home from the executable's marked install folder, then the marked
default install. A temporary downloader uses the marked default; a custom install can use `--home <DIR>`
or `AGENT_WORKER_HOME`, which take precedence.
The version a worker reports is the first 16 hex characters of the SHA-256 of its program, so
the server and a PC always agree on what "the same version" means.

The worker provides program steps and cards, agent sessions over ACP (permissions, questions,
terminals, interrupts, retries), the built-in demo agent, environment setup and files, repositories,
and Confluence page trees attached to a run. The worker binary needs no Python; `git` is needed for
repositories.

The Fleet installer runs `npm ci` for the pinned ACP adapters (Claude Code, Codex, opencode v2 and
pi) into the worker's `adapters/` directory. Node.js and npm must be available on the PC before
install; `npm_config_registry` can point to an offline mirror such as `http://<server>:4873/`. The
mirror must already cache the adapter dependencies. For an existing worker,
`agent-worker agents install [--registry URL]` installs or refreshes the adapters;
`agent-worker agents status` shows what can run. `AGENT_WORKER_ACP_<AGENT>` can override an adapter
with a JSON command list.
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
