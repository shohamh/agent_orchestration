# agent-worker

The execution worker for a PC, as one static program for Windows x86_64 and Linux x86_64 (musl).
It runs the execution service, the outbound relay connector and any registered components, and
carries its own `install`, `update`, `uninstall` and control commands. How to register a PC, how
it starts and restarts, and how a fleet update works: [docs/workers-and-context.md](../docs/workers-and-context.md#register-a-pc).

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

Not built yet: running agent sessions (ACP) and the environment/Confluence setup inside the
Rust worker. Those still use the Python worker.
