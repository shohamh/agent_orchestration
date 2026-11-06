# Agent orchestration: Hatchet vs Trigger.dev vs Prefect (self-hosted, offline-ready)

Self-hosted Docker Compose setups for running AI-agent workloads on Hatchet, Trigger.dev and Prefect, built to run in an offline network and to stay upgradable to new upstream releases. Each stack has the same example agents (a DAG pipeline, a durable agent loop, a human-steerable agent, hardware-bound tasks), and [COMPARISON.md](COMPARISON.md) compares the three.

| Folder | What | Local URL |
|---|---|---|
| [hatchet/](hatchet/README.md) | Hatchet v0.114.2: Postgres + engine + dashboard, plus a Python example worker | http://localhost:8080 |
| [trigger.dev/](trigger.dev/README.md) | Trigger.dev v4.8.3 combined stack, plus a TypeScript example project | http://localhost:8030 |
| [board/](board/README.md) | Agent Board over Hatchet, Trigger.dev and Prefect: live cards, DAGs, per-agent chat, program logs, step restart and native-dashboard links. | http://localhost:8090 |
| [agents/](agents/README.md) | Local ACP coding sessions inside the Agent Board: chat UI, permissions, question forms, interrupt, Continue and human completion. | http://localhost:8090 (the board) |
| [prefect/](prefect/README.md) | Prefect 3.9.1 server, served example flows and two hardware-station workers; tested against the same requirements, including on a network with no internet route | http://localhost:4200 |
| [observability/](observability/docker-compose.yml) | Optional shared trace backend (Jaeger, OTLP) for agents on all three systems | http://localhost:16686 |
| [offline/](OFFLINE.md) | `bundle.sh` (connected side) / `load.sh` (offline side), offline npm mirror | — |
| [COMPARISON.md](COMPARISON.md) | Capabilities, ops cost and offline difficulty compared | — |
| [comparison-page/](comparison-page/index.html) | The comparison as a single web page, published by `.github/workflows/pages.yml` | https://shohamh.github.io/agent_orchestration/ |

All three stacks run side by side on one machine with no port conflicts. Idle RAM is about 0.7 GB for Hatchet, 2 GB for Trigger.dev and 0.2 GB for Prefect, so give Docker Desktop at least 4 GB to run them together.

## What's built on top

Beyond the three stacks, the repo now holds a layer that runs the same agent work on any of them. Each item links to its own documentation.

| Feature | What it gives you | Docs |
|---|---|---|
| **Portable flows** | One YAML DAG (program, agent and approval steps) that runs natively on Hatchet, Prefect and Trigger.dev. Saved files are drafts; activation is explicit and reversible. Native step restart where the engine lacks it, run and step times in your timezone, and examples such as `slug-relay` (a multi-agent flow made in the builder) and `multi-agent-design`. | [flows/](flows/README.md) |
| **Graphical workflow builder** | Build and edit a flow on a canvas in the board: drafts, layout, a graph and steps view, launch checks, and a confirmed activation that shows which engines and active runs a change affects. | [flows/](flows/README.md#graphical-builder) |
| **Agent Board upgrades** | Live card updates, drag and drop between columns (engine-aware drop targets, priority order in Queued), live search filter, big boards (20,000 cards stay responsive, cards load on scroll), per-step chat and a login for phone access. | [board/](board/README.md) |
| **ACP coding sessions** | Codex, Claude Code, OpenCode and others run as cards through ACP: permissions and question forms, interrupt and Continue, slash commands, model and mode settings, usage and cost, attachments, resume and confirmed terminal commands. | [agents/](agents/README.md) |
| **Workers and environment context** | Per-step workers with pools and tags, transferred workspaces, an interactive context-setup agent, and Confluence pages exported as Markdown. | [docs/workers-and-context.md](docs/workers-and-context.md) |
| **Declarative worker fleet** | A Fleet tab to register execution PCs once. Steps and cards declare what they need (software, pool, tags) and are placed on a compatible PC, or wait visibly with the reason. Shared CPU, RAM and slot limits per PC, capability detection with last-checked times, launch checks, immutable per-revision flow assets, and cancel and settings changes that cannot race a start. Works the same on all three engines and offline. | [Execution PCs](agents/README.md#execution-pcs-fleet) |
| **One-line PC install and `agent-worker`** | Register a PC with a validated form and paste one command (PowerShell, cmd or bash, no Administrator or sudo). The server issues a uuid for the install and the single static `agent-worker` binary (Windows and Linux x86_64, no Python or Node on the PC) claims it, receives its settings and refuses a second PC with the same command. The form chooses when the worker starts (at logon, at boot before anyone logs in, or never) and explains the consequences; everything works without administrator rights, and starting at boot asks for UAC approval on Windows. The PC connects out to the server, so no inbound port or firewall rule is needed. `agent-worker status/start/stop/restart/logs/update/uninstall/component` manage it, and Fleet → Update workers updates the whole fleet (drain, swap, roll back). GitHub Actions and GitLab CI build and release the binaries from the same scripts. | [Register a PC](docs/workers-and-context.md#register-a-pc) |

The board's tests (Python, Node and headless-Chrome) and the agent tests are listed under [Verify](agents/README.md#verify).

## The plan

1. **Research.** I read the official self-hosting docs and the upstream compose files, plus the parts of the Trigger.dev CLI and webapp source that affect offline use: how deploys build images, the base images, and the API origin handling.
2. **Compose setups that track upstream.**
   - **Hatchet** follows the documented compose layout. One version variable (`HATCHET_VERSION`) controls it, and outbound calls are off.
   - **Trigger.dev** uses the *unmodified* upstream files from a release tag. All local changes live in one override file, and `scripts/update-upstream.sh <tag>` upgrades it.
   - **Prefect** runs the official server image with Postgres. One version variable (`PREFECT_VERSION`) controls the server and the workers, and analytics are off.
3. **Verify end to end.** Each tool runs the same examples: a task, a fan-out "agent pipeline", a `research-report` DAG (fan-out, chain and fan-in) and a looping "agent" with waits. For Trigger.dev this includes a real `trigger deploy` through the local registry.
4. **Offline.**
   - Bundle everything on the connected side: images for all three stacks, Trigger.dev deploy base images (digest-preserving), a Verdaccio npm cache and Linux Python wheels.
   - Verify checksums and load it on the offline side.
   - Point Trigger.dev deploys at the mirrored base images and the npm mirror.
   - All lists are derived from the compose files and installed CLIs, so upgrades just mean re-bundling.
5. **Agent requirements.** I tested three requirements in all three tools, with example code for each:
   - Hardware attached to a specific PC.
   - Human interrupt and steering.
   - Trace collection: in Hatchet's and Trigger.dev's own dashboards, and for all three in a shared Jaeger (Prefect has no trace viewer of its own).
6. **Compare.** See [COMPARISON.md](COMPARISON.md).

## Quick start (connected machine)

```bash
# Hatchet
cd hatchet
cp .env.example .env && docker compose up -d && ./scripts/create-token.sh
docker compose --profile worker up -d --build && docker compose exec example-worker python trigger.py

# Trigger.dev
cd ../trigger.dev
cp .env.example .env && cat local.env.example >> .env && ./generate-secrets.sh
docker compose up -d   # then follow trigger.dev/README.md (login, project, deploy)

# Prefect
cd ../prefect
cp .env.example .env && docker compose up -d --build
docker compose exec example-worker python demo.py
```

## Secrets and local files

No secrets are committed. Every `.env`, `webapp.env`, `.env.local`, generated registry password and offline bundle (`dist/`) is in [.gitignore](.gitignore). Each one has a committed `*.example` next to it, or is created by a script (`trigger.dev/generate-secrets.sh`, `hatchet/scripts/create-token.sh`, `offline/load.sh`). The example files hold local-only defaults (for example the Hatchet admin password), so change them before exposing a stack beyond your machine.

## Upgrading later

| | How |
|---|---|
| Hatchet | Bump `HATCHET_VERSION` in `hatchet/.env`, then `docker compose pull && docker compose up -d`. Migrations run automatically. Bump `hatchet-sdk` in your workers. |
| Trigger.dev | Run `trigger.dev/scripts/update-upstream.sh vX.Y.Z`, then `docker compose pull && docker compose up -d --build`. Bump `trigger.dev` / `@trigger.dev/*` in projects to the same version. |
| Prefect | Bump `PREFECT_VERSION` in `prefect/.env`, then `docker compose up -d --build`. The server migrates its database on start, and the workers are rebuilt with the matching SDK. |
| Offline | Re-run `./offline/bundle.sh` and carry the new bundle over. |

Back up Postgres before every upgrade. Each README shows the command.
