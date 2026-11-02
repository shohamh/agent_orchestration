# Agent orchestration: Hatchet vs Trigger.dev (self-hosted, offline-ready)

Self-hosted Docker Compose setups for running AI-agent workloads on Hatchet and Trigger.dev, built to run in an offline network and to stay upgradable to new upstream releases. Each stack has the same example agents (a DAG pipeline, a durable agent loop, a human-steerable agent, hardware-bound tasks), and [COMPARISON.md](COMPARISON.md) compares the two.

| Folder | What | Local URL |
|---|---|---|
| [hatchet/](hatchet/README.md) | Hatchet v0.114.2: Postgres + engine + dashboard, plus a Python example worker | http://localhost:8080 |
| [trigger.dev/](trigger.dev/README.md) | Trigger.dev v4.8.3 combined stack, plus a TypeScript example project | http://localhost:8030 |
| [board/](board/README.md) | Agent Board: kanban view over both systems. Shows queue position and live progress, and can steer, stop, cancel and replay cards. | http://localhost:8090 |
| [prefect/](prefect/docker-compose.yml) | Prefect 3 server and example flows for the same scenarios (work in progress) | http://localhost:4200 |
| [observability/](observability/docker-compose.yml) | Optional shared trace backend (Jaeger, OTLP) for agents on both systems | http://localhost:16686 |
| [offline/](OFFLINE.md) | `bundle.sh` (connected side) / `load.sh` (offline side), offline npm mirror | — |
| [COMPARISON.md](COMPARISON.md) | Capabilities, ops cost and offline difficulty compared | — |
| [comparison-page/](comparison-page/index.html) | The comparison as a single web page | — |

Both stacks run side by side on one machine with no port conflicts. Docker Desktop needs about 3 GB of RAM for both.

## The plan

1. **Research.** I read the official self-hosting docs and the upstream compose files, plus the parts of the Trigger.dev CLI and webapp source that affect offline use: how deploys build images, the base images, and the API origin handling.
2. **Compose setups that track upstream.**
   - **Hatchet** follows the documented compose layout. One version variable (`HATCHET_VERSION`) controls it, and outbound calls are off.
   - **Trigger.dev** uses the *unmodified* upstream files from a release tag. All local changes live in one override file, and `scripts/update-upstream.sh <tag>` upgrades it.
3. **Verify end to end.** Each tool runs the same examples: a task, a fan-out "agent pipeline", a `research-report` DAG (fan-out, chain and fan-in) and a looping "agent" with waits. For Trigger.dev this includes a real `trigger deploy` through the local registry.
4. **Offline.**
   - Bundle everything on the connected side: images, Trigger.dev deploy base images (digest-preserving), a Verdaccio npm cache and Python wheels.
   - Verify checksums and load it on the offline side.
   - Point Trigger.dev deploys at the mirrored base images and the npm mirror.
   - All lists are derived from the compose files and installed CLIs, so upgrades just mean re-bundling.
5. **Agent requirements.** I tested three requirements in both tools, with example code for each:
   - Hardware attached to a specific PC.
   - Human interrupt and steering.
   - Trace collection, both in each tool's own dashboard and in a shared Jaeger.
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
```

## Secrets and local files

No secrets are committed. Every `.env`, `webapp.env`, `.env.local`, generated registry password and offline bundle (`dist/`) is in [.gitignore](.gitignore). Each one has a committed `*.example` next to it, or is created by a script (`trigger.dev/generate-secrets.sh`, `hatchet/scripts/create-token.sh`, `offline/load.sh`). The example files hold local-only defaults (for example the Hatchet admin password), so change them before exposing a stack beyond your machine.

## Upgrading later

| | How |
|---|---|
| Hatchet | Bump `HATCHET_VERSION` in `hatchet/.env`, then `docker compose pull && docker compose up -d`. Migrations run automatically. Bump `hatchet-sdk` in your workers. |
| Trigger.dev | Run `trigger.dev/scripts/update-upstream.sh vX.Y.Z`, then `docker compose pull && docker compose up -d --build`. Bump `trigger.dev` / `@trigger.dev/*` in projects to the same version. |
| Offline | Re-run `./offline/bundle.sh` and carry the new bundle over. |

Back up Postgres before every upgrade. Each README shows the command.
