# Agent Orchestration

A self-hosted platform for running AI-agent sessions and workflows. Use the web Board to create and monitor work, connect execution PCs, and run portable workflows on Hatchet, Prefect, or Trigger.dev. The repo includes example engine stacks and tools for offline deployment.

## What it does

- **Board:** create coding-agent sessions, command cards, schedules, and workflows; follow live progress, respond to permission or question prompts, and review results.
- **Portable workflows:** define a workflow once in YAML, edit it in the visual builder, then activate it on Hatchet, Prefect, or Trigger.dev.
- **Worker fleet:** register Windows or Linux PCs to run work. The `agent-worker` service connects outbound to the server, so Fleet workers need no inbound port.
- **Offline deployment:** bundle container images, worker runtimes, Python dependencies, and npm packages on a connected machine, then transfer the bundle to the offline host.

## Start the Board

Requirements: Docker Compose v2. From the repository root:

```bash
cp .env.example .env
```

Set `FLOW_HOST_TOKEN` in `.env` (for example, `openssl rand -hex 32`), then start the Board and flow host:

```bash
docker compose up -d --build
```

Open [http://localhost:8090](http://localhost:8090). Engine servers run separately; start at least one engine and configure its credentials before launching work. See the engine setup links below. For access beyond localhost, set a Board password and follow the [Board security guidance](board/README.md#login-and-phone-access-tailscale-funnel).

## Run work

1. Start the Board and an engine: [Hatchet](hatchet/README.md), [Prefect](prefect/README.md), or [Trigger.dev](trigger.dev/README.md).
2. Register an execution PC in **Fleet → Register PC** and run its generated install command. The worker needs Node.js/npm for the pinned ACP adapters. See [worker installation and lifecycle](docs/workers-and-context.md#register-a-pc).
3. Create a coding-agent session or command card in **New card**. Real coding agents also need their CLI and provider credentials on the worker. The built-in `demo` agent is a no-credentials connectivity check.

For portable workflows, open **Flows** in the Board or see the [workflow guide](flows/README.md). Saved workflows are drafts; activation is a separate, explicit action.

## Offline deployment

Build the bundle on a connected machine and transfer it to the offline host. The bundle includes the selected images and runtimes, Python wheels, worker binaries, and npm mirror contents; it contains no credentials. Follow [OFFLINE.md](OFFLINE.md) for prerequisites, bundling, transfer, and startup steps.

## Repository map

| Path | Purpose |
|---|---|
| [`board/`](board/README.md) | Web Board, engine status, cards, chat, Fleet, and workflow builder |
| [`agents/`](agents/README.md) | Coding-agent sessions, worker services, and execution adapters |
| [`flows/`](flows/README.md) | Portable YAML workflows and examples |
| [`hatchet/`](hatchet/README.md) | Hatchet example stack |
| [`prefect/`](prefect/README.md) | Prefect example stack |
| [`trigger.dev/`](trigger.dev/README.md) | Trigger.dev example stack |
| [`offline/`](OFFLINE.md) | Offline bundle and deployment instructions |
| [`COMPARISON.md`](COMPARISON.md) | Engine capabilities and operational trade-offs |

See the [worker guide](docs/workers-and-context.md) for Fleet setup, identity, updates, and troubleshooting. Each component README has its detailed configuration and verification steps.
