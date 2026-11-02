# Hatchet (self-hosted, Docker Compose)

Hatchet `v0.114.2`, Postgres-only (no RabbitMQ), offline-friendly. Verified end-to-end on 2026-11-02:

- A plain task, two DAG workflows (incl. `research-report`) and a durable task ran through the example worker.
- A steerable agent was redirected and stopped mid-run, and another was cancelled.
- Hardware-bound tasks were routed to labelled workers.
- Traces were collected.

| URL / port | What |
|---|---|
| http://localhost:8080 | Dashboard + REST API (login `admin@example.com` / `Admin123!!` unless changed in `.env`) |
| `localhost:7077` | gRPC engine; this is what workers connect to |
| `127.0.0.1:5435` | Postgres (host access for backups and debugging) |

## Setup (about 2 minutes, 4 commands)

```bash
cd hatchet
cp .env.example .env          # set HATCHET_HOST, admin password, POSTGRES_PASSWORD before first start
docker compose up -d          # migration -> setup-config -> engine + dashboard
./scripts/create-token.sh     # writes HATCHET_CLIENT_TOKEN into .env
docker compose --profile worker up -d --build   # optional example worker
```

To trigger the examples:

```bash
docker compose exec example-worker python trigger.py
```

Then open the dashboard and look at **Runs** and **Workers**.

### Steerable agents, hardware workers, traces

```bash
docker compose --profile worker --profile hardware up -d --build    # + two simulated hardware PCs
docker compose exec example-worker python demo_steer_and_hardware.py
```

These were tested on 2026-11-02.

| Feature | How | Files |
|---|---|---|
| **Steer** a running agent | Push an event scoped to the run: `hatchet.event.push("agent:steer", {"action": "redirect", "instruction": ...}, scope=run_id)`. `{"action": "stop"}` ends it gracefully. The agent durably waits on `or_(SleepCondition, UserEventCondition)` between steps. | `example-worker/steering.py` |
| **Interrupt** | Dashboard **Cancel**, or `hatchet.runs.cancel(run_id)` | — |
| **Hardware** | Run `hardware_worker.py` on each PC that has the device. Use `STATION=...` and `SENSOR_DEVICE=/dev/...`, natively or with Docker `devices:`. Tasks declare `desired_worker_labels=[device=sensor, required]`. Callers can pin a run with `desired_worker_labels=[station=...]`. `slots=1` serializes device access. | `example-worker/hardware_worker.py`, `hardware-station-a/-b` services |
| **Traces** | `SERVER_OBSERVABILITY_ENABLED=true` turns on the engine's OTel collector, and `HatchetInstrumentor` in the worker sends spans to it. Open a run in the dashboard to see its trace. Add custom spans with `tracer.start_as_current_span(...)`. | `example-worker/tracing.py` |
| **Shared trace backend** (optional) | Start `../observability` (Jaeger), then set `COMPOSE_FILE=docker-compose.yml:docker-compose.otel.yml` in `.env`. Workers then also export to `http://jaeger:4318`. | `docker-compose.otel.yml` |

Containers reach the REST API (cancel, status) through `HATCHET_CLIENT_SERVER_URL=http://hatchet-dashboard:80`, because the URL inside the token is `localhost:8080`.

### Running a worker outside Docker (on your dev machine)

```bash
pip install -r example-worker/requirements.txt
export HATCHET_CLIENT_TOKEN=...            # from .env
export HATCHET_CLIENT_TLS_STRATEGY=none
python example-worker/worker.py            # the token already contains <HATCHET_HOST>:7077
```

## What the services do

- `migration`: one-shot. Applies DB schema migrations on **every** `up`, which is how upgrades get applied.
- `setup-config`: one-shot. Generates encryption keys, cookie secrets and config into the `hatchet_config` volume, then seeds the admin user and default tenant. It runs with `--overwrite=false`, so it never rotates existing secrets.
- `hatchet-engine`: the scheduler and queue. Workers connect to it over gRPC.
- `hatchet-dashboard`: REST API plus the web UI.

## Choices made here (and why)

- **No RabbitMQ** (`SERVER_MSGQUEUE_KIND=postgres`). That's one less image and service to carry into the offline network. Hatchet supports this officially. Add RabbitMQ only for very high throughput.
- **Everything is pinned through `HATCHET_VERSION`.** No `:latest` anywhere, so a bundle always matches what you tested.
- **Outbound calls are off.** That covers `SERVER_SECURITY_CHECK_ENABLED=false` (by default the server calls `security.hatchet.run`), PostHog, Sentry and Pylon.
- **`HATCHET_HOST` is the address baked into API tokens.** Workers dial it. In the offline network, set it to the server's DNS name or IP *before* the first `up`.

## Upgrading (future versions)

The compose file follows the official docs layout. The only thing tied to a version is `HATCHET_VERSION`.

```bash
docker compose exec postgres pg_dump -U hatchet hatchet > backup-$(date +%F).sql   # 1. back up
sed -i 's/^HATCHET_VERSION=.*/HATCHET_VERSION=v0.115.0/' .env                       # 2. bump
docker compose pull && docker compose up -d                                       # 3. migrations run automatically
```

Also bump `hatchet-sdk` in `example-worker/requirements.txt` (and in your own workers) to a compatible SDK version. Read the release notes first: https://github.com/hatchet-dev/hatchet/releases. For the offline, re-run `../offline/bundle.sh` afterwards.

## Changing host or ports later

The broadcast address is stored in the generated config **and** in every token. After changing `HATCHET_HOST` or `HATCHET_GRPC_PORT`:

```bash
docker compose down
docker volume rm hatchet_hatchet_config
docker compose up -d
./scripts/create-token.sh
```

Re-deploy any workers that use the old token.

## Limitations and gotchas found

- Workers are **your** processes. Hatchet schedules work onto them but doesn't build, deploy or scale them. That's simple and very offline-friendly, but packaging, rollout and versioning of worker code are on you (Docker images, systemd, k8s...).
- TLS is off (`SERVER_GRPC_INSECURE=t`, insecure cookies). Put it behind a TLS reverse proxy, or enable Hatchet's own TLS, before real use.
- Changing the gRPC host after first boot means regenerating the config and tokens (see above).
- With Git Bash on Windows, `docker compose run ... /hatchet/...` paths get rewritten. The scripts set `MSYS_NO_PATHCONV=1`.
- Lighter alternative: Hatchet Lite (`ghcr.io/hatchet-dev/hatchet/hatchet-lite`) runs everything in one container plus Postgres. It's good for a laptop, but this repo uses the multi-service layout because it's closer to production.
