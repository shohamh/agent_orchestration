# Trigger.dev v4 (self-hosted, Docker Compose)

This setup runs Trigger.dev `v4.8.3` as the combined single-host stack: the webapp and the worker run on one Docker host. It was verified end-to-end on 2026-11-02:

- Login, project setup and `trigger deploy`.
- Runs of a plain task, a fan-out pipeline, the `research-report` DAG and a task with waits.
- A steerable agent that was redirected, stopped and cancelled.
- Traces in the dashboard and in Jaeger.

| URL / port | What |
|---|---|
| http://localhost:8030 | Dashboard + API. Login is by magic link, sent by email. |
| http://localhost:8025 | Mailpit inbox: login links and alert emails land here until you switch to a real mail server. See [Email](#email-mailpit-or-exchange). |
| `127.0.0.1:5000` | Docker registry for deployed task images (htpasswd auth) |
| `127.0.0.1:9000` / `:9001` | MinIO S3 API / console (large payloads, outputs) |
| `127.0.0.1:5433`, `:6389`, `:9123`, `:19090` | Postgres, Redis, ClickHouse HTTP, ClickHouse native |

## Layout: built for upgrades

```
webapp/docker-compose.yml        \
worker/docker-compose.yml         |  UNMODIFIED upstream files from the tag in UPSTREAM_VERSION
clickhouse/*.xml, .env.example    |  (github.com/triggerdotdev/trigger.dev/tree/<tag>/hosting/docker)
generate-secrets.sh              /
docker-compose.local.yml         <- ALL local changes, as additive overrides
docker-compose.no-bind-mounts.yml<- only for Docker hosts without host file sharing (this Windows box)
local/                           <- Dockerfiles used by the file above
local.env.example                <- local .env additions (COMPOSE_FILE, pinned tags, ...)
webapp.env.example               <- optional webapp settings (signup whitelist, SMTP, base images)
scripts/update-upstream.sh       <- upgrade to a new release
docker-compose.otel.yml          <- optional: lets runs export traces to ../observability (Jaeger)
docker-compose.mailpit.yml       <- local mail inbox for login links (on by default; swap for Exchange later)
certs/                           <- internal CA certificates for the mail server, if needed
example-project/                 <- a Trigger.dev project with example tasks (incl. steerable agent, research-report DAG)
```

`COMPOSE_FILE` in `.env` stacks these files, so a plain `docker compose up -d` works from this folder.

## Setup (about 10 minutes)

```bash
cd trigger.dev
cp .env.example .env && cat local.env.example >> .env
./generate-secrets.sh               # fills every secret + registry/auth.htpasswd
# Linux server: remove ":docker-compose.no-bind-mounts.yml" from COMPOSE_FILE in .env (optional)
docker compose up -d                # ~2.5 GB of images, ~1-2 min to healthy
docker compose logs -f webapp       # wait for "Worker token saved"
```

1. Open http://localhost:8030 and enter any email address.
2. Open the login email in the Mailpit inbox at http://localhost:8025, click the link **in the same browser**, then create an organization and a project.

3. Under **Account → Personal Access Tokens**, create a token for the CLI. The alternative is `npx trigger.dev login -a http://localhost:8030`.
4. Deploy the example project:

   ```bash
   . ./.env && echo "$DOCKER_REGISTRY_PASSWORD" | docker login localhost:5000 -u "$DOCKER_REGISTRY_USERNAME" --password-stdin
   cd example-project && npm install
   export TRIGGER_API_URL=http://localhost:8030 TRIGGER_ACCESS_TOKEN=tr_pat_...
   export TRIGGER_PROJECT_REF=proj_...      # from the dashboard (or edit trigger.config.ts)
   npx trigger.dev deploy --skip-update-check
   ```

5. In the dashboard, go to **Test** (prod environment), pick a task and run it. You can also use `npx trigger.dev dev` for local hot-reload development; runs then execute on your machine.

### Steering, interrupting, tracing

These were tested on 2026-11-02:

```bash
# secret key: dashboard → API keys (prod)
TRIGGER_SECRET_KEY=tr_prod_... TRIGGER_API_URL=http://localhost:8030 npx tsx scripts/demo-steer.ts
```

| Feature | How | Files |
|---|---|---|
| **Steer** a running agent | Input stream `steer`. From outside: `steer.send(runId, { action: "redirect", instruction })` or `{ action: "stop" }`. Inside: `steer.once({ timeoutMs })` between steps. | `src/trigger/steering.ts` |
| **Interrupt** | Dashboard **Cancel**, or `runs.cancel(runId)` | — |
| **Traces** | Always on: open a run and look at the trace view. Custom spans come from `logger.trace("agent.step", ...)`. | — |
| **Shared trace backend** (optional) | Start `../observability` (Jaeger), append `:docker-compose.otel.yml` to `COMPOSE_FILE`, then set the project env var `AGENT_TRACES_OTLP_ENDPOINT=http://jaeger:4318`. `trigger.config.ts` adds an OTLP exporter when that variable is set. | `docker-compose.otel.yml`, `trigger.config.ts` |
| **Hardware on the run's PC** | **Not supported by the Docker supervisor.** Run containers get no devices, volumes or privileged mode. Options: a hardware bridge service on each PC that tasks call over HTTP, or `trigger dev` on that PC (dev mode only). See `../COMPARISON.md`. | — |

## Local changes vs upstream (all in `docker-compose.local.yml`)

| Change | Why |
|---|---|
| `TRIGGER_TELEMETRY_DISABLED=1` | Upstream sends telemetry, and PostHog has a default key. This setup is offline. |
| `INTERNAL_API_ORIGIN=http://webapp:3000` + `INTERNAL_API_ORIGIN_ENABLED=1` | Deployed runs call the API from inside containers, where `localhost:8030` is the container itself. Without this, `triggerAndWait` / `batch` fail with `ECONNREFUSED`. Found while testing. |
| `STREAM_ORIGIN=http://webapp:3000` | Same problem for realtime streams. Without it, **input streams (human steering) never reach the run**. Found while testing. Trade-off: `trigger dev` runs on a laptop get this URL too. If you need input streams in dev mode, give the server a DNS name and use that for `API_ORIGIN`. |
| Registry data volume | Upstream loses pushed images when the registry container is recreated. |
| `NODE_MAX_OLD_SPACE_SIZE=1600` default | Upstream leaves it unset. |
| ClickHouse native port configurable (`CLICKHOUSE_NATIVE_HOST_PORT`) | `9090` is inside a Windows/Hyper-V reserved port range on this machine. |
| `env_file: webapp.env` (optional) | Some settings, such as `WHITELISTED_EMAILS`, SMTP and `DEPLOY_BASE_IMAGES`, can't be passed as empty strings. |
| `docker-compose.no-bind-mounts.yml` | Docker Desktop here has no host folders shared. ClickHouse config and the htpasswd file are baked into small derived images instead. |

## Upgrading (future versions)

```bash
./scripts/update-upstream.sh v4.9.0     # refresh upstream files, show diff, bump TRIGGER_IMAGE_TAG, validate overrides
docker compose exec postgres pg_dump -U postgres main > backup-$(date +%F).sql
docker compose pull && docker compose up -d --build
cd example-project && npm i trigger.dev@4.9.0 @trigger.dev/sdk@4.9.0 @trigger.dev/build@4.9.0
```

The CLI/SDK version should match the server version. The script never touches `.env`, `webapp.env`, the htpasswd file or the override files. If upstream renames a service, `docker compose config` fails at the end of the script and tells you so. Then re-run `../offline/bundle.sh` for the offline network side.

## Email: Mailpit or Exchange

Trigger.dev has no password login, and its GitHub/Google login needs the internet, so in an offline
network the dashboard login is always an emailed magic link.

- **Default: Mailpit.** `docker-compose.mailpit.yml` (in `COMPOSE_FILE`) runs a local inbox at
  http://localhost:8025 and points the webapp's login and alert emails at it. To let other machines
  open the inbox, set `MAILPIT_PUBLISH_IP=0.0.0.0` in `.env`. Anyone with the inbox can read every
  login link, so keep it to a trusted team.
- **Later: Exchange (or any SMTP server).**
  1. In `webapp.env`, uncomment and fill in the "real mail server" block (host, port 587, service mailbox, and the `ALERT_*` copies).
     The block is already in `webapp.env.example`.
  2. If Exchange uses a certificate from an internal CA: copy the CA certificate into `certs/`, uncomment the `certs` volume in
     `docker-compose.local.yml`, and set `NODE_EXTRA_CA_CERTS=/certs/<file>.pem` in `webapp.env`.
  3. Remove `:docker-compose.mailpit.yml` from `COMPOSE_FILE` in `.env`, then run `docker compose up -d --remove-orphans`.

  While the Mailpit file is loaded it overrides `webapp.env`, so you can fill in the Exchange details ahead of time.

## Limitations and gotchas found

- Self-hosting has **no checkpoints, warm starts or autoscaling**. Each run is a fresh container. `wait.for` / `triggerAndWait` keep the container alive and holding resources instead of suspending it. Throughput is bounded by one Docker host unless you move to Kubernetes (Helm chart).
- **Deploys build images.** The deploy machine needs Docker with buildx. BuildKit runs in a `moby/buildkit` container, pulls digest-pinned `triggerdotdev/node` base images and runs `npm i` inside the build. This is the hard part for an offline network (see `../OFFLINE.md`).
- Parallel child runs must use `batch.triggerByTaskAndWait(...)`. `Promise.all` over `triggerAndWait` fails with `TASK_DID_CONCURRENT_WAIT`.
- The magic link must be opened in the same browser that requested it, unless `MAGIC_LINK_SAME_BROWSER_REQUIRED=false`. **Set `WHITELISTED_EMAILS`**, or anyone who can reach the port can sign up.
- The webapp always sends an SMTP auth object, even an empty one. A mail server that advertises AUTH then fails with `Missing credentials for "PLAIN"` unless `SMTP_USER`/`SMTP_PASSWORD` are set. For that reason Mailpit runs without SMTP auth.
- The webapp has no SMTP TLS options. A mail server certificate from an internal CA needs `NODE_EXTRA_CA_CERTS` (see `webapp.env.example`).
- On first start the supervisor may restart once while the webapp writes the worker token. That's harmless.
- Heavier: about 2 GB RAM idle, 11 containers. Upstream recommends 3+ vCPU / 6 GB for the webapp side and 4+ vCPU / 8 GB for workers.
- Upstream says the Docker guide "alone is unlikely to result in a production-ready deployment". You still need TLS, backups and monitoring.
