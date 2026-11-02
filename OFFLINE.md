# Moving the setups into an offline network

## What each tool needs at runtime vs. build time

| | Hatchet | Trigger.dev |
|---|---|---|
| Server images | 5 (4 Hatchet + Postgres) | 11 (webapp, supervisor, Postgres, Redis, ClickHouse, Electric, MinIO, registry, s2, busybox, socket-proxy) |
| Internet at **runtime** | None once the security check, analytics and Sentry are off (they are, here) | None once telemetry is off (it is, here) |
| Getting *your code* running | You build and run worker images yourself, like any other app | `trigger deploy` builds an image with BuildKit. That needs **(a)** the `moby/buildkit` image, **(b)** the digest-pinned `triggerdotdev/node` base images in a reachable registry, **(c)** the `docker/dockerfile:1` syntax frontend, which is normally pulled from Docker Hub, and **(d)** an npm registry, because `npm i` runs inside the build |
| Traces | Built-in collector in the engine, stored in Postgres. Optional Jaeger image (`observability/`) is in the bundle. | Built-in, always on. Optional Jaeger, same as Hatchet. |
| Developer packages | PyPI/npm mirror or a wheelhouse for `hatchet-sdk` and your deps | npm mirror for `trigger.dev`, `@trigger.dev/*` and your deps |
| Offline difficulty | **Easy**: `docker save` / `docker load` and you're done | **Moderate**: images plus a base-image mirror plus an npm mirror plus a host-network builder. The scripts here automate all of it. |

## The plan

```
CONNECTED SIDE                                   OFFLINE SIDE (Linux + Docker 24+, Compose 2.24+)
./offline/bundle.sh  ──► dist/offline-bundle/ ──►  ./load.sh install
   docker save (images from compose config)         docker load, unpack source
   skopeo copy base images (digests kept)           Verdaccio (offline) with the cached npm packages
   Verdaccio pull-through cache of npm deps         wheels -> hatchet/example-worker/wheelhouse
   pip download wheels                              [start stacks]
   source.tar (no secrets) + SHA256SUMS          ./load.sh trigger-base-images
                                                    push base images into the Trigger registry
                                                    write DEPLOY_BASE_IMAGES to webapp.env
                                                    write trigger.dev/offline-deploy.env
```

**Nothing in the bundle is hard-coded to a version.**

- Image lists come from `docker compose config --images`.
- Trigger.dev base-image digests are read out of the installed `trigger.dev` CLI.
- npm packages come from installing your actual `package.json` through the mirror.

After an upgrade, re-run `bundle.sh`.

## Step by step

### Connected side

```bash
cd trigger.dev/example-project && npm install && cd ../..      # CLI must be installed (digests come from it)
./offline/bundle.sh                                              # -> dist/offline-bundle (~2.3 GB for these examples)
# options: TRIGGER_RUNTIMES="node-22 node-24"  NPM_PROJECTS="trigger.dev/example-project path/to/your-app"
#          PY_REQUIREMENTS="hatchet/example-worker/requirements.txt your/requirements.txt"  PLATFORM_ARCH=arm64
```

Copy `dist/offline-bundle/` across using whatever transfer process you have. `SHA256SUMS` is verified on arrival.

### Offline side

```bash
cd offline-bundle
./load.sh install /opt/agent_orchestration

# Hatchet
cd /opt/agent_orchestration/hatchet
cp .env.example .env    # set HATCHET_HOST=<server DNS/IP>
docker compose up -d && ./scripts/create-token.sh
docker compose --profile worker --profile hardware up -d   # worker images were in the bundle

# Optional shared trace backend
cd ../observability && docker compose up -d && cd ../hatchet

# Trigger.dev
cd ../trigger.dev
cp .env.example .env && cat local.env.example >> .env
#   in .env: APP_ORIGIN/LOGIN_ORIGIN/API_ORIGIN=http://<server>:8030, DOCKER_REGISTRY_URL=<server>:5000 (or keep localhost:5000 for single-host)
#   COMPOSE_FILE: drop docker-compose.no-bind-mounts.yml on Linux (optional)
./generate-secrets.sh                          # uses the httpd:2 image from the bundle
docker compose up -d
<bundle>/load.sh trigger-base-images           # registry push + DEPLOY_BASE_IMAGES + webapp restart

# Agent board (kanban over both systems)
cd ../board
cp .env.example .env    # HATCHET_CLIENT_TOKEN and/or TRIGGER_SECRET_KEY
docker compose up -d --no-build                # image is in the bundle; http://<server>:8090
```

### Deploying Trigger.dev tasks offline

```bash
npm config set registry http://<server>:4873/
cd your-project && npm install                 # served by Verdaccio
export TRIGGER_API_URL=http://<server>:8030 TRIGGER_ACCESS_TOKEN=tr_pat_...
. /opt/agent_orchestration/trigger.dev/offline-deploy.env   # NPM_MIRROR_URL + BUILDKIT_SYNTAX_IMAGE
docker login <server>:5000
npx trigger.dev deploy --skip-update-check --network host
```

- `--network host` puts the BuildKit builder and its `RUN` steps on the host network. That lets them reach the registry (for base images and the syntax frontend) and Verdaccio.
- The `offline` build extension in `example-project/trigger.config.ts` turns those two variables into build args:
  - `NPM_CONFIG_REGISTRY` for the in-image `npm i`.
  - `BUILDKIT_SYNTAX`, which replaces the Docker Hub `docker/dockerfile:1` frontend.

  Copy it into your own projects. It does nothing when the variables are unset.
- On a machine other than the server, replace `localhost` in `offline-deploy.env` with the server name.
- **Tested on 2026-11-02:** with the npm mirror in offline mode, which returns 404 for anything uncached, a deploy built only from `localhost:5000` images and mirror packages. All three example tasks then ran on that version. A negative test with a wrong mirror URL failed at `npm i` as expected.
- If `<server>:5000` is plain HTTP and not `localhost`, add it to `insecure-registries` on the Docker hosts. The cleaner fix is to put TLS on the registry.

### Hatchet workers offline

- **Containers:** build your worker images on the connected side and add them to the bundle, or rebuild them inside the offline network. The example `Dockerfile` installs from `wheelhouse/` with `--no-index` when wheels are present.
- **Bare Python:** use `pip install --no-index --find-links wheelhouse -r requirements.txt`.

## Things to decide or watch out for

- **CPU architecture.** The bundle contains images for the bundling machine's architecture plus `PLATFORM_ARCH` (default `amd64`) for the Trigger base images. Bundle on the same architecture as the offline servers.
- **New dependencies need a new bundle.** Any new npm or pip dependency needs a bundle refresh or an internal mirror that is fed regularly. For ongoing development inside the offline network, a permanent Verdaccio (npm) and devpi or a simple index (pip) is the sustainable answer. `offline/npm-mirror` is already that for npm. You can also `npm publish` internal packages to it.
- **LLM access.** Agents inside the offline network need an on-prem model endpoint (vLLM, Ollama, TGI, ...). Both orchestrators just call it over HTTP from task code. Neither needs anything special for this.
- **Email.** Trigger.dev login links go to the bundled Mailpit inbox (`http://<server>:8025`, set `MAILPIT_PUBLISH_IP=0.0.0.0`). To use Exchange or another internal mail server instead, follow "Email: Mailpit or Exchange" in `trigger.dev/README.md`.
- **What was and wasn't tested here.**
  - **Tested:**
    - `bundle.sh` end to end.
    - `load.sh trigger-base-images`.
    - The offline Trigger.dev deploy path, against the running stack on this Windows/Docker Desktop machine.
  - **Not tested:**
    - `load.sh install` on a fresh host.
    - Anything on a truly disconnected Linux machine. The machine here still had internet, so a forgotten dependency elsewhere would not have shown up.

  Do one dry run on a disconnected VM, for example with its network removed, before relying on this.
- **The connected-side mirror stops after bundling.** `bundle.sh` stops the online Verdaccio when it finishes. Start it with `MODE=offline docker compose up -d` in `offline/npm-mirror` if you want to keep using it locally.
