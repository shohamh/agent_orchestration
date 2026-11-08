# Moving the setups into an offline network

## What each tool needs at runtime vs. build time

| | Hatchet | Trigger.dev | Prefect |
|---|---|---|---|
| Server images | 5 (4 Hatchet + Postgres) | 11 (webapp, supervisor, Postgres, Redis, ClickHouse, Electric, MinIO, registry, s2, busybox, socket-proxy) | 2 (`prefecthq/prefect` + Postgres), plus the example worker image |
| Internet at **runtime** | None once the security check, analytics and Sentry are off (they are, here) | None once telemetry is off (it is, here) | None once analytics are off (`PREFECT_SERVER_ANALYTICS_ENABLED=false`, set here). The UI's block logos and docs links point online, so offline you'll see a few broken icons. The work-pool type list falls back to a copy inside the package. |
| Getting *your code* running | You build and run worker images yourself, like any other app | `trigger deploy` builds an image with BuildKit. That needs **(a)** the `moby/buildkit` image, **(b)** the digest-pinned `triggerdotdev/node` base images in a reachable registry, **(c)** the `docker/dockerfile:1` syntax frontend, which is normally pulled from Docker Hub, and **(d)** an npm registry, because `npm i` runs inside the build | Workers run your flows from local code, like Hatchet. The example worker image is rebuilt from the bundled wheels. |
| Traces | Built-in collector in the engine, stored in Postgres. Optional Jaeger image (`observability/`) is in the bundle. | Built-in, always on. Optional Jaeger, same as Hatchet. | No built-in viewer; export to the bundled Jaeger (`prefect/docker-compose.otel.yml`) |
| Developer packages | PyPI/npm mirror or a wheelhouse for `hatchet-sdk` and your deps | npm mirror for `trigger.dev`, `@trigger.dev/*` and your deps | Wheelhouse (or a PyPI mirror) for `prefect[otel]` and your deps |
| Offline difficulty | **Easy**: `docker save` / `docker load` and you're done | **Moderate**: images plus a base-image mirror plus an npm mirror plus a host-network builder. The scripts here automate all of it. | **Easy**: same shape as Hatchet |

## The plan

```
CONNECTED SIDE                                   OFFLINE SIDE (Linux + Docker 24+, Compose 2.24+)
./offline/bundle.sh  ──► dist/offline-bundle/ ──►  ./load.sh install
   docker save (images from compose config)         docker load, unpack source
   skopeo copy base images (digests kept)           Verdaccio (offline) with the cached npm packages
   Verdaccio pull-through cache of npm deps         wheels -> hatchet, prefect and board wheelhouses
   pip download Linux wheels (in a container)       [start stacks]
   source.tar (no secrets) + SHA256SUMS          ./load.sh trigger-base-images
                                                    push base images into the Trigger registry
                                                    write DEPLOY_BASE_IMAGES to webapp.env
                                                    write trigger.dev/offline-deploy.env
```

**The bundle follows the repository's explicit version pins and lockfiles.**

- Image references come from the Compose files and their environment settings.
- Trigger.dev deploy-image digests come from the installed Trigger CLI.
- npm packages come from the checked-in package manifests and lockfiles.
- The portable worker Node.js runtime is pinned to v24.22.2 and checksum-verified.

After changing an upstream version or dependency, re-run `bundle.sh`.

## Step by step

### Connected side

Start from a fresh clone on a connected machine. The bundle pulls/builds Docker images, fills the npm
mirror, downloads Linux Python wheels, and fetches official portable Node archives. It needs Git,
Bash (Git Bash or WSL), Docker Desktop/Compose with Linux containers, `curl`, `sha256sum`, and npm.
Install Trigger's locked CLI dependencies before bundling so the script can read its pinned deploy
image digests:

```bash
git clone <repository-url> agent-orchestration
cd agent-orchestration
```

Create the root `.env` before bundling:
```bash
cp .env.example .env
```
It defaults to `ENGINES=hatchet`, the recommended offline path. Set `ENGINES=prefect` for a
Prefect-only Board, or `ENGINES=hatchet,prefect` to include both SDKs. The Board image is prebuilt;
changing `ENGINES` on the offline host cannot add an SDK. Rebuild and rebundle if the required SDK
was not included.

On Windows, install Trigger's locked dependencies from PowerShell (`npm.cmd` is required; plain
`npm` may fail in Git Bash):

```powershell
cd agent-orchestration
npm.cmd ci --prefix trigger.dev/example-project
```

Then run the bundle builder from Git Bash at the repository root:

```bash
./offline/bundle.sh                           # writes dist/offline-bundle/
```

The bundle builder chooses a unique npm mirror project, volume, port and image tag by default, so
an existing mirror on the connected host is left untouched. Set explicit values only if needed:

```bash
NPM_MIRROR_PROJECT=agent-orchestration-bundle \
NPM_MIRROR_VOLUME=agent-orchestration-bundle-storage \
NPM_MIRROR_PORT=14873 ./offline/bundle.sh
```

The npm package archive is portable. On a fresh offline Docker host, use the loader defaults; if a
mirror already uses them, choose a different project, volume and free port when running `load.sh`.

The Trigger CLI supplies its pinned deploy-image digests. Container images and package dependencies
are obtained online; do not copy generated `.env` files or credentials to the offline host.
The bundler runs `trigger.dev/generate-secrets.sh` to fill missing local Compose secrets. Its `.env`
and generated registry password hash stay on the connected machine and are excluded from the bundle.

`OUT=/path/to/bundle` selects another output directory. The script caches ACP adapter packages for
both Windows x64 and Linux x64, plus Trigger's npm dependencies, Linux Python wheels, engine/app
images, source, worker binaries/runtime archives, and official Node.js v24.22.2 x64 archives. On a
Windows connected host, the Windows worker binary is built natively and needs Rust/cargo, rustup,
and the MSVC C++ build tools; Docker can build the Linux musl worker and the Confluence runtimes.
Alternatively, place all four worker/runtime artifacts in `dist/worker` or fetch them from a release
before bundling. The script fails with the exact missing artifacts rather than silently omitting them.
`PLATFORM_ARCH=arm64` selects ARM64 Linux engine/wheel assets; the Fleet binaries and bundled Node
archives remain x64. Inspect the final summary for transfer size; it varies with image versions.

Copy the entire `dist/offline-bundle/` directory to the offline network using approved media. The
offline loader verifies every file against `SHA256SUMS`.

### Offline side: Hatchet, Board, and a Fleet worker

The bundle includes the root Board/flow-host images, source and `dist/worker` assets. Use one
engine first; **Hatchet** is the recommended offline choice. The separate Hatchet server and the
root Board stack share `agent-net`.

The included worker binaries and portable Node.js archives support **Windows x64 and Linux x64**.
macOS is not packaged as an offline Fleet worker target; use Windows or Linux for this procedure.

This procedure installs the standalone Rust `agent-worker` for coding-agent sessions; it needs
outbound access only. The separate Python `flow_execution_worker.py` runs portable-flow steps on a
worker PC and uses a different token and inbound-network setup. Follow **Admin → Fleet setup** and
`docs/workers-and-context.md` for that path.

On Windows, run `load.sh` and `create-token.sh` from Git Bash with Docker Desktop in Linux-container mode.
The commands use `/opt/agent_orchestration` as a Linux path; replace it with `/c/agent_orchestration`
in Git Bash. `COMPOSE_FILE` uses `;` on Windows Docker Desktop and `:` on Linux/WSL.
The Windows worker EXE is signed with a free self-signed certificate; it does not gain SmartScreen
reputation or public trust automatically. See [worker release signing](worker/README.md) for details.

```bash
cd offline-bundle
./load.sh install /opt/agent_orchestration

# Start Hatchet on the offline server.
cd /opt/agent_orchestration/hatchet
cp .env.example .env
# Set HATCHET_HOST to this server's offline DNS name/IP and change HATCHET_ADMIN_PASSWORD
# before the first start. Set COMPOSE_FILE with the platform separator:
# Windows Docker Desktop: COMPOSE_FILE=docker-compose.yml;docker-compose.agents.yml
# Linux/WSL: COMPOSE_FILE=docker-compose.yml:docker-compose.agents.yml
docker compose up -d
./scripts/create-token.sh               # writes HATCHET_CLIENT_TOKEN into hatchet/.env
# Copy the token into board/.env as HATCHET_CLIENT_TOKEN and root .env as
# HATCHET_WORKER_CLIENT_TOKEN.

# Configure the root Board + flow-host stack.
cd /opt/agent_orchestration
cp .env.example .env
# Set FLOW_HOST_TOKEN to a random secret and keep only Hatchet enabled:
# COMPOSE_PROFILES=hatchet
# FLOW_BACKENDS=hatchet
# ENGINES=hatchet
# HATCHET_GRPC=hatchet-engine:7070
# HATCHET_URL=http://hatchet-dashboard:80
cp board/.env.example board/.env
# Set BOARD_PASSWORD and HATCHET_CLIENT_TOKEN in board/.env; set HATCHET_WORKER_CLIENT_TOKEN in root .env.
docker compose --profile hatchet up -d --no-build
```

The `hatchet` profile starts both `flow-worker-hatchet` for portable YAML flows and
`agent-session-worker-hatchet` for standalone `agent-session` cards. The session worker reuses the
portable worker image and the root repo mount; no additional image is required.

`load.sh install` verifies `SHA256SUMS`, loads the images, unpacks the source and worker assets,
and starts the local npm mirror on port 4873. The root Compose command uses only preloaded images;
`--no-build` prevents an accidental online build.

The loader defaults are project `npm-mirror`, volume `npm-mirror-storage` and port `4873`. If
those conflict on the offline host, use unique values instead of the default install command:

```bash
cd offline-bundle
NPM_MIRROR_PROJECT=agent-orchestration-offline \
NPM_MIRROR_VOLUME=agent-orchestration-offline-storage \
NPM_MIRROR_PORT=14873 ./load.sh install /opt/agent_orchestration
```

Open `http://<server>:8090`, sign in, then open **Admin**. For Hatchet, the API URL must be
reachable from the Board container (on `agent-net`, `http://hatchet-dashboard:80`); the dashboard
URL must be reachable from the browser (for example `http://<server>:8080`). Use **Check now**.
Credentials are in `board/.env`; Admin does not store credentials. Restart the Board after changing
Hatchet's API URL because its SDK endpoint is fixed at startup.

In **Fleet → Register PC**, choose Hatchet, the worker's OS, pool/tags and limits, and the server
address reachable from that PC (for example `http://<server>:8090`). The bundle includes pinned
official Node.js v24.22.2 archives at
`/opt/agent_orchestration/dist/offline-bundle/worker-tools/` on the offline host. Copy the matching
archive to the worker PC (for example by approved removable media). If it already has Node.js/npm,
skip extraction and make sure both are on the account's `PATH`.

For Linux x64, replace the archive path with the copy on the worker PC:

```bash
NODE_ARCHIVE=/mnt/transfer/node-v24.22.2-linux-x64.tar.xz
mkdir -p "$HOME/.local/lib/nodejs"
tar -xJf "$NODE_ARCHIVE" -C "$HOME/.local/lib/nodejs"
export PATH="$HOME/.local/lib/nodejs/node-v24.22.2-linux-x64/bin:$PATH"
node --version && npm --version
npm config set registry 'http://<server>:4873/'
```

For Windows x64, use PowerShell; set the copied archive path and add Node to the user `PATH` and
current shell:


```powershell
$archive = 'C:\transfer\node-v24.22.2-win-x64.zip'
$programs = Join-Path $env:LOCALAPPDATA 'Programs'
Expand-Archive -Path $archive -DestinationPath $programs
$node = Join-Path $programs 'node-v24.22.2-win-x64'
$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
[Environment]::SetEnvironmentVariable('Path', "$node;$userPath", 'User')
$env:Path = "$node;$env:Path"
node.exe --version
npm.cmd --version
npm.cmd config set registry 'http://<server>:4873/'
```

The mirror's default port is 4873; if `load.sh install` used another `NPM_MIRROR_PORT`, use that
port in the registry URL. Then run the generated Fleet command as a normal user. The Fleet installer
runs `npm ci` for the pinned ACP adapters, downloads the matching worker executable from the Board,
and connects outbound; no worker port needs to be opened. Autostart also requires Node on the PATH
for the worker's service account. The bundle's `dist/worker` binaries are what Fleet serves.

The Linux one-line install command uses `curl`; Windows uses PowerShell's built-in web request
command. Neither requires public Internet access when the Board and npm mirror are reachable.

For an offline transport smoke test, open the Hatchet tab and choose **New card → Coding agent
session → demo**. Under **More placement options**, set **Run on one specific worker** to the PC
you just registered, then launch. `demo` makes no model request; this checks engine scheduling,
relay dispatch, and agent execution without provider credentials. The install still needs Node.js/npm
and the cached adapter packages.

For a real coding agent, separately install that agent's CLI and its offline dependencies on the
worker, then configure the CLI's local or on-prem model endpoint and credentials there. The adapter
mirror does not include a model, CLI login, or provider credentials.

### Other engines

The bundle also carries Trigger.dev and Prefect server images. Choose an engine by aligning
`COMPOSE_PROFILES`, `FLOW_BACKENDS`, and `ENGINES` in the offline root `.env`, then configure its
server and credentials in its own `.env` and in `board/.env`. Use the engine's `docker-compose.agents.yml`
overlay to join `agent-net`. Do not start every engine by default; Trigger.dev has additional
registry/base-image setup documented below, while Prefect uses the simpler image-and-wheelhouse path.

`ENGINES` is baked into the Board image on the connected side. The offline root `.env` can select
only SDKs included in that image; if Prefect was not included, setting `ENGINES=prefect` here is not
enough. Rebuild and rebundle with the required SDKs.

```bash
# Prefect only (after setting its .env and root/board configuration):
cd /opt/agent_orchestration/prefect
cp .env.example .env
echo 'COMPOSE_FILE=docker-compose.yml:docker-compose.agents.yml' >> .env
docker compose up -d --no-build

# Then in /opt/agent_orchestration/.env:
# COMPOSE_PROFILES=prefect
# FLOW_BACKENDS=prefect
# ENGINES=prefect
# PREFECT_API_URL=http://prefect-server:4200/api
```

The `prefect` profile starts both `flow-worker-prefect` for portable YAML flows and
`agent-session-worker-prefect` for standalone `agent-session` cards. Both workers use the existing
Prefect flow-worker image and mounted repo; no additional image is required.

The examples below cover Trigger.dev deployment and native Prefect/Hatchet workers; they are not
needed for the Hatchet Fleet smoke path above.

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

### Prefect workers offline

- **Containers:** `prefect/example-worker/Dockerfile` installs from `wheelhouse/` with `--no-index` when wheels are present. `load.sh install` copies them in, so `docker compose build` also works offline.
- **Natively on a hardware PC:** use `pip install --no-index --find-links wheelhouse "prefect[otel]==<version>"`, then run `prefect worker start --pool hardware --work-queue <pc> --work-queue any-station --limit 1` with `PREFECT_API_URL=http://<server>:4200/api`.

### Hatchet workers offline

- **Containers:** build your worker images on the connected side and add them to the bundle, or rebuild them inside the offline network. The example `Dockerfile` installs from `wheelhouse/` with `--no-index` when wheels are present.
- **Bare Python:** use `pip install --no-index --find-links wheelhouse -r requirements.txt`.

## Things to decide or watch out for

- **CPU architecture.** The bundle contains images for the bundling machine's architecture. `PLATFORM_ARCH` (default `amd64`) selects the Trigger.dev base images and the Python wheels. Wheels are downloaded inside a Linux container, because pip on a Windows host would otherwise pull Windows-only dependencies. Bundle on the same architecture as the offline servers.
- **New dependencies need a new bundle.** Any new npm or pip dependency needs a bundle refresh or an internal mirror that is fed regularly. For ongoing development inside the offline network, a permanent Verdaccio (npm) and devpi or a simple index (pip) is the sustainable answer. `offline/npm-mirror` is already that for npm. You can also `npm publish` internal packages to it.
- **LLM access.** Agents inside the offline network need an on-prem model endpoint (vLLM, Ollama, TGI, ...). All three orchestrators just call it over HTTP from task code. None of them needs anything special for this.
- **Email.** Trigger.dev login links go to the bundled Mailpit inbox (`http://<server>:8025`, set `MAILPIT_PUBLISH_IP=0.0.0.0`). To use Exchange or another internal mail server instead, follow "Email: Mailpit or Exchange" in `trigger.dev/README.md`.
- **Windows Docker Desktop VMM mounts.** On this host, VMM rejected bind mounts from WSL home. A 1.4 MB source tree under `C:\Users\Shoham\workspace` stalled during container creation, while the same tree under `%LOCALAPPDATA%\Temp` mounted successfully. This is environment-specific; use a Windows-drive path Docker Desktop can access and validate Compose startup before disconnecting.
- **What was and wasn’t tested here.**
  - **Tested:**
    - `load.sh install` from a fresh clone into a fresh install target on this Windows/Docker Desktop host. `SHA256SUMS` validation passed; bundled images and runtime artifacts loaded.
    - `load.sh trigger-base-images`.
    - The offline Trigger.dev deploy path against the running stack on this host (2026-11-02).
    - Prefect: the worker image built without networking using bundled wheels; the stack ran on a Docker network without an internet route and its requirement checks passed.
    - The npm mirror's offline configuration has no uplinks, verified by running it with `--network none`.
    - The Admin page's Hatchet connectivity check returned `Reachable`; saving the Hatchet URLs returned HTTP 200.
    - Fleet installed the bundled Linux worker on an internal Docker network. It connected to the Board, reached the internal npm mirror (`npm ping` returned `PONG`), and an external HTTPS probe from the worker failed as expected.
    - A simulated demo task dispatched to that worker reached approval, question, answer, and a completed turn using the bundled Hatchet worker image. No model was called. **The Hatchet run then remained `RUNNING` and the Board remained in `Waiting`; Hatchet worker logs showed gRPC heartbeat `DEADLINE_EXCEEDED`. Final task completion is not verified.**
    - For local browser access, the test Board was temporarily attached to Docker's `bridge` network. The test worker, Hatchet services, and npm mirror used the internal network; this host was not fully air-gapped.
  - **Not tested:**
    - An uninterrupted successful `bundle.sh` run. The transfer bundle was assembled from verified stages after bundle attempts were interrupted; fresh online bundle creation is not verified end to end.
    - A physically separate offline PC/host. This test used containers on the same Windows/Docker Desktop machine.
    - A real model or provider CLI in the offline environment.
    - End-to-end task completion after the observed Hatchet heartbeat failure.

  Disconnecting a container network is not a substitute for testing on a separate offline host; do a dry run there before relying on this.
- **The connected-side mirror stops after bundling.** `bundle.sh` stops the online Verdaccio when it finishes. Start it with `MODE=offline docker compose up -d` in `offline/npm-mirror` if you want to keep using it locally.
