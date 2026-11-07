#!/usr/bin/env bash
# Build the offline transfer bundle. Run on the CONNECTED side from anywhere:
#
#   ./offline/bundle.sh                 # everything -> dist/offline-bundle/
#   OUT=/media/usb/bundle ./offline/bundle.sh
#
# Image references and dependencies follow repository Compose settings and lockfiles. The bundled
# worker Node.js version is pinned and checksum-verified below; re-run after changing version pins.
#
# Output:
#   images/*.tar             docker save archives (load with `docker load`)
#   trigger-base-images.tar  triggerdotdev/node base images for `trigger deploy` (OCI dirs, digests preserved)
#   npm-storage.tar          Verdaccio storage with every npm package the JS projects need
#   wheelhouse/              Python wheels for the Hatchet and Prefect example workers (linux x86_64, py3.12)
#   source.tar               this repo without secrets / node_modules
#   load.sh, SHA256SUMS
set -euo pipefail
export MSYS_NO_PATHCONV=1 # Git Bash on Windows: don't rewrite /paths in docker args

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT=${OUT:-$ROOT/dist/offline-bundle}
# Which Trigger.dev deploy runtimes to mirror (node-22, node-24, node-26). `runtime: "node"` = node-24.
TRIGGER_RUNTIMES=${TRIGGER_RUNTIMES:-node-24}
# JS projects whose dependencies must be installable offline (space separated, relative to ROOT).
NPM_PROJECTS=${NPM_PROJECTS:-agents trigger.dev/example-project}
PY_REQUIREMENTS=${PY_REQUIREMENTS:-hatchet/example-worker/requirements.txt board/requirements.txt board/requirements-hatchet.txt}
PLATFORM_ARCH=${PLATFORM_ARCH:-amd64}

SKOPEO_IMAGE=${SKOPEO_IMAGE:-quay.io/skopeo/stable:v1.20.0}
BUILDKIT_IMAGE=${BUILDKIT_IMAGE:-moby/buildkit:buildx-stable-1}
PY_IMAGE=${PY_IMAGE:-python:3.12-slim}  # downloads Linux wheels and builds cross-platform Confluence runtimes
WORKER_BUILD_IMAGE=${WORKER_BUILD_IMAGE:-rust:alpine}
NPM_MIRROR_PORT=${NPM_MIRROR_PORT:-4873}
NPM_MIRROR_PROJECT=${NPM_MIRROR_PROJECT:-npm-mirror}
NPM_MIRROR_VOLUME=${NPM_MIRROR_VOLUME:-npm-mirror-storage}
BUNDLE_IMAGE_SUFFIX="offline-bundle-${BASHPID:-$$}-$(date +%s)"
SKOPEO_VOLUME="offline-skopeo-${BASHPID:-$$}-$(date +%s)"
WORKER_NODE_VERSION=v24.22.2
WORKER_NODE_LINUX_SHA256=48cc0e21e048790e54c0cf5749c2725005313d652651c40b7a21feb9697aaf75
WORKER_NODE_WINDOWS_SHA256=ca1bff5d8ead13c9f2ea404a176db46bd430704d5b5113734049fa365e7799e2
if [ -z "${NPM_BIN:-}" ]; then
  NPM_BIN=npm
  case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) command -v npm.cmd >/dev/null 2>&1 && NPM_BIN=npm.cmd ;;
  esac
fi

log() { printf '\n==> %s\n' "$*"; }
mkdir -p "$OUT/images" "$OUT/wheelhouse"
OUT=$(cd "$OUT" && pwd)

# Interpolated defaults like ${CLICKHOUSE_IMAGE_TAG:-26.2} need the project's .env to exist.
ensure_env() { [ -f "$1/.env" ] || cp "$1/.env.example" "$1/.env"; }
# Native path for non-POSIX tools (Windows Python under Git Bash); identity elsewhere.
native() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else echo "$1"; fi; }
bundle_compose_tmp=$(mktemp -d)
trap 'rm -rf "$bundle_compose_tmp"' EXIT
package_worker_assets() (
  set -euo pipefail
  local stage root_native stage_native host powershell python_cmd asset missing=()
  stage=$(mktemp -d)
  trap 'rm -rf "$stage"' EXIT

  local -a assets=(
    agent-worker-x86_64-pc-windows-msvc.exe
    agent-worker-x86_64-unknown-linux-musl
    confluence-runtime-windows.tar.gz
    confluence-runtime-linux.tar.gz
  )
  mkdir -p "$OUT/worker"
  for asset in "${assets[@]}"; do
    if [ -s "$ROOT/dist/worker/$asset" ]; then
      cp -p "$ROOT/dist/worker/$asset" "$stage/$asset"
    fi
  done
  root_native=$(native "$ROOT")
  stage_native=$(native "$stage")
  host=$(uname -s)

  asset=agent-worker-x86_64-pc-windows-msvc.exe
  if [ ! -s "$stage/$asset" ]; then
    powershell=
    for candidate in powershell.exe powershell pwsh.exe pwsh; do
      if command -v "$candidate" >/dev/null 2>&1; then powershell=$candidate; break; fi
    done
    case "$host" in
      MINGW*|MSYS*|CYGWIN*)
        if [ -n "$powershell" ] && command -v cargo >/dev/null 2>&1 && command -v rustup >/dev/null 2>&1; then
          log "Building the Windows worker into a temporary bundle staging directory"
          if ! OUT="$stage_native" "$powershell" -NoProfile -ExecutionPolicy Bypass \
            -File "$(native "$ROOT/worker/ci/build.ps1")"; then
            echo "Windows worker build failed; provide the Windows binary or install the Rust/MSVC prerequisites." >&2
          fi
        fi
        ;;
    esac
  fi

  asset=agent-worker-x86_64-unknown-linux-musl
  if [ ! -s "$stage/$asset" ]; then
    if [ "$host" = Linux ] && command -v cargo >/dev/null 2>&1 && command -v rustc >/dev/null 2>&1; then
      if ! OUT="$stage" "$ROOT/worker/ci/build.sh"; then
        echo "Linux worker build failed; provide the Linux binary or make Rust/cargo available." >&2
      fi
    elif command -v docker >/dev/null 2>&1; then
      log "Building the Linux worker in $WORKER_BUILD_IMAGE into temporary bundle staging"
      if ! docker run --rm --platform linux/amd64 -v "$root_native:/repo" -v "$stage_native:/out" \
        -w /repo/worker "$WORKER_BUILD_IMAGE" sh -c 'OUT=/out ./ci/build.sh'; then
        echo "Linux worker container build failed; provide the Linux binary or check Docker/build prerequisites." >&2
      fi
    fi
  fi

  for asset in confluence-runtime-windows.tar.gz confluence-runtime-linux.tar.gz; do
    if [ ! -s "$stage/$asset" ]; then
      if command -v docker >/dev/null 2>&1; then
        log "Building $asset in $PY_IMAGE into temporary bundle staging"
        os=${asset#confluence-runtime-}
        os=${os%.tar.gz}
        if ! docker run --rm -v "$root_native:/repo:ro" -v "$stage_native:/out" -w /repo \
          "$PY_IMAGE" python worker/ci/build-confluence-runtime.py --os "$os" --out /out; then
          echo "Could not build $asset in $PY_IMAGE; provide the archive or check online access." >&2
        fi
      else
        python_cmd=
        for candidate in python3.12 python3 python; do
          if command -v "$candidate" >/dev/null 2>&1 &&
             "$candidate" -c 'import sys; raise SystemExit(sys.version_info < (3, 12))' 2>/dev/null; then
            python_cmd=$candidate
            break
          fi
        done
        if [ -n "$python_cmd" ]; then
          log "Building $asset with $python_cmd into temporary bundle staging"
          os=${asset#confluence-runtime-}
          os=${os%.tar.gz}
          if ! "$python_cmd" "$ROOT/worker/ci/build-confluence-runtime.py" --os "$os" --out "$stage"; then
            echo "Could not build $asset with $python_cmd; provide the archive or check pip/network access." >&2
          fi
        fi
      fi
    fi
  done

  for asset in "${assets[@]}"; do
    if [ -s "$stage/$asset" ]; then
      cp -p "$stage/$asset" "$OUT/worker/$asset"
    else
      missing+=("$asset")
    fi
  done
  if [ "${#missing[@]}" -gt 0 ]; then
    echo "Required Fleet worker assets are missing:" >&2
    printf '  %s\n' "${missing[@]}" >&2
    cat >&2 <<EOF
Provide all four files in $ROOT/dist/worker and rerun, or build/fetch them online first:
  Windows binary: worker/ci/build.ps1 on Windows with Rust/cargo, rustup, and the MSVC C++ build tools.
  Linux binary: worker/ci/build.sh on Linux with Rust/cargo, or rerun with Docker available (uses $WORKER_BUILD_IMAGE).
  Confluence runtimes: worker/ci/build-confluence-runtime.py --os windows and --os linux with Python 3.12, pip, and network access; Docker with $PY_IMAGE can build them automatically.
  A published release can provide all assets via worker/ci/fetch-release.sh github <owner/repo> worker-v<version>.
These build/fetch steps must run on the connected side; this bundler never writes to dist/worker.
EOF
    exit 1
  fi
  log "Packaged Windows/Linux worker binaries and Confluence runtimes"
)

download_worker_node() {
  local archive=$1 expected=$2 target="$OUT/worker-tools/$1" temporary curl_target
  mkdir -p "$OUT/worker-tools"
  if [ -e "$target" ]; then
    printf '%s  %s\n' "$expected" "$target" | sha256sum -c --quiet - ||
      { echo "Packaged Node.js archive failed its pinned $WORKER_NODE_VERSION checksum: $target" >&2; exit 1; }
    return
  fi
  temporary=$(mktemp "$OUT/worker-tools/$archive.part.XXXXXX")
  curl_target=$(native "$temporary")
  if ! curl -fsSL "https://nodejs.org/dist/$WORKER_NODE_VERSION/$archive" -o "$curl_target"; then
    rm -f "$temporary"
    echo "Could not download $archive from the official Node.js distribution site" >&2
    exit 1
  fi
  if ! printf '%s  %s\n' "$expected" "$temporary" | sha256sum -c --quiet -; then
    rm -f "$temporary"
    echo "Official Node.js archive checksum mismatch for $archive" >&2
    exit 1
  fi
  mv "$temporary" "$target"
}

package_worker_node() {
  log "Fetching pinned official Node.js $WORKER_NODE_VERSION portable runtimes"
  download_worker_node "node-$WORKER_NODE_VERSION-linux-x64.tar.xz" "$WORKER_NODE_LINUX_SHA256"
  download_worker_node "node-$WORKER_NODE_VERSION-win-x64.zip" "$WORKER_NODE_WINDOWS_SHA256"
}

# Scripts go into the bundle as they are in the working copy. One saved with Windows (CRLF) line
# endings fails on the Linux side ("bash\r: not found"), so stop before doing any work.
crlf_scripts=$(cd "$ROOT" && find hatchet trigger.dev prefect offline observability board agents worker \
  \( -name node_modules -o -name target -o -name .venv \) -prune -o -name '*.sh' -type f -print 2>/dev/null |
  while read -r script; do [ "$(tr -cd '\r' <"$script" | wc -c)" -eq 0 ] || echo "$script"; done || true)
if [ -n "$crlf_scripts" ]; then
  echo "These scripts have Windows (CRLF) line endings and would not run on the offline Linux servers:" >&2
  echo "$crlf_scripts" | sed 's/^/  /' >&2
  echo "Convert them to LF, e.g.: (cd \"$ROOT\" && sed -i 's/\r\$//' $(echo $crlf_scripts))" >&2
  exit 1
fi

# ---------------------------------------------------------------- 1. Hatchet images
if [ "${SKIP_IMAGES:-0}" != 1 ]; then # SKIP_IMAGES=1 reuses images/*.tar from a previous run
log "Hatchet images"
ensure_env "$ROOT/hatchet"
(
  cd "$ROOT/hatchet"
  docker compose --profile worker --profile hardware pull --ignore-buildable --quiet
  docker compose --profile worker --profile hardware build
  images=$(docker compose --profile worker --profile hardware config --images)
  # base of the example worker, so workers can be rebuilt offline
  images="$images $(sed -n 's/^FROM //p' example-worker/Dockerfile)"
  docker pull --quiet "$(sed -n 's/^FROM //p' example-worker/Dockerfile)" >/dev/null
  echo "$images" | tr ' ' '\n' | sort -u | tee "$OUT/images/hatchet.txt"
  docker save $(cat "$OUT/images/hatchet.txt") >"$OUT/images/hatchet.tar"
)

# ---------------------------------------------------------------- 2. Trigger.dev images
log "Trigger.dev images"
(
  cd "$ROOT/trigger.dev"
  [ -f .env ] || { cp .env.example .env && cat local.env.example >>.env; }
  set -a; . ./.env; set +a
  docker compose pull --ignore-buildable --quiet
  docker compose build
  {
    docker compose config --images
    # bases of the derived images (docker-compose.no-bind-mounts.yml), for offline rebuilds
    echo "clickhouse/clickhouse-server:${CLICKHOUSE_IMAGE_TAG:-26.2}"
    echo "registry:${REGISTRY_IMAGE_TAG:-2}"
    echo "httpd:2"            # generate-secrets.sh hashes the registry password with it
    echo "axllent/mailpit:${MAILPIT_IMAGE_TAG:-v1.27.10}"  # even when docker-compose.mailpit.yml is switched off
    echo "$BUILDKIT_IMAGE"    # `trigger deploy` creates a docker-container buildx builder
    echo "$SKOPEO_IMAGE"      # load.sh pushes the deploy base images into the local registry
  } | sort -u >"$OUT/images/trigger.txt"
  grep -vE '^trigger-(clickhouse|registry):' "$OUT/images/trigger.txt" | while read -r img; do
    docker image inspect "$img" >/dev/null 2>&1 || docker pull --quiet "$img" >/dev/null
  done
  cat "$OUT/images/trigger.txt"
  docker save $(cat "$OUT/images/trigger.txt") >"$OUT/images/trigger.tar"
)

# ---------------------------------------------------------------- 2b. Shared trace backend (optional)
log "Observability images"
(
  cd "$ROOT/observability"
  docker compose pull --quiet
  docker compose config --images | tee "$OUT/images/observability.txt"
  docker save $(cat "$OUT/images/observability.txt") >"$OUT/images/observability.tar"
)

# ---------------------------------------------------------------- 2c. Agent board images
log "Agent board images"
ensure_env "$ROOT"
ensure_env "$ROOT/board"
root_engines=$(sed -n 's/^ENGINES=//p' "$ROOT/.env" | tail -n1)
board_engines=${BOARD_ENGINES:-$root_engines}
(
  cd "$ROOT/board"
  docker build --build-arg "ENGINES=$board_engines" --tag "agent-board:$BUNDLE_IMAGE_SUFFIX" .
  { echo "agent-board:$BUNDLE_IMAGE_SUFFIX"; sed -n 's/^FROM //p' Dockerfile; } |
    sort -u | tee "$OUT/images/board.txt"
  docker save $(cat "$OUT/images/board.txt") >"$OUT/images/board.tar"
)

# ---------------------------------------------------------------- 2d. Prefect images
log "Prefect images"
ensure_env "$ROOT/prefect"
(
  cd "$ROOT/prefect"
  set -a; . ./.env; set +a
  docker compose pull --ignore-buildable --quiet
  docker compose build
  {
    docker compose config --images
    # base of the example worker (FROM uses the PREFECT_VERSION build arg), for offline rebuilds
    echo "prefecthq/prefect:${PREFECT_VERSION:-3.9.1}-python3.12"
  } | sort -u | tee "$OUT/images/prefect.txt"
  docker save $(cat "$OUT/images/prefect.txt") >"$OUT/images/prefect.tar"
)

# ---------------------------------------------------------------- 2e. Root board and flow-host images
log "Root board and flow-host images"
root_override="$bundle_compose_tmp/root-compose.override.yml"
cat >"$root_override" <<EOF
services:
  flow-host:
    image: agent-flow-host:$BUNDLE_IMAGE_SUFFIX
  flow-worker-hatchet:
    image: agent-flow-worker-hatchet:$BUNDLE_IMAGE_SUFFIX
  flow-worker-prefect:
    image: agent-flow-worker-prefect:$BUNDLE_IMAGE_SUFFIX
EOF
(
  cd "$ROOT"
  bundle_flow_token=${FLOW_HOST_TOKEN:-offline-bundle-build-only}
  FLOW_HOST_TOKEN="$bundle_flow_token" docker compose -f docker-compose.yml -f "$(native "$root_override")" \
    --profile hatchet --profile prefect build flow-host flow-worker-hatchet flow-worker-prefect
  {
    echo "agent-board:$BUNDLE_IMAGE_SUFFIX"
    FLOW_HOST_TOKEN="$bundle_flow_token" docker compose -f docker-compose.yml -f "$(native "$root_override")" \
      --profile hatchet --profile prefect config --images | grep -v '^agent-board:local$'
  } | sort -u | tee "$OUT/images/root.txt"
  docker save $(cat "$OUT/images/root.txt") >"$OUT/images/root.tar"
)
fi
if [ ! -s "$OUT/images/root.tar" ] || [ ! -s "$OUT/images/root.txt" ]; then
  echo "Root Board/flow images are missing. Run without SKIP_IMAGES=1 to build them." >&2
  exit 1
fi

# ---------------------------------------------------------------- 3. Trigger.dev deploy base images
log "Trigger.dev deploy base images ($TRIGGER_RUNTIMES)"
cli_dist=$(ls -d "$ROOT"/trigger.dev/example-project/node_modules/trigger.dev/dist 2>/dev/null || true)
[ -n "$cli_dist" ] || { echo "Run npm install in trigger.dev/example-project first (need the CLI to read base image digests)"; exit 1; }
: >"$OUT/trigger-base-images.txt"
for rt in $TRIGGER_RUNTIMES; do
  ver=${rt#node-}
  for kind in "" "-build"; do
    ref=$(grep -rhoE "triggerdotdev/node:${ver}-bookworm${kind}@sha256:[a-f0-9]{64}" "$cli_dist" | sort -u | head -n1)
    [ -n "$ref" ] || { echo "No base image for $rt$kind in the installed CLI"; exit 1; }
    name="node-${ver}${kind}"
    echo "  $ref"
    docker run --rm -v "$SKOPEO_VOLUME:/work" "$SKOPEO_IMAGE" copy --quiet --preserve-digests \
      --override-os linux --override-arch "$PLATFORM_ARCH" "docker://docker.io/${ref%%:*}@${ref##*@}" "dir:/work/$name"
    # columns: runtime  env-var  dir  repo:tag
    env_var=DEPLOY_BASE_IMAGES; [ -n "$kind" ] && env_var=DEPLOY_BUILD_BASE_IMAGES
    echo "$rt $env_var $name ${ref%@*}" >>"$OUT/trigger-base-images.txt"
  done
done
# The generated Containerfile starts with `# syntax=docker/dockerfile:1`, which BuildKit fetches
# from Docker Hub. Mirror it too; builds are pointed at it via the BUILDKIT_SYNTAX build arg.
syntax_ref=$(grep -rhoE "syntax=docker/dockerfile:[0-9.]+" "$cli_dist" | sort -u | head -n1 | cut -d= -f2)
syntax_ref=${syntax_ref:-docker/dockerfile:1}
echo "  $syntax_ref"
docker run --rm -v "$SKOPEO_VOLUME:/work" "$SKOPEO_IMAGE" copy --quiet --preserve-digests \
  --override-os linux --override-arch "$PLATFORM_ARCH" "docker://docker.io/$syntax_ref" "dir:/work/dockerfile-frontend"
echo "- BUILDKIT_SYNTAX dockerfile-frontend $syntax_ref" >>"$OUT/trigger-base-images.txt"
docker run --rm -v "$SKOPEO_VOLUME:/work" "$SKOPEO_IMAGE" sh -c 'tar -C /work -cf - .' >"$OUT/trigger-base-images.tar" 2>/dev/null ||
  docker run --rm -v "$SKOPEO_VOLUME:/work" busybox tar -C /work -cf - . >"$OUT/trigger-base-images.tar"
docker volume rm -f "$SKOPEO_VOLUME" >/dev/null

# ---------------------------------------------------------------- 4. npm mirror
log "npm mirror (Verdaccio) for: $NPM_PROJECTS"
(
  cd "$ROOT/offline/npm-mirror"
  COMPOSE_PROJECT_NAME="$NPM_MIRROR_PROJECT" NPM_MIRROR_PROJECT="$NPM_MIRROR_PROJECT" \
    NPM_MIRROR_VOLUME="$NPM_MIRROR_VOLUME" NPM_MIRROR_PORT="$NPM_MIRROR_PORT" MODE=offline docker compose build --quiet
  COMPOSE_PROJECT_NAME="$NPM_MIRROR_PROJECT" NPM_MIRROR_PROJECT="$NPM_MIRROR_PROJECT" \
    NPM_MIRROR_VOLUME="$NPM_MIRROR_VOLUME" NPM_MIRROR_PORT="$NPM_MIRROR_PORT" MODE=online docker compose up -d --build --wait 2>/dev/null ||
    COMPOSE_PROJECT_NAME="$NPM_MIRROR_PROJECT" NPM_MIRROR_PROJECT="$NPM_MIRROR_PROJECT" \
      NPM_MIRROR_VOLUME="$NPM_MIRROR_VOLUME" NPM_MIRROR_PORT="$NPM_MIRROR_PORT" MODE=online docker compose up -d --build
)
run_npm_project() {
  local proj=$1 target_os=$2 phase=$3 tmp cache
  local -a npm_args
  tmp=$(mktemp -d)
  cp "$ROOT/$proj/package.json" "$tmp/"
  [ -f "$ROOT/$proj/package-lock.json" ] && cp "$ROOT/$proj/package-lock.json" "$tmp/"
  if [ -f "$tmp/package-lock.json" ]; then
    (cd "$tmp" && sed -i "s#https://registry.npmjs.org/#http://localhost:$NPM_MIRROR_PORT/#g" package-lock.json)
  fi
  if [ "$proj" = agents ] || [ -n "$target_os" ]; then
    npm_args=(ci --no-audit --no-fund --ignore-scripts)
  else
    npm_args=(install --no-audit --no-fund)
    [ "$phase" != verify ] || npm_args+=(--ignore-scripts)
  fi
  if [ -n "$target_os" ]; then npm_args+=(--os="$target_os" --cpu=x64); fi
  cache=$(native "$tmp/.npm-cache")
  log "$phase npm packages: $proj${target_os:+ ($target_os x64)}"
  (cd "$tmp" && "$NPM_BIN" "${npm_args[@]}" \
    --registry "http://localhost:$NPM_MIRROR_PORT/" --cache "$cache")
  rm -rf "$tmp"
}

for proj in $NPM_PROJECTS; do
  if [ "$proj" = agents ]; then
    run_npm_project "$proj" win32 warm
    run_npm_project "$proj" linux warm
  else
    run_npm_project "$proj" "" warm
  fi
done

(
  cd "$ROOT/offline/npm-mirror"
  COMPOSE_PROJECT_NAME="$NPM_MIRROR_PROJECT" NPM_MIRROR_PROJECT="$NPM_MIRROR_PROJECT" \
    NPM_MIRROR_VOLUME="$NPM_MIRROR_VOLUME" NPM_MIRROR_PORT="$NPM_MIRROR_PORT" MODE=online docker compose stop
  COMPOSE_PROJECT_NAME="$NPM_MIRROR_PROJECT" NPM_MIRROR_PROJECT="$NPM_MIRROR_PROJECT" \
    NPM_MIRROR_VOLUME="$NPM_MIRROR_VOLUME" NPM_MIRROR_PORT="$NPM_MIRROR_PORT" MODE=offline docker compose up -d --no-build --wait
)
log "Verifying npm cache without upstream access"
for proj in $NPM_PROJECTS; do
  if [ "$proj" = agents ]; then
    run_npm_project "$proj" win32 verify
    run_npm_project "$proj" linux verify
  else
    run_npm_project "$proj" "" verify
  fi
done
(
    COMPOSE_PROJECT_NAME="$NPM_MIRROR_PROJECT" NPM_MIRROR_PROJECT="$NPM_MIRROR_PROJECT" \
      NPM_MIRROR_VOLUME="$NPM_MIRROR_VOLUME" NPM_MIRROR_PORT="$NPM_MIRROR_PORT" MODE=offline docker compose stop
)
docker save npm-mirror:offline busybox:1.37 >"$OUT/images/npm-mirror.tar"
docker run --rm -v "$NPM_MIRROR_VOLUME:/s" busybox tar -C /s -cf - . >"$OUT/npm-storage.tar"
# Downloaded inside a Linux container: pip evaluates environment markers against the machine it
# runs on even with --platform, so on a Windows bundling host it would demand Windows-only deps
# (e.g. Prefect -> docker -> pywin32). The container is exactly the target platform.
# Prefect's requirements leave the version to PREFECT_VERSION in prefect/.env; pin it with a constraint.
prefect_version=$(sed -n 's/^PREFECT_VERSION=//p' "$ROOT/prefect/.env" | tail -n1)
wheel_cid=$(docker create --platform "linux/$PLATFORM_ARCH" -e "PREFECT_PIN=prefect==${prefect_version:-3.9.1}" "$PY_IMAGE" sh -c '
  set -e; mkdir -p /wh; echo "$PREFECT_PIN" >/tmp/constraints.txt
  for r in /tmp/req-*.txt; do pip download --quiet --disable-pip-version-check -c /tmp/constraints.txt -d /wh -r "$r"; done')
i=0
for req in $PY_REQUIREMENTS prefect/example-worker/requirements.txt; do
  i=$((i + 1))
  docker cp "$(native "$ROOT/$req")" "$wheel_cid:/tmp/req-$i.txt"
done
docker start -a "$wheel_cid"
docker cp "$wheel_cid:/wh/." "$(native "$OUT/wheelhouse")"
docker rm "$wheel_cid" >/dev/null
package_worker_assets
package_worker_node

# ---------------------------------------------------------------- 6. Source + loader
log "Source (no secrets, no node_modules)"
tar -C "$ROOT" -cf "$OUT/source.tar" \
  --exclude=node_modules --exclude=dist --exclude=target --exclude=.venv --exclude=__pycache__ \
  --exclude=.flow-state --exclude=.workers.lock --exclude=.trigger --exclude='*.pyc' \
  --exclude='.env*' --exclude='*/.env*' --exclude='*.env' --exclude='*.env.*' \
  --exclude=auth.htpasswd --exclude='wheelhouse/*.whl' \
  hatchet trigger.dev prefect offline observability board agents flows worker docs docker-compose.yml \
  $(cd "$ROOT" && ls *.md 2>/dev/null)
(
  cd "$ROOT"
  {
    find . -maxdepth 1 -type f -name '.env.example' -print0
    find hatchet trigger.dev prefect offline observability board agents worker \
      -type d \( -name node_modules -o -name target -o -name .venv \) -prune -o \
      -type f -name '.env.example' -print0
  } | tar --null --append --file="$OUT/source.tar" --files-from=-
)

(cd "$OUT" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum >SHA256SUMS)
log "Done: $OUT ($(du -sh "$OUT" | cut -f1)). Copy the whole directory to the offline network."
