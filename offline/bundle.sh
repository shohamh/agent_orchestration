#!/usr/bin/env bash
# Build the offline transfer bundle. Run on the CONNECTED side from anywhere:
#
#   ./offline/bundle.sh                 # everything -> dist/offline-bundle/
#   OUT=/media/usb/bundle ./offline/bundle.sh
#
# Version-agnostic by design: image lists come from `docker compose config --images` and the
# Trigger.dev deploy base images are read out of the installed trigger.dev CLI, so after an
# upgrade (HATCHET_VERSION / scripts/update-upstream.sh / npm i trigger.dev@x) just re-run this.
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
NPM_PROJECTS=${NPM_PROJECTS:-trigger.dev/example-project}
PY_REQUIREMENTS=${PY_REQUIREMENTS:-hatchet/example-worker/requirements.txt board/requirements.txt board/requirements-hatchet.txt}
PLATFORM_ARCH=${PLATFORM_ARCH:-amd64}

SKOPEO_IMAGE=${SKOPEO_IMAGE:-quay.io/skopeo/stable:v1.20.0}
BUILDKIT_IMAGE=${BUILDKIT_IMAGE:-moby/buildkit:buildx-stable-1}
PY_IMAGE=${PY_IMAGE:-python:3.12-slim}  # downloads the Linux wheels (also the Hatchet worker's base image)
NPM_MIRROR_PORT=${NPM_MIRROR_PORT:-4873}

log() { printf '\n==> %s\n' "$*"; }
mkdir -p "$OUT/images" "$OUT/wheelhouse"

# Interpolated defaults like ${CLICKHOUSE_IMAGE_TAG:-26.2} need the project's .env to exist.
ensure_env() { [ -f "$1/.env" ] || cp "$1/.env.example" "$1/.env"; }
# Native path for non-POSIX tools (Windows Python under Git Bash); identity elsewhere.
native() { if command -v cygpath >/dev/null 2>&1; then cygpath -w "$1"; else echo "$1"; fi; }

# Scripts go into the bundle as they are in the working copy. One saved with Windows (CRLF) line
# endings fails on the Linux side ("bash\r: not found"), so stop before doing any work.
crlf_scripts=$(cd "$ROOT" && find hatchet trigger.dev prefect offline observability board \
  -name node_modules -prune -o -name '*.sh' -type f -print 2>/dev/null |
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
ensure_env "$ROOT/board"
(
  cd "$ROOT/board"
  ENGINES=${BOARD_ENGINES:-hatchet} docker compose build
  { docker compose config --images; sed -n 's/^FROM //p' Dockerfile; } | sort -u | tee "$OUT/images/board.txt"
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
fi

# ---------------------------------------------------------------- 3. Trigger.dev deploy base images
log "Trigger.dev deploy base images ($TRIGGER_RUNTIMES)"
cli_dist=$(ls -d "$ROOT"/trigger.dev/example-project/node_modules/trigger.dev/dist 2>/dev/null || true)
[ -n "$cli_dist" ] || { echo "Run npm install in trigger.dev/example-project first (need the CLI to read base image digests)"; exit 1; }
docker volume rm -f offline-skopeo >/dev/null 2>&1 || true
: >"$OUT/trigger-base-images.txt"
for rt in $TRIGGER_RUNTIMES; do
  ver=${rt#node-}
  for kind in "" "-build"; do
    ref=$(grep -rhoE "triggerdotdev/node:${ver}-bookworm${kind}@sha256:[a-f0-9]{64}" "$cli_dist" | sort -u | head -n1)
    [ -n "$ref" ] || { echo "No base image for $rt$kind in the installed CLI"; exit 1; }
    name="node-${ver}${kind}"
    echo "  $ref"
    docker run --rm -v offline-skopeo:/work "$SKOPEO_IMAGE" copy --quiet --preserve-digests \
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
docker run --rm -v offline-skopeo:/work "$SKOPEO_IMAGE" copy --quiet --preserve-digests \
  --override-os linux --override-arch "$PLATFORM_ARCH" "docker://docker.io/$syntax_ref" "dir:/work/dockerfile-frontend"
echo "- BUILDKIT_SYNTAX dockerfile-frontend $syntax_ref" >>"$OUT/trigger-base-images.txt"
docker run --rm -v offline-skopeo:/work "$SKOPEO_IMAGE" sh -c 'tar -C /work -cf - .' >"$OUT/trigger-base-images.tar" 2>/dev/null ||
  docker run --rm -v offline-skopeo:/work busybox tar -C /work -cf - . >"$OUT/trigger-base-images.tar"
docker volume rm -f offline-skopeo >/dev/null

# ---------------------------------------------------------------- 4. npm mirror
log "npm mirror (Verdaccio) for: $NPM_PROJECTS"
(
  cd "$ROOT/offline/npm-mirror"
  MODE=offline docker compose build --quiet           # image used on the offline side
  MODE=online docker compose up -d --build --wait 2>/dev/null || MODE=online docker compose up -d --build
  for _ in $(seq 1 30); do curl -sf "http://localhost:$NPM_MIRROR_PORT/-/ping" >/dev/null && break; sleep 2; done
)
for proj in $NPM_PROJECTS; do
  tmp=$(mktemp -d)
  cp "$ROOT/$proj/package.json" "$tmp/"
  [ -f "$ROOT/$proj/package-lock.json" ] && cp "$ROOT/$proj/package-lock.json" "$tmp/"
  # fresh cache + resolved URLs rewritten so every tarball is fetched *through* the mirror
  (cd "$tmp" && [ -f package-lock.json ] && sed -i "s#https://registry.npmjs.org/#http://localhost:$NPM_MIRROR_PORT/#g" package-lock.json || true)
  (cd "$tmp" && npm install --no-audit --no-fund --registry "http://localhost:$NPM_MIRROR_PORT/" --cache "$tmp/.npm-cache")
  rm -rf "$tmp"
done
(cd "$ROOT/offline/npm-mirror" && MODE=online docker compose stop)
docker run --rm -v npm-mirror-storage:/s busybox tar -C /s -cf - . >"$OUT/npm-storage.tar"
docker save npm-mirror:offline busybox:1.37 >"$OUT/images/npm-mirror.tar"

# ---------------------------------------------------------------- 5. Python wheels
log "Python wheels ($PY_REQUIREMENTS prefect/example-worker/requirements.txt)"
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

# ---------------------------------------------------------------- 6. Source + loader
log "Source (no secrets, no node_modules)"
tar -C "$ROOT" -cf "$OUT/source.tar" \
  --exclude=node_modules --exclude=dist --exclude=.trigger --exclude='*.pyc' \
  --exclude=.env --exclude=webapp.env --exclude=offline-deploy.env --exclude=.env.local --exclude=auth.htpasswd \
  --exclude='wheelhouse/*.whl' \
  hatchet trigger.dev prefect offline observability board $(cd "$ROOT" && ls *.md 2>/dev/null)
cp "$ROOT/offline/load.sh" "$OUT/"

(cd "$OUT" && find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum >SHA256SUMS)
log "Done: $OUT ($(du -sh "$OUT" | cut -f1)). Copy the whole directory to the offline network."
