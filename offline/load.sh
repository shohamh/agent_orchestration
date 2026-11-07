#!/usr/bin/env bash
# Install the offline bundle on the OFFLINE side. Run from inside the copied bundle directory:
#
#   ./load.sh install [target-dir]    # verify, docker load, unpack source, start npm mirror, wheels
#   ./load.sh trigger-base-images     # AFTER the Trigger.dev stack is up: push deploy base images
#                                     # into its registry and write DEPLOY_*BASE_IMAGES to webapp.env
set -euo pipefail
export MSYS_NO_PATHCONV=1

BUNDLE=$(cd "$(dirname "$0")" && pwd)
TARGET_FILE="$BUNDLE/.target"
NPM_MIRROR_PORT=${NPM_MIRROR_PORT:-4873}
NPM_MIRROR_PROJECT=${NPM_MIRROR_PROJECT:-npm-mirror}
NPM_MIRROR_VOLUME=${NPM_MIRROR_VOLUME:-npm-mirror-storage}
WORKER_NODE_VERSION=v24.22.2
WORKER_NODE_LINUX_SHA256=48cc0e21e048790e54c0cf5749c2725005313d652651c40b7a21feb9697aaf75
WORKER_NODE_WINDOWS_SHA256=ca1bff5d8ead13c9f2ea404a176db46bd430704d5b5113734049fa365e7799e2

verify_worker_node_assets() {
  local linux_archive="worker-tools/node-$WORKER_NODE_VERSION-linux-x64.tar.xz"
  local windows_archive="worker-tools/node-$WORKER_NODE_VERSION-win-x64.zip"
  [ -s "$BUNDLE/$linux_archive" ] && [ -s "$BUNDLE/$windows_archive" ] || {
    echo "The bundle is missing the pinned portable Node.js $WORKER_NODE_VERSION archives" >&2
    exit 1
  }
  (cd "$BUNDLE" && printf '%s  %s\n' "$WORKER_NODE_LINUX_SHA256" "$linux_archive" |
    sha256sum -c --quiet -)
  (cd "$BUNDLE" && printf '%s  %s\n' "$WORKER_NODE_WINDOWS_SHA256" "$windows_archive" |
    sha256sum -c --quiet -)
}
log() { printf '\n==> %s\n' "$*"; }

install() {
  local target=${1:-$PWD/agent_orchestration}

  log "Verifying checksums"
  (cd "$BUNDLE" && sha256sum -c --quiet SHA256SUMS)
  verify_worker_node_assets

  log "Loading images"
  for tar in "$BUNDLE"/images/*.tar; do docker load -i "$tar"; done

  log "Unpacking source into $target"
  mkdir -p "$target"
  tar -C "$target" -xf "$BUNDLE/source.tar"
  echo "$target" >"$TARGET_FILE"
  touch "$target/trigger.dev/registry/auth.htpasswd" # generate-secrets.sh fills it
  # Preserve any existing live :local aliases. Use bundle-specific tags in this extracted Compose
  # checkout when a different local image already owns the shared tag.
  local name packaged compose_file packaged_id existing_id
  for name in agent-board agent-flow-host agent-flow-worker-hatchet agent-flow-worker-prefect; do
    packaged=$(grep -E "^${name}:offline-bundle-" "$BUNDLE/images/root.txt" | head -n1 || true)
    [ -n "$packaged" ] || { echo "Missing bundled root image tag for $name" >&2; exit 1; }
    packaged_id=$(docker image inspect "$packaged" --format '{{.Id}}')
    if [ "$name" = agent-board ]; then
      compose_file="$target/board/docker-compose.yml"
    else
      compose_file="$target/docker-compose.yml"
    fi
    existing_id=$(docker image inspect "$name:local" --format '{{.Id}}' 2>/dev/null || true)
    if [ -z "$existing_id" ]; then
      docker tag "$packaged" "$name:local"
    elif [ "$existing_id" != "$packaged_id" ]; then
      sed -i "s#${name}:local#${packaged}#g" "$compose_file"
      log "Keeping existing $name:local; extracted Compose uses $packaged"
    fi
  done
  log "Root Board/flow images are ready for Compose"
  for dir in hatchet/example-worker prefect/example-worker board; do
    mkdir -p "$target/$dir/wheelhouse"
    cp "$BUNDLE"/wheelhouse/*.whl "$target/$dir/wheelhouse/" 2>/dev/null || true
  done
  local asset node_tools
  mkdir -p "$target/dist/worker"
  for asset in agent-worker-x86_64-pc-windows-msvc.exe agent-worker-x86_64-unknown-linux-musl \
    confluence-runtime-windows.tar.gz confluence-runtime-linux.tar.gz; do
    [ -s "$BUNDLE/worker/$asset" ] || { echo "The bundle is missing worker asset $asset" >&2; exit 1; }
    cp -p "$BUNDLE/worker/$asset" "$target/dist/worker/$asset"
  done
  # Portable official Node archives are kept here for transfer to Windows/Linux worker PCs.
  node_tools="$target/dist/offline-bundle/worker-tools"
  mkdir -p "$node_tools"
  cp -p "$BUNDLE/worker-tools/node-$WORKER_NODE_VERSION-linux-x64.tar.xz" "$node_tools/"
  cp -p "$BUNDLE/worker-tools/node-$WORKER_NODE_VERSION-win-x64.zip" "$node_tools/"

  log "Starting the offline npm mirror (Verdaccio) on :$NPM_MIRROR_PORT"
  docker volume create "$NPM_MIRROR_VOLUME" >/dev/null
  docker run --rm -i -v "$NPM_MIRROR_VOLUME:/s" busybox:1.37 sh -c 'tar -C /s -xf - && chown -R 10001:65533 /s' \
    <"$BUNDLE/npm-storage.tar"
  (cd "$target/offline/npm-mirror" &&
    COMPOSE_PROJECT_NAME="$NPM_MIRROR_PROJECT" NPM_MIRROR_PROJECT="$NPM_MIRROR_PROJECT" \
      NPM_MIRROR_VOLUME="$NPM_MIRROR_VOLUME" NPM_MIRROR_PORT="$NPM_MIRROR_PORT" \
      MODE=offline docker compose up -d --no-build)

  cat <<EOF

Done. Next steps (details in $target/OFFLINE.md):
  cd $target/hatchet     && cp .env.example .env && docker compose up -d && ./scripts/create-token.sh
  cd $target/trigger.dev && cp .env.example .env && cat local.env.example >> .env && ./generate-secrets.sh
                            docker compose up -d
  $BUNDLE/load.sh trigger-base-images
  cd $target/prefect     && cp .env.example .env && docker compose up -d --no-build   # -> http://<this-host>:4200
  cd $target             && cp .env.example .env   # set FLOW_HOST_TOKEN and choose enabled engine profiles
                            docker compose up -d --no-build   # Admin/Fleet -> http://<this-host>:8090
  Portable Node $WORKER_NODE_VERSION archives: $target/dist/offline-bundle/worker-tools/
  Copy the matching archive to each worker PC, extract it, and add its Node directory (Linux bin/) to PATH.
  Before each Fleet install command, set npm_config_registry=http://<coordinator-host>:${NPM_MIRROR_PORT}/ in that shell.
EOF
}

trigger_base_images() {
  local target
  target=$(cat "$TARGET_FILE" 2>/dev/null || echo "$PWD/agent_orchestration")
  local tdir="$target/trigger.dev"
  set -a; . "$tdir/.env"; set +a
  local skopeo
  skopeo=$(grep -E '^quay.io/skopeo' "$BUNDLE/images/trigger.txt" | head -n1)
  local registry=${DOCKER_REGISTRY_URL:-localhost:5000}
  local skopeo_volume="offline-skopeo-${BASHPID:-$$}-$(date +%s)"

  log "Pushing deploy base images into $registry"
  docker run --rm -i -v "$skopeo_volume:/work" busybox:1.37 tar -C /work -xf - <"$BUNDLE/trigger-base-images.tar"

  local base="" build=""
  while read -r runtime env_var dir repo; do
    # Push over the compose network (service name `registry`); the digest is identical, so the
    # image can then be referenced as $registry/... by the BuildKit builder on the host network.
    docker run --rm --network webapp -v "$skopeo_volume:/work" "$skopeo" copy --quiet --preserve-digests \
      --dest-tls-verify=false --dest-creds "$DOCKER_REGISTRY_USERNAME:$DOCKER_REGISTRY_PASSWORD" \
      "dir:/work/$dir" "docker://registry:5000/$repo"
    digest=sha256:$(docker run --rm -v "$skopeo_volume:/work" busybox:1.37 sha256sum "/work/$dir/manifest.json" | cut -d' ' -f1)
    image="$registry/${repo%%:*}@$digest"
    echo "  $env_var $image"
    case "$env_var" in
      DEPLOY_BASE_IMAGES) base="${base:+$base,}$runtime=$image" ;;
      DEPLOY_BUILD_BASE_IMAGES) build="${build:+$build,}$runtime=$image" ;;
      BUILDKIT_SYNTAX) syntax="$image" ;;
    esac
  done <"$BUNDLE/trigger-base-images.txt"
  docker volume rm -f "$skopeo_volume" >/dev/null

  # Server side: the webapp tells the CLI which base images to build on.
  touch "$tdir/webapp.env"
  sed -i '/^DEPLOY_BASE_IMAGES=/d;/^DEPLOY_BUILD_BASE_IMAGES=/d' "$tdir/webapp.env"
  printf 'DEPLOY_BASE_IMAGES=%s\nDEPLOY_BUILD_BASE_IMAGES=%s\n' "$base" "$build" >>"$tdir/webapp.env"
  (cd "$tdir" && docker compose up -d webapp)

  # Client side: sourced by whoever runs `trigger deploy` (read by the build extension in trigger.config.ts).
  host=${registry%%:*}
  cat >"$tdir/offline-deploy.env" <<EOF
# source this before \`npx trigger.dev deploy --network host\` (replace localhost with the server name on other machines)
export NPM_MIRROR_URL=http://$host:${NPM_MIRROR_PORT:-4873}/
export BUILDKIT_SYNTAX_IMAGE=${syntax:-}
EOF
  log "webapp.env updated + webapp restarted; wrote $tdir/offline-deploy.env. See OFFLINE.md to deploy."
}

case "${1:-}" in
  install) shift; install "$@" ;;
  trigger-base-images) trigger_base_images ;;
  *) sed -n '2,8p' "$0"; exit 1 ;;
esac
