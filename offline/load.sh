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
log() { printf '\n==> %s\n' "$*"; }

install() {
  local target=${1:-$PWD/agent_orchestration}

  log "Verifying checksums"
  (cd "$BUNDLE" && sha256sum -c --quiet SHA256SUMS)

  log "Loading images"
  for tar in "$BUNDLE"/images/*.tar; do docker load -i "$tar"; done

  log "Unpacking source into $target"
  mkdir -p "$target"
  tar -C "$target" -xf "$BUNDLE/source.tar"
  echo "$target" >"$TARGET_FILE"
  touch "$target/trigger.dev/registry/auth.htpasswd" # generate-secrets.sh fills it
  for dir in hatchet/example-worker board; do
    mkdir -p "$target/$dir/wheelhouse"
    cp "$BUNDLE"/wheelhouse/*.whl "$target/$dir/wheelhouse/" 2>/dev/null || true
  done

  log "Starting the offline npm mirror (Verdaccio) on :${NPM_MIRROR_PORT:-4873}"
  docker volume create npm-mirror-storage >/dev/null
  docker run --rm -i -v npm-mirror-storage:/s busybox:1.37 sh -c 'tar -C /s -xf - && chown -R 10001:65533 /s' \
    <"$BUNDLE/npm-storage.tar"
  (cd "$target/offline/npm-mirror" && MODE=offline docker compose up -d --no-build)

  cat <<EOF

Done. Next steps (details in $target/OFFLINE.md):
  npm config set registry http://<this-host>:${NPM_MIRROR_PORT:-4873}/
  cd $target/hatchet     && cp .env.example .env && docker compose up -d && ./scripts/create-token.sh
  cd $target/trigger.dev && cp .env.example .env && cat local.env.example >> .env && ./generate-secrets.sh
                            docker compose up -d
  $BUNDLE/load.sh trigger-base-images
  cd $target/board       && cp .env.example .env   # add the Hatchet token / Trigger.dev secret key
                            docker compose up -d --no-build   # -> http://<this-host>:8090
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

  log "Pushing deploy base images into $registry"
  docker volume rm -f offline-skopeo >/dev/null 2>&1 || true
  docker run --rm -i -v offline-skopeo:/work busybox:1.37 tar -C /work -xf - <"$BUNDLE/trigger-base-images.tar"

  local base="" build=""
  while read -r runtime env_var dir repo; do
    # Push over the compose network (service name `registry`); the digest is identical, so the
    # image can then be referenced as $registry/... by the BuildKit builder on the host network.
    docker run --rm --network webapp -v offline-skopeo:/work "$skopeo" copy --quiet --preserve-digests \
      --dest-tls-verify=false --dest-creds "$DOCKER_REGISTRY_USERNAME:$DOCKER_REGISTRY_PASSWORD" \
      "dir:/work/$dir" "docker://registry:5000/$repo"
    digest=sha256:$(docker run --rm -v offline-skopeo:/work busybox:1.37 sha256sum "/work/$dir/manifest.json" | cut -d' ' -f1)
    image="$registry/${repo%%:*}@$digest"
    echo "  $env_var $image"
    case "$env_var" in
      DEPLOY_BASE_IMAGES) base="${base:+$base,}$runtime=$image" ;;
      DEPLOY_BUILD_BASE_IMAGES) build="${build:+$build,}$runtime=$image" ;;
      BUILDKIT_SYNTAX) syntax="$image" ;;
    esac
  done <"$BUNDLE/trigger-base-images.txt"
  docker volume rm -f offline-skopeo >/dev/null

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
