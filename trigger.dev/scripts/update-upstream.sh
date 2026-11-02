#!/bin/sh
# Upgrade the Trigger.dev self-hosting files to an upstream release tag.
#
#   ./scripts/update-upstream.sh v4.9.0          # refresh files + bump TRIGGER_IMAGE_TAG in .env
#   docker compose pull && docker compose up -d --build
#
# What it does:
#   1. sparse-clones hosting/docker at <tag> from github.com/triggerdotdev/trigger.dev
#   2. overwrites ONLY upstream-owned files (webapp/, worker/, clickhouse/*.xml, .env.example,
#      generate-secrets.sh, docker-compose.traefik.yml) - never .env, webapp.env,
#      registry/auth.htpasswd, docker-compose.local.yml or anything under local/
#   3. prints the upstream diff so you can check whether docker-compose.local.yml still applies
#   4. sets TRIGGER_IMAGE_TAG=<tag> in .env and records the tag in UPSTREAM_VERSION
#
# Needs internet (run it on the connected side, then re-bundle for the offline network).
set -eu

tag="${1:?usage: $0 <release tag, e.g. v4.9.0>}"
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

git clone --quiet --depth 1 --branch "$tag" --filter=blob:none --sparse \
  https://github.com/triggerdotdev/trigger.dev.git "$tmp/src"
git -C "$tmp/src" sparse-checkout set hosting/docker
src="$tmp/src/hosting/docker"

echo "== upstream changes since $(cat "$root/UPSTREAM_VERSION" 2>/dev/null || echo '?') =="
for f in webapp/docker-compose.yml worker/docker-compose.yml .env.example generate-secrets.sh \
         clickhouse/data-paths.xml clickhouse/override.xml clickhouse/users-override.xml; do
  diff -u "$root/$f" "$src/$f" || true
done

mkdir -p "$root/webapp" "$root/worker" "$root/clickhouse"
cp "$src/webapp/docker-compose.yml" "$root/webapp/"
cp "$src/worker/docker-compose.yml" "$root/worker/"
cp "$src"/clickhouse/*.xml "$root/clickhouse/"
cp "$src/.env.example" "$src/generate-secrets.sh" "$src/docker-compose.traefik.yml" "$root/"
# Upstream ships webapp/.env and worker/.env as symlinks to ../.env. Recreate them where
# symlinks work; elsewhere (Windows) Compose falls back to the .env in the working directory.
for d in webapp worker; do
  rm -f "$root/$d/.env"
  case "$(uname -s)" in
    MINGW* | MSYS* | CYGWIN*) ;; # Git Bash `ln -s` makes a copy, which would go stale
    *) ln -s ../.env "$root/$d/.env" ;;
  esac
done

if [ -f "$root/.env" ]; then
  sed -i "s|^TRIGGER_IMAGE_TAG=.*|TRIGGER_IMAGE_TAG=$tag|" "$root/.env"
fi
echo "$tag" >"$root/UPSTREAM_VERSION"

# Fail loudly if the override no longer merges (e.g. a service was renamed upstream).
(cd "$root" && docker compose config -q) && echo "OK: upstream $tag + local overrides resolve cleanly."
echo "Next: read the release notes, back up postgres, then: docker compose pull && docker compose up -d --build"
