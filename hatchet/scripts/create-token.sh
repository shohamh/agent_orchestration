#!/bin/sh
# Issue an API token for the default tenant and store it in .env as HATCHET_CLIENT_TOKEN.
# Run from the hatchet/ directory after `docker compose up -d`.
set -eu
cd "$(dirname "$0")/.."

# Stop Git Bash on Windows from rewriting /hatchet/... into C:/Program Files/Git/hatchet/...
export MSYS_NO_PATHCONV=1

DEFAULT_TENANT_ID=707d0855-80ab-4e1f-a156-f1c4546cbf52
[ -f .env ] || cp .env.example .env

token=$(docker compose run --rm --no-deps -T setup-config \
  /hatchet/hatchet-admin token create --config /hatchet/config --tenant-id "$DEFAULT_TENANT_ID" \
  | tr -d '\r' | grep -E '^ey' | tail -n1)

if [ -z "$token" ]; then
  echo "Failed to create token (is the stack up?)" >&2
  exit 1
fi

if grep -q '^HATCHET_CLIENT_TOKEN=' .env; then
  sed -i "s|^HATCHET_CLIENT_TOKEN=.*|HATCHET_CLIENT_TOKEN=$token|" .env
else
  printf 'HATCHET_CLIENT_TOKEN=%s\n' "$token" >>.env
fi
echo "Wrote HATCHET_CLIENT_TOKEN to .env"
