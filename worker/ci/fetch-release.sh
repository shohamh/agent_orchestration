#!/bin/sh
# Download a published worker release into dist/worker/, where the coordinator serves the binaries from.
# Works with a GitHub release or a GitLab release; run it on the coordinator (or anywhere, then copy dist/worker).
#   worker/ci/fetch-release.sh github shohamh/agent_orchestration worker-v0.1.0
#   worker/ci/fetch-release.sh gitlab https://gitlab.example.com group/project worker-v0.1.0
# Private projects: set GITHUB_TOKEN, or GITLAB_TOKEN (a token with read_api / read_package_registry).
set -eu
provider="${1:?usage: fetch-release.sh github <owner/repo> <tag> | gitlab <base-url> <group/project> <tag>}"
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT="${OUT:-$ROOT/dist/worker}"
mkdir -p "$OUT"
FILES="agent-worker-x86_64-pc-windows-msvc.exe agent-worker-x86_64-unknown-linux-musl SHA256SUMS"

case "$provider" in
  github)
    repo="${2:?owner/repo}"; tag="${3:?tag}"
    auth=""; [ -n "${GITHUB_TOKEN:-}" ] && auth="Authorization: Bearer $GITHUB_TOKEN"
    for f in $FILES; do
      echo "Downloading $f"
      curl -fsSL ${auth:+-H "$auth"} -o "$OUT/$f" "https://github.com/$repo/releases/download/$tag/$f"
    done ;;
  gitlab)
    base="${2:?base url}"; project="${3:?group/project}"; tag="${4:?tag}"
    encoded=$(printf '%s' "$project" | sed 's#/#%2F#g')
    auth=""; [ -n "${GITLAB_TOKEN:-}" ] && auth="PRIVATE-TOKEN: $GITLAB_TOKEN"
    version="${tag#worker-v}"
    for f in $FILES; do
      echo "Downloading $f"
      curl -fsSL ${auth:+-H "$auth"} -o "$OUT/$f" "$base/api/v4/projects/$encoded/packages/generic/agent-worker/$version/$f"
    done ;;
  *) echo "unknown provider $provider" >&2; exit 1 ;;
esac

chmod 755 "$OUT/agent-worker-x86_64-unknown-linux-musl" 2>/dev/null || true
( cd "$OUT" && grep -E 'agent-worker-x86_64' SHA256SUMS | sha256sum -c - )
echo "The coordinator now serves these from $OUT (restart is not needed: versions are read from the files)."
