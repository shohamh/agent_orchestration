#!/bin/sh
# Collect what a release publishes: the binaries from dist/worker plus one SHA256SUMS file, into the folder given
# (default dist/release). Both CI systems call this, then attach everything in that folder to the release.
#   worker/ci/release-assets.sh [folder]
set -eu
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
SRC="$ROOT/dist/worker"
DEST="${1:-$ROOT/dist/release}"
mkdir -p "$DEST"
found=0
for name in agent-worker-x86_64-pc-windows-msvc.exe agent-worker-x86_64-unknown-linux-musl; do
  if [ -f "$SRC/$name" ]; then cp "$SRC/$name" "$DEST/$name"; found=$((found + 1)); else echo "missing $SRC/$name" >&2; fi
done
[ "$found" -eq 2 ] || { echo "a release needs both binaries (windows and linux); found $found" >&2; exit 1; }
( cd "$DEST" && sha256sum agent-worker-x86_64-pc-windows-msvc.exe agent-worker-x86_64-unknown-linux-musl > SHA256SUMS )
cat "$DEST/SHA256SUMS"
echo "Release assets are in $DEST"
