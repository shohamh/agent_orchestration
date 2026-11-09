#!/bin/sh
# Collect what a release publishes: both worker binaries, both Confluence and native runtimes, plus SHA256SUMS,
# into the folder given (default dist/release). Both CI systems call this, then attach everything in that folder.
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
for name in confluence-runtime-windows.tar.gz confluence-runtime-linux.tar.gz native-runtime-windows.tar.gz native-runtime-linux.tar.gz; do
  [ -f "$SRC/$name" ] || { echo "a release needs $SRC/$name" >&2; exit 1; }
  cp "$SRC/$name" "$DEST/$name"
done
( cd "$DEST" && sha256sum agent-worker-x86_64-pc-windows-msvc.exe agent-worker-x86_64-unknown-linux-musl confluence-runtime-windows.tar.gz confluence-runtime-linux.tar.gz native-runtime-windows.tar.gz native-runtime-linux.tar.gz > SHA256SUMS )
cat "$DEST/SHA256SUMS"
echo "Release assets are in $DEST"
