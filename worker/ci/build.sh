#!/bin/sh
# Build the worker for Linux x86_64 as one static (musl) binary and write it, with a checksum, to dist/worker/.
# CI (GitHub Actions and GitLab CI) and developers run this same script:
#   worker/ci/build.sh                 # builds on a musl system (Alpine, e.g. the rust:alpine image) or adds the musl target elsewhere
# Output: dist/worker/agent-worker-x86_64-unknown-linux-musl and its .sha256
set -eu
cd "$(dirname "$0")/.."            # the worker/ folder
ROOT=$(cd .. && pwd)
TARGET=x86_64-unknown-linux-musl
OUT="${OUT:-$ROOT/dist/worker}"
NAME="agent-worker-$TARGET"

if [ "$(uname -s)" != Linux ]; then echo "build.sh builds the Linux binary; on Windows run worker/ci/build.ps1" >&2; exit 1; fi
if ! command -v cargo >/dev/null 2>&1; then echo "cargo is not installed (use the rust image or https://rustup.rs)" >&2; exit 1; fi

# On a musl system (Alpine) the default target is already musl; elsewhere add the target.
if ! rustc -vV | grep -q 'host: .*musl'; then
  command -v rustup >/dev/null 2>&1 && rustup target add "$TARGET"
  FLAGS="--target $TARGET"
  BUILT="target/$TARGET/release/agent-worker"
else
  FLAGS=""
  BUILT="target/release/agent-worker"
fi

# shellcheck disable=SC2086
cargo build --release --locked $FLAGS
mkdir -p "$OUT"
cp "$BUILT" "$OUT/$NAME"
chmod 755 "$OUT/$NAME"
( cd "$OUT" && sha256sum "$NAME" > "$NAME.sha256" )
echo "Built $OUT/$NAME"
# A static binary has no dynamic section: it needs no system libraries on the PC.
if command -v file >/dev/null 2>&1; then file "$OUT/$NAME"; fi
