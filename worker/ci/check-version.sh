#!/bin/sh
# Fail unless a release tag (worker-vX.Y.Z) matches the version in worker/Cargo.toml. Used by both CI systems before a release.
#   worker/ci/check-version.sh worker-v0.1.0
set -eu
tag="${1:?usage: check-version.sh <tag like worker-v0.1.0>}"
want="${tag#worker-v}"
if [ "$want" = "$tag" ]; then echo "the tag must look like worker-v0.1.0, got $tag" >&2; exit 1; fi
have=$(sed -n 's/^version *= *"\(.*\)"/\1/p' "$(dirname "$0")/../Cargo.toml" | head -1)
if [ "$have" != "$want" ]; then echo "tag $tag does not match worker/Cargo.toml version $have: bump the version, commit, then tag" >&2; exit 1; fi
echo "version $have matches $tag"
