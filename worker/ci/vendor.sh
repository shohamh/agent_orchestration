#!/bin/sh
# Re-vendor the worker's dependencies into worker/vendor (needs network once). Commit the result.
set -eu
cd "$(dirname "$0")/.."
# cargo vendor reads the source replacement below, so lift it while fetching
cp .cargo/config.toml /tmp/agent-worker-cargo-config.bak
trap 'cp /tmp/agent-worker-cargo-config.bak .cargo/config.toml' EXIT
python3 - <<'PY'
import re
p = '.cargo/config.toml'
s = open(p).read()
open(p, 'w').write(re.split(r'\n# Every dependency is in worker/vendor', s)[0] + '\n')
PY
rm -rf vendor
cargo vendor --locked vendor >/dev/null
python3 ci/prune-vendor.py vendor
