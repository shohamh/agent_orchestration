#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
    echo "usage: fleet-e2e-entrypoint install <install-uuid> --server <board-url>" >&2
    exit 64
fi

export AGENT_WORKER_SERVICE=process
/usr/local/bin/agent-worker "$@"
exec sleep infinity
