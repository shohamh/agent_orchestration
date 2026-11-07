"""What the agent-worker's bundled Python runs: the same import and validation a Python worker does.

    python -m flowkit.confluence_runner   stdin: {"mode": "import"|"validate", "item": {...}, "target": "<path>"}

Exits 0 when it succeeded; otherwise the reason is on stderr, for the worker to report.
"""
import asyncio
import json
import sys
from pathlib import Path

from .confluence_export import import_confluence, validate_confluence


def main():
    request = json.load(sys.stdin)
    action = {'import': import_confluence, 'validate': validate_confluence}[request['mode']]
    asyncio.run(action(request['item'], Path(request['target'])))


if __name__ == '__main__':
    try:
        main()
    except BaseException as exc:  # the exporter's auth errors inherit BaseException
        print(f'{exc}', file=sys.stderr, flush=True)
        sys.exit(1)
