"""`{{ input.x }}` / `{{ steps.<id>.output[.field...] }}` substitution in run commands and prompts."""

import json
import re
from typing import Any

_EXPR = re.compile(r"\{\{\s*([a-zA-Z_][\w.]*)\s*\}\}")


def lookup(context: dict[str, Any], path: str) -> Any:
    value: Any = context
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            raise KeyError(f"{{{{ {path} }}}} is not available (known: input.*, steps.<needed step>.output)")
    return value


def render(text: str, context: dict[str, Any], flow_dir: str = "", shell: bool = False) -> str:
    """Replace expressions; non-text values become JSON. In shell commands values are quoted."""

    def value(match: re.Match) -> str:
        found = lookup(context, match.group(1))
        out = found if isinstance(found, str) else json.dumps(found, ensure_ascii=False)
        return shell_quote(out) if shell else out

    return _EXPR.sub(value, text).replace("{flow_dir}", flow_dir)


def shell_quote(text: str) -> str:
    import os
    import shlex
    import subprocess

    return subprocess.list2cmdline([text]) if os.name == "nt" else shlex.quote(text)
