"""`{{ input.x }}` / `{{ steps.<id>.output[.field...] }}` substitution in run commands and prompts."""

import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Any

_EXPR = re.compile(r"\{\{\s*([a-zA-Z_][\w.]*)\s*\}\}")
# Outside quotes these make cmd.exe do more than start one program.
_CMD_SYNTAX = set("&|<>^()")
# cmd.exe runs these itself even when a program of the same name is on PATH.
_CMD_BUILTINS = {"assoc", "break", "call", "cd", "chdir", "cls", "color", "copy", "date", "del", "dir", "echo",
                 "endlocal", "erase", "exit", "for", "ftype", "goto", "if", "md", "mkdir", "mklink", "move", "path",
                 "popd", "prompt", "pushd", "rd", "rem", "ren", "rename", "rmdir", "set", "setlocal", "shift",
                 "start", "time", "title", "type", "ver", "verify", "vol"}


def lookup(context: dict[str, Any], path: str) -> Any:
    value: Any = context
    for part in path.split("."):
        if isinstance(value, dict) and part in value:
            value = value[part]
        else:
            raise KeyError(f"{{{{ {path} }}}} is not available (known: input.*, steps.<needed step>.output)")
    return value


def _text(found: Any) -> str:
    return found if isinstance(found, str) else json.dumps(found, ensure_ascii=False)


def render(text: str, context: dict[str, Any], flow_dir: str = "", shell: bool = False) -> str:
    """Replace expressions; non-text values become JSON. In shell commands values are quoted."""
    if shell and os.name == "nt":
        return _render_cmd(text, context).replace("{flow_dir}", flow_dir)

    def value(match: re.Match) -> str:
        out = _text(lookup(context, match.group(1)))
        return shell_quote(out) if shell else out

    return _EXPR.sub(value, text).replace("{flow_dir}", flow_dir)


def shell_quote(text: str) -> str:
    import shlex
    import subprocess

    return subprocess.list2cmdline([text]) if os.name == "nt" else shlex.quote(text)


def _render_cmd(text: str, context: dict[str, Any]) -> str:
    """cmd.exe has no escape that survives every context, so a value is always in double quotes
    (where & | < > ^ are plain text) and may not contain what cmd.exe would still act on there."""
    out, at, quoted = [], 0, False
    for match in _EXPR.finditer(text):
        literal = text[at:match.start()]
        out.append(literal)
        quoted ^= literal.count('"') % 2 == 1
        value = _text(lookup(context, match.group(1)))
        if any(c in value for c in '"%\r\n'):
            raise ValueError(f"{{{{ {match.group(1)} }}}} contains a double quote, percent sign or line break, which cmd.exe "
                             "cannot pass on safely. Start the program directly (without pipes, redirection, built-in "
                             "commands or %VARIABLES%), or read the value from the FLOW_CONTEXT environment variable.")
        closing = text[match.end():match.end() + 1] == '"' if quoted else True
        if closing:  # backslashes before a closing quote would escape it for the program
            value = value + "\\" * (len(value) - len(value.rstrip("\\")))
        out.append(value if quoted else f'"{value}"')
        at = match.end()
    out.append(text[at:])
    return "".join(out)


def command_argv(text: str, context: dict[str, Any], flow_dir: str = "") -> list[str] | None:
    """Windows: the arguments of a command that can start without cmd.exe, or None.

    The command is split the way the C runtime splits a command line, and each value becomes part
    of its argument unchanged, so line breaks and quotes reach the program exactly and a value can
    never start another command. Commands that need cmd.exe (operators, %VARIABLES%, built-in
    commands, .cmd/.bat programs) return None.
    """
    pieces, at = [], 0
    for match in re.finditer(r"\{\{\s*([a-zA-Z_][\w.]*)\s*\}\}|\{flow_dir\}", text):
        pieces += [(False, text[at:match.start()]), (True, flow_dir if match.group(1) is None else _text(lookup(context, match.group(1))))]
        at = match.end()
    pieces.append((False, text[at:]))
    args, current, started, quoted = [], [], False, False
    for is_value, chunk in pieces:
        if is_value:
            current.append(chunk)
            started = True
            continue
        i = 0
        while i < len(chunk):
            c = chunk[i]
            if c == "\\":
                end = i
                while end < len(chunk) and chunk[end] == "\\":
                    end += 1
                count = end - i
                if chunk[end:end + 1] == '"':  # 2n backslashes + quote: n and a quote toggle; 2n+1: n and a quote
                    current.append("\\" * (count // 2) + ('"' if count % 2 else ""))
                    i = end + (count % 2)
                else:
                    current.append("\\" * count)
                    i = end
                started = True
            elif c == '"':
                if quoted and chunk[i + 1:i + 2] == '"':
                    current.append('"')
                    i += 2
                else:
                    quoted, i = not quoted, i + 1
                started = True
            elif c in " \t" and not quoted:
                if started:
                    args.append("".join(current))
                    current, started = [], False
                i += 1
            elif c == "%" or (c in _CMD_SYNTAX and not quoted):
                return None
            else:
                current.append(c)
                started = True
                i += 1
    if started:
        args.append("".join(current))
    if not args or args[0].lower() in _CMD_BUILTINS:
        return None
    # `python` means this worker's interpreter, so steps don't depend on PATH.
    program = sys.executable if args[0] == "python" else shutil.which(args[0])
    if not program or Path(program).suffix.lower() not in (".exe", ".com"):
        return None
    return [program, *args[1:]]
