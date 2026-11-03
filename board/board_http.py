"""urlopen() for the board's scripts: signs in first when the board has a password.

The password comes from BOARD_PASSWORD, or else from board/.env (the same file docker compose reads).
"""

import http.cookiejar
import os
import urllib.parse
import urllib.request
from pathlib import Path

_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
_signed_in: set[str] = set()


def _password() -> str:
    if os.environ.get("BOARD_PASSWORD"):
        return os.environ["BOARD_PASSWORD"]
    env = Path(__file__).with_name(".env")
    if env.is_file():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith("BOARD_PASSWORD="):
                return line.split("=", 1)[1].strip()
    return ""


def urlopen(request: urllib.request.Request, timeout: float = 60):
    parts = urllib.parse.urlsplit(request.full_url)
    origin = f"{parts.scheme}://{parts.netloc}"
    password = _password()
    if password and origin not in _signed_in:
        login = urllib.request.Request(origin + "/login", data=urllib.parse.urlencode({"password": password}).encode())
        _opener.open(login, timeout=30).close()
        _signed_in.add(origin)
    return _opener.open(request, timeout=timeout)
