"""Password login for the board. The board can start coding agents that run commands on this PC,
so it must not be reachable without a login once exposed (LAN, Tailscale Serve/Funnel).

Enabled when BOARD_PASSWORD is set. A successful login sets a signed, expiring cookie
(HttpOnly, SameSite=Strict, Secure over HTTPS). Every other route requires it: pages redirect to
/login, API calls get 401. Failed attempts are slowed down globally.
"""

import asyncio
import hashlib
import hmac
import html
import os
import secrets
import time

from aiohttp import web

COOKIE = "board_session"
LIFETIME = 30 * 24 * 3600
PUBLIC = {"/login", "/logout"}
# A PC installing or relaying has no browser session: these paths are proxied to the relay service,
# which checks the one-time install code or the PC's own token (see relay_proxy.py).
PUBLIC_PREFIXES = ("/install/", "/relay/")


class Auth:
    def __init__(self, password: str) -> None:
        self.password = password.encode()
        # Rotating BOARD_SECRET (or the password) signs everyone out.
        self.key = hashlib.sha256(os.environ.get("BOARD_SECRET", "").encode() + b"\0" + self.password).digest()
        self.failures = 0

    def _sign(self, expires: int) -> str:
        return hmac.new(self.key, f"v1:{expires}".encode(), hashlib.sha256).hexdigest()

    def token(self) -> str:
        expires = int(time.time()) + LIFETIME
        return f"{expires}.{self._sign(expires)}"

    def valid(self, token: str | None) -> bool:
        try:
            expires_text, signature = (token or "").split(".", 1)
            expires = int(expires_text)
        except ValueError:
            return False
        return expires > time.time() and hmac.compare_digest(signature, self._sign(expires))

    @web.middleware
    async def middleware(self, request: web.Request, handler):
        if request.path in PUBLIC or request.path.startswith(PUBLIC_PREFIXES) or self.valid(request.cookies.get(COOKIE)):
            return await handler(request)
        if request.path.startswith("/api/"):
            return web.json_response({"error": "Sign in required"}, status=401)
        raise web.HTTPFound("/login")

    async def login_page(self, request: web.Request, error: str = "") -> web.Response:
        message = f'<p class="error" role="alert">{html.escape(error)}</p>' if error else ""
        return web.Response(content_type="text/html", headers={"Cache-Control": "no-store"}, text=LOGIN_HTML.replace("{message}", message))

    async def login(self, request: web.Request) -> web.StreamResponse:
        if request.method == "GET":
            return await self.login_page(request)
        form = await request.post()
        if not hmac.compare_digest(str(form.get("password", "")).encode(), self.password):
            self.failures += 1
            await asyncio.sleep(min(2 ** min(self.failures, 4), 10) * (0.5 + secrets.randbelow(100) / 100))
            return await self.login_page(request, "Wrong password.")
        self.failures = 0
        response = web.HTTPFound("/")
        # Behind Tailscale Serve/Funnel the board sees plain HTTP from 127.0.0.1; the visitor used
        # HTTPS on the *.ts.net name (Tailscale terminates TLS there).
        host = request.headers.get("Host", "").split(":")[0]
        secure = request.secure or request.headers.get("X-Forwarded-Proto") == "https" or host.endswith(".ts.net")
        response.set_cookie(COOKIE, self.token(), max_age=LIFETIME, httponly=True, samesite="Strict", secure=secure)
        raise response

    async def logout(self, request: web.Request) -> web.StreamResponse:
        response = web.HTTPFound("/login")
        response.del_cookie(COOKIE)
        raise response


def install(app: web.Application) -> None:
    password = os.environ.get("BOARD_PASSWORD", "")
    if not password:
        return
    auth = Auth(password)
    app.middlewares.insert(0, auth.middleware)
    app.router.add_route("*", "/login", auth.login)
    app.router.add_get("/logout", auth.logout)


LOGIN_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex"><title>Agent Board · sign in</title>
<style>
  :root { --bg: #f6f7f9; --panel: #fff; --ink: #1d2330; --muted: #677085; --line: #e2e5ec; --accent: #3b5bdb; --danger: #c92a2a; }
  @media (prefers-color-scheme: dark) { :root { --bg: #12151b; --panel: #1b2029; --ink: #e6e9f0; --muted: #98a1b3; --line: #2b313d; --accent: #7c93f2; --danger: #ff6b6b; } }
  * { box-sizing: border-box; }
  body { margin: 0; min-height: 100vh; display: grid; place-items: center; padding: 16px; background: var(--bg); color: var(--ink);
         font: 16px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
  form { width: min(360px, 100%); background: var(--panel); border: 1px solid var(--line); border-radius: 12px; padding: 24px; display: grid; gap: 14px; }
  h1 { font-size: 20px; margin: 0; } p { margin: 0; color: var(--muted); font-size: 14px; }
  input { font: inherit; width: 100%; padding: 10px 12px; border-radius: 8px; border: 1px solid var(--line); background: var(--bg); color: var(--ink); }
  button { font: inherit; padding: 10px; border: 0; border-radius: 8px; background: var(--accent); color: #fff; font-weight: 600; cursor: pointer; }
  .error { color: var(--danger); }
</style></head>
<body><form method="post" action="/login">
  <h1>Agent Board</h1><p>This board can run coding agents on its host. Sign in to continue.</p>
  {message}
  <label for="password" style="font-size:14px">Password</label>
  <input id="password" name="password" type="password" autocomplete="current-password" required autofocus>
  <button type="submit">Sign in</button>
</form></body></html>"""
