"""Persistent engine URL settings and bounded connectivity checks for the board Admin tab."""
from __future__ import annotations

import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import aiohttp


ENGINES = {
    "hatchet": {"label": "Hatchet", "api_env": "HATCHET_CLIENT_SERVER_URL", "dashboard_env": "HATCHET_DASHBOARD_URL", "api_default": "http://host.docker.internal:8080", "dashboard_default": "http://localhost:8080", "credential_env": "HATCHET_CLIENT_TOKEN", "auth_env": "HATCHET_CLIENT_TOKEN"},
    "trigger": {"label": "Trigger.dev · production", "api_env": "TRIGGER_API_URL", "dashboard_env": "TRIGGER_DASHBOARD_URL", "api_default": "http://host.docker.internal:8030", "dashboard_default": "http://localhost:8030", "credential_env": "TRIGGER_SECRET_KEY", "auth_env": "TRIGGER_SECRET_KEY"},
    "trigger-dev": {"label": "Trigger.dev · local", "api_env": "TRIGGER_API_URL", "dashboard_env": "TRIGGER_DASHBOARD_URL", "api_default": "http://host.docker.internal:8030", "dashboard_default": "http://localhost:8030", "credential_env": "TRIGGER_DEV_SECRET_KEY", "auth_env": "TRIGGER_DEV_SECRET_KEY"},
    "prefect": {"label": "Prefect", "api_env": "PREFECT_API_URL", "dashboard_env": "PREFECT_DASHBOARD_URL", "api_default": "http://host.docker.internal:4200/api", "dashboard_default": "http://localhost:4200", "credential_env": "PREFECT_API_URL"},
}



def prepare_environment() -> None:
    """Apply persisted endpoints before adapter constructors read their environment."""
    settings = _read()
    for engine, item in ENGINES.items():
        saved = settings.get(engine, {})
        if "api_url" in saved:
            os.environ[item["api_env"]] = saved["api_url"]
        if "dashboard_url" in saved:
            os.environ[item["dashboard_env"]] = saved["dashboard_url"]


def apply_adapter_url(engine: str, url: str, dashboard_url: str, app: Any) -> None:
    """Update HTTP adapters immediately; Hatchet's SDK endpoint is constructor-bound."""
    os.environ[ENGINES[engine]["api_env"]] = url
    os.environ[ENGINES[engine]["dashboard_env"]] = dashboard_url
    if engine in ("trigger", "trigger-dev"):
        adapter = app.get("adapters", {}).get(engine)
        if adapter is not None:
            adapter.base_url = url
            if adapter._session is not None:
                asyncio.get_running_loop().create_task(adapter._session.close())
                adapter._session = None
    elif engine == "prefect":
        adapter = app.get("adapters", {}).get(engine)
        if adapter is not None:
            adapter.api = url
            if adapter._session is not None:
                asyncio.get_running_loop().create_task(adapter._session.close())
                adapter._session = None
def _path() -> Path:
    return Path(os.environ.get("BOARD_ENGINE_SETTINGS_PATH", "/data/engine-settings.json"))


def _read() -> dict[str, dict[str, str]]:
    try:
        value = json.loads(_path().read_text(encoding="utf-8"))
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _write(settings: dict[str, dict[str, str]]) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(settings, indent=2) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _valid_url(value: Any, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or len(value) > 2048:
        raise ValueError(f"{field} must be an HTTP or HTTPS URL")
    value = value.strip()
    if not value and allow_empty:
        return ""
    parts = urlsplit(value)
    if not value or parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password or parts.fragment:
        raise ValueError(f"{field} must be an HTTP or HTTPS URL without credentials or fragments")
    return value.rstrip("/")


def config_for(engine: str) -> dict[str, str]:
    item = ENGINES[engine]
    saved = _read().get(engine, {})
    return {
        "api_url": saved.get("api_url", os.environ.get(item["api_env"]) or item["api_default"]),
        "dashboard_url": saved.get("dashboard_url", os.environ.get(item["dashboard_env"]) or item["dashboard_default"]),
    }


async def record(engine: str, *, check: bool = False, app: Any = None) -> dict[str, Any]:
    item = ENGINES[engine]
    cfg = config_for(engine)
    enabled = bool(os.environ.get(item["credential_env"]))
    status, checked, error = "disabled", None, None
    if enabled:
        status = "unknown"
        if check:
            checked = datetime.now(timezone.utc).isoformat()
            try:
                timeout = aiohttp.ClientTimeout(total=3)
                headers = {}
                token = os.environ.get(item.get("auth_env", ""))
                if token:
                    headers["Authorization"] = f"Bearer {token}"
                async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
                    async with session.get(cfg["api_url"], allow_redirects=False) as response:
                        status = "unauthorized" if response.status in (401, 403) else "reachable"
                        if status == "unauthorized":
                            error = f"HTTP {response.status}: credentials rejected"
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as exc:
                status, error = "unreachable", (str(exc) or type(exc).__name__)[:300]
        elif app is not None:
            previous = app.setdefault("engine_status", {}).get(engine)
            if previous:
                status, checked, error = previous
    return {"id": engine, "label": item["label"], "enabled": enabled, **cfg,
            "status": status, "checked_at": checked, "error": error}


async def check(engine: str, app: Any) -> dict[str, Any]:
    result = await record(engine, check=True, app=app)
    app.setdefault("engine_status", {})[engine] = (result["status"], result["checked_at"], result["error"])
    return result


def save(engine: str, body: Any) -> dict[str, str]:
    if not isinstance(body, dict) or set(body) != {"api_url", "dashboard_url"}:
        raise ValueError("body must contain api_url and dashboard_url")
    values = {"api_url": _valid_url(body["api_url"], "api_url"),
              "dashboard_url": _valid_url(body["dashboard_url"], "dashboard_url", allow_empty=True)}
    settings = _read()
    settings[engine] = values
    _write(settings)
    return values
