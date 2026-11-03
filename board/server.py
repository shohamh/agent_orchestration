"""Agent board: kanban-style view over Hatchet and Trigger.dev runs.

  GET  /api/systems                                   -> enabled backends + column order
  GET  /api/boards/{system}/{board}/cards             -> cards on a board
  POST /api/boards/{system}/{board}/cards             {kind, goal, turns, stepSeconds} -> new run
                                                      kind: steerable-agent (default) | research-report
  GET  /api/cards/{system}/{run_id}/dag               -> nodes + edges of the run's DAG / call tree
  GET  /api/cards/{system}/{run_id}/nodes/{ref}/logs  -> intermediate results of one DAG node
  POST /api/cards/{system}/{run_id}/steer             {instruction}
  POST /api/cards/{system}/{run_id}/{stop|cancel|replay}
  POST /api/queues/{system}/{queue}/pause             {paused: bool}   (Trigger.dev only)

A backend is enabled when its credentials are present (HATCHET_CLIENT_TOKEN / TRIGGER_SECRET_KEY).
No external assets: the UI in ./static works with no internet.
"""

import logging
import json
import os
from pathlib import Path
from typing import Any

from aiohttp import web

import auth
from cards import COLUMNS

STATIC = Path(__file__).parent / "static"
log = logging.getLogger("board")


def load_adapters() -> dict[str, Any]:
    adapters: dict[str, Any] = {}
    if os.environ.get("HATCHET_CLIENT_TOKEN"):
        from hatchet_adapter import HatchetAdapter

        adapters["hatchet"] = HatchetAdapter()
    if os.environ.get("TRIGGER_SECRET_KEY"):
        from trigger_adapter import TriggerAdapter

        adapters["trigger"] = TriggerAdapter()
    if os.environ.get("TRIGGER_DEV_SECRET_KEY"):
        from trigger_adapter import TriggerAdapter

        adapters["trigger-dev"] = TriggerAdapter(development=True)
    if os.environ.get("PREFECT_API_URL"):
        from prefect_adapter import PrefectAdapter

        adapters["prefect"] = PrefectAdapter()
    return adapters


def adapter_for(request: web.Request) -> Any:
    system = request.match_info["system"]
    adapter = request.app["adapters"].get(system)
    if adapter is None:
        raise web.HTTPNotFound(text=f"backend {system!r} is not configured")
    return adapter


@web.middleware
async def errors_as_json(request: web.Request, handler: Any) -> web.StreamResponse:
    try:
        return await handler(request)
    except web.HTTPException as exc:
        if exc.status >= 400:
            return web.json_response({"error": exc.text or exc.reason}, status=exc.status)
        raise
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        return web.json_response({"error": str(exc)}, status=400)
    except NotImplementedError as exc:
        return web.json_response({"error": str(exc)}, status=501)
    except Exception as exc:  # surface backend errors in the UI instead of a bare 500
        log.exception("request failed: %s %s", request.method, request.path)
        return web.json_response({"error": str(exc)}, status=502)


async def systems(request: web.Request) -> web.Response:
    return web.json_response({"systems": list(request.app["adapters"]), "columns": COLUMNS})


async def list_cards(request: web.Request) -> web.Response:
    cards = await adapter_for(request).list_cards(request.match_info["board"])
    return web.json_response({"cards": [card.to_json() for card in cards]})


async def create_card(request: web.Request) -> web.Response:
    body = await json_object(request)
    goal = text_field(body, "goal", 20000).strip()
    kind = body.get("kind", "steerable-agent")
    if kind not in ("steerable-agent", "research-report", "agent-session"):
        raise web.HTTPBadRequest(text=f"unknown kind {kind!r}")
    options = {}
    if kind == "agent-session":
        agent = body.get("agent", "codex")
        if agent not in ("codex", "claude", "opencode", "opencode2", "omp", "pi", "demo"):
            raise ValueError("agent must be codex, claude, opencode, opencode2, omp, pi or demo")
        workspace = body.get("workspace", "")
        if not isinstance(workspace, str) or len(workspace) > 4096 or "\x00" in workspace:
            raise ValueError("workspace must be a path of at most 4096 characters")
        options["session_options"] = {"agent": agent, "workspace": workspace.strip(),
                                      "simulateFailures": bounded_int(body, "simulateFailures", 0, 0, 20)}
    run_id = await adapter_for(request).create(
        request.match_info["board"],
        kind,
        goal,
        bounded_int(body, "turns", 6, 1, 1000),
        bounded_int(body, "stepSeconds", 5, 1, 3600),
        **options,
    )
    return web.json_response({"id": run_id}, status=201)


async def json_object(request: web.Request) -> dict[str, Any]:
    body = await request.json()
    if not isinstance(body, dict):
        raise ValueError("request body must be an object")
    return body


def text_field(body: dict, name: str, limit: int) -> str:
    value = body.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > limit or "\x00" in value:
        raise ValueError(f"{name} must be nonempty text of at most {limit} characters")
    return value


def bounded_int(body: dict, name: str, default: int, minimum: int, maximum: int) -> int:
    value = body.get(name, default)
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def validate_session_input(body: dict[str, Any]) -> dict[str, Any]:
    kind = body.get("type")
    fields = {"message": {"type", "text"}, "interrupt": {"type"}, "done": {"type"},
              "approval": {"type", "id", "optionId"}, "answer": {"type", "id", "answers"}}
    if not isinstance(kind, str) or kind not in fields or set(body) != fields[kind]:
        raise ValueError("input must be message, interrupt, done, approval or answer with its required fields")
    if kind == "message":
        text_field(body, "text", 20000)
    if kind in ("approval", "answer"):
        text_field(body, "id", 512)
    if kind == "approval":
        text_field(body, "optionId", 512)
    if kind == "answer":
        answers = body["answers"]
        scalar = lambda value: value is None or type(value) in (str, int, float, bool)
        if not isinstance(answers, dict) or len(answers) > 100:
            raise ValueError("answers must be an object with at most 100 fields")
        for key, value in answers.items():
            if len(key) > 512 or not (scalar(value) or (isinstance(value, list) and len(value) <= 100 and all(map(scalar, value)))):
                raise ValueError("answers values must be scalars or arrays of scalars")
        if len(json.dumps(answers, allow_nan=False)) > 20000:
            raise ValueError("answers exceed 20000 characters")
    return body


async def session_input(request: web.Request) -> web.Response:
    body = validate_session_input(await json_object(request))
    await adapter_for(request).input(request.match_info["run_id"], body)
    return web.json_response({"ok": True})


async def card_dag(request: web.Request) -> web.Response:
    dag = await adapter_for(request).dag(request.match_info["run_id"])
    return web.json_response(dag.topologically_sorted().to_json())


async def node_logs(request: web.Request) -> web.Response:
    lines = await adapter_for(request).node_logs(request.match_info["ref"])
    return web.json_response({"lines": lines})


async def card_action(request: web.Request) -> web.Response:
    adapter = adapter_for(request)
    run_id, action = request.match_info["run_id"], request.match_info["action"]
    if action == "steer":
        instruction = str((await request.json()).get("instruction", "")).strip()
        if not instruction:
            raise web.HTTPBadRequest(text="instruction is required")
        await adapter.steer(run_id, {"action": "redirect", "instruction": instruction})
    elif action == "stop":
        await adapter.steer(run_id, {"action": "stop"})
    elif action == "cancel":
        await adapter.cancel(run_id)
    elif action == "replay":
        await adapter.replay(run_id)
    else:
        raise web.HTTPNotFound()
    return web.json_response({"ok": True})


async def pause_queue(request: web.Request) -> web.Response:
    paused = bool((await request.json()).get("paused"))
    await adapter_for(request).set_queue_paused(request.match_info["queue"], paused)
    return web.json_response({"ok": True})


async def index(_: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC / "index.html")


async def close_adapters(app: web.Application) -> None:
    for adapter in app["adapters"].values():
        if hasattr(adapter, "close"):
            await adapter.close()


async def revalidate_ui(request: web.Request, response: web.StreamResponse) -> None:
    # Without Cache-Control, browsers heuristically reuse the UI files after the board is rebuilt.
    if request.path == "/" or request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache"


def make_app() -> web.Application:
    app = web.Application(middlewares=[errors_as_json])
    app["adapters"] = load_adapters()
    app.on_cleanup.append(close_adapters)
    app.on_response_prepare.append(revalidate_ui)
    auth.install(app)
    app.router.add_get("/", index)
    app.router.add_get("/api/systems", systems)
    app.router.add_get("/api/boards/{system}/{board}/cards", list_cards)
    app.router.add_post("/api/boards/{system}/{board}/cards", create_card)
    app.router.add_post("/api/cards/{system}/{run_id}/input", session_input)
    app.router.add_get("/api/cards/{system}/{run_id}/dag", card_dag)
    app.router.add_get("/api/cards/{system}/{run_id}/nodes/{ref}/logs", node_logs)
    app.router.add_post("/api/cards/{system}/{run_id}/{action}", card_action)
    app.router.add_post("/api/queues/{system}/{queue}/pause", pause_queue)
    app.router.add_static("/static", STATIC)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    web.run_app(make_app(), port=int(os.environ.get("BOARD_PORT", "8090")))
