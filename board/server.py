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
import os
from pathlib import Path
from typing import Any

from aiohttp import web

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
    except web.HTTPException:
        raise
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
    body = await request.json()
    goal = str(body.get("goal", "")).strip()
    if not goal:
        raise web.HTTPBadRequest(text="goal is required")
    kind = str(body.get("kind", "steerable-agent"))
    if kind not in ("steerable-agent", "research-report"):
        raise web.HTTPBadRequest(text=f"unknown kind {kind!r}")
    run_id = await adapter_for(request).create(
        request.match_info["board"],
        kind,
        goal,
        int(body.get("turns", 6)),
        int(body.get("stepSeconds", 5)),
    )
    return web.json_response({"id": run_id}, status=201)


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


def make_app() -> web.Application:
    app = web.Application(middlewares=[errors_as_json])
    app["adapters"] = load_adapters()
    app.on_cleanup.append(close_adapters)
    app.router.add_get("/", index)
    app.router.add_get("/api/systems", systems)
    app.router.add_get("/api/boards/{system}/{board}/cards", list_cards)
    app.router.add_post("/api/boards/{system}/{board}/cards", create_card)
    app.router.add_get("/api/cards/{system}/{run_id}/dag", card_dag)
    app.router.add_get("/api/cards/{system}/{run_id}/nodes/{ref}/logs", node_logs)
    app.router.add_post("/api/cards/{system}/{run_id}/{action}", card_action)
    app.router.add_post("/api/queues/{system}/{queue}/pause", pause_queue)
    app.router.add_static("/static", STATIC)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    web.run_app(make_app(), port=int(os.environ.get("BOARD_PORT", "8090")))
