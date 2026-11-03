"""Agent board: kanban-style view over Hatchet and Trigger.dev runs.

  GET  /api/systems                                   -> enabled backends + column order
  GET  /api/live?system=&board=[&card=]               -> server-sent events: card changes, plus the
                                                      open card's new transcript rows / log lines
  GET  /api/boards/{system}/{board}/cards             -> cards on a board (no transcripts or logs)
  POST /api/boards/{system}/{board}/cards             {kind, goal, turns, stepSeconds} -> new run
                                                      kind: steerable-agent (default) | research-report
  GET  /api/cards/{system}/{run_id}/dag               -> nodes + edges of the run's DAG / call tree
  GET  /api/cards/{system}/{run_id}/transcript       ?before=<row key>&limit=100 -> older session rows
  GET  /api/cards/{system}/{run_id}/logs             ?before=<index>&limit=1000 -> older log lines
  GET  /api/cards/{system}/{run_id}/events           -> raw session events (scripts and tests)
  GET  /api/cards/{system}/{run_id}/nodes/{ref}/logs  -> one DAG node's log, paged like /logs
                                                      (&after=<index> returns only newer lines)
  POST /api/cards/{system}/{run_id}/nodes/{ref}/input  agent input for one step of a portable flow
  GET  /api/flows                                     -> portable flows (flows/*.yaml via the flow host)
  POST /api/flows/validate {yaml}                     -> {ok, flow | error}
  PUT|DELETE /api/flows/{name} {yaml}                 save / delete a flow (all backends re-register it)
  POST /api/cards/{system}/{run_id}/steer             {instruction}
  POST /api/cards/{system}/{run_id}/{stop|cancel|replay}
  POST /api/queues/{system}/{queue}/pause             {paused: bool}   (Trigger.dev only)

A backend is enabled when its credentials are present (HATCHET_CLIENT_TOKEN / TRIGGER_SECRET_KEY).
No external assets: the UI in ./static works with no internet.
"""

import asyncio
import logging
import json
import os
from pathlib import Path
from typing import Any

from aiohttp import web

import auth
from cards import COLUMNS, Dag, DagNode
from flows_client import FlowHost, flow_for_task
from live import Hub
from transcript import page
from steps import node_session
from backend_links import backend_url, tenant_id

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


async def label_flows(app: web.Application, cards: list[Any]) -> None:
    flows = await app["flow_host"].flows()
    for card in cards:
        card.flow = flow_for_task(card.task, flows)
        card.backend_url = backend_url(card.system, card.id, tenant=tenant_id(app['adapters'].get(card.system)))


async def list_cards(request: web.Request) -> web.Response:
    cards = await adapter_for(request).list_cards(request.match_info["board"])
    await label_flows(request.app, cards)
    return web.json_response({"cards": [card.to_json() for card in cards]})


async def live(request: web.Request) -> web.StreamResponse:
    system, board = request.query.get("system", ""), request.query.get("board", "")
    if system not in request.app["adapters"]:
        raise web.HTTPNotFound(text=f"backend {system!r} is not configured")
    if not board or len(board) > 200:
        raise ValueError("board is required")
    response = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache",
                                           "X-Accel-Buffering": "no"})
    await response.prepare(request)
    watcher, sub = request.app["hub"].subscribe(system, board, request.query.get("card") or None,
                                              node=request.query.get("node"), kind=request.query.get("kind"))
    try:
        while True:
            try:
                message = await asyncio.wait_for(sub.queue.get(), 15)
            except asyncio.TimeoutError:
                await response.write(b": keep-alive\n\n")  # proxies close idle streams
                continue
            if message is None:
                break
            await response.write(f"data: {json.dumps(message, ensure_ascii=False, default=str)}\n\n".encode())
    except (ConnectionResetError, asyncio.CancelledError):
        pass
    finally:
        watcher.unsubscribe(sub)
    return response


def limit_param(request: web.Request, default: int, maximum: int) -> int:
    value = int(request.query.get("limit", default))
    if not 1 <= value <= maximum:
        raise ValueError(f"limit must be between 1 and {maximum}")
    return value


def lines_page(request: web.Request, lines: list[str]) -> dict[str, Any]:
    limit = limit_param(request, 1000, 5000)
    if "after" in request.query:
        after = int(request.query["after"])
        if 0 <= after <= len(lines) and len(lines) - after <= limit:
            return {"start": after, "lines": lines[after:], "total": len(lines)}
        before = len(lines)  # too far behind: start over from the newest page
    else:
        before = max(0, min(int(request.query.get("before", len(lines))), len(lines)))
    start = max(0, before - limit)
    return {"start": start, "lines": lines[start:before], "total": len(lines), "more": start > 0, "reset": True}


async def card_transcript(request: web.Request) -> web.Response:
    session = await adapter_for(request).session(request.match_info["run_id"])
    rows, more = page(session.rows, request.query.get("before"), limit_param(request, 100, 1000))
    return web.json_response({"rows": rows, "more": more, "truncated": session.truncated})


async def card_logs(request: web.Request) -> web.Response:
    return web.json_response(lines_page(request, await adapter_for(request).run_lines(request.match_info["run_id"])))


async def card_events(request: web.Request) -> web.Response:
    dropped, events = await adapter_for(request).events(request.match_info["run_id"])
    return web.json_response({"dropped": dropped, "events": events})


async def create_card(request: web.Request) -> web.Response:
    body = await json_object(request)
    kind = body.get("kind", "steerable-agent")
    if kind == "flow":
        return await create_flow_card(request, body)
    goal = text_field(body, "goal", 20000).strip()
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


async def create_flow_card(request: web.Request, body: dict[str, Any]) -> web.Response:
    name = text_field(body, "flow", 100)
    flow = (await request.app["flow_host"].flows()).get(name)
    if flow is None:
        raise ValueError(f"unknown flow {name!r}")
    raw = body.get("input") or {}
    if not isinstance(raw, dict) or set(raw) - set(flow["input"]):
        raise ValueError(f"input must only contain the flow's parameters: {', '.join(flow['input']) or 'none'}")
    params = {}
    for key, default in flow["input"].items():
        value = raw.get(key, default)
        if not (value is None or type(value) in (str, int, float, bool)) or (isinstance(value, str) and len(value) > 20000):
            raise ValueError(f"{key} must be text (at most 20000 characters), a number or true/false")
        if isinstance(default, bool):
            value = value if isinstance(value, bool) else str(value).lower() in ("1", "true", "yes", "on")
        elif isinstance(default, (int, float)) and isinstance(value, str):
            value = type(default)(value)
        params[key] = value
    first = next((v for v in params.values() if isinstance(v, str) and v.strip()), "")
    title = str(body.get("title") or (f"{name}: {first}" if first else name))[:200]
    run_id = await adapter_for(request).create_flow(request.match_info["board"], name, params, title)
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
    flows = await request.app["flow_host"].flows()
    name = flow_for_task(request.query.get("task", ""), flows)
    if name:
        dag = fit_flow_dag(dag, name, flows[name])
    for node in dag.nodes:
        node.restart_supported = hasattr(adapter_for(request), "restart_node") or request.match_info['system'] in ('prefect', 'trigger-dev')
        node.restart_mode = 'native' if hasattr(adapter_for(request), 'restart_node') else 'runtime'
        if not node.restart_supported:
            node.restart_reason = "This engine has no native step restart that invalidates descendants. Replay the whole flow instead."
        if node.ref:
            node.backend_url = backend_url(request.match_info['system'], node.ref, node=True, tenant=tenant_id(adapter_for(request)))
    return web.json_response(dag.topologically_sorted().to_json())


def fit_flow_dag(dag: Dag, name: str, flow: dict[str, Any]) -> Dag:
    """Show a portable flow as its definition: one node per step (grey until it starts), the
    definition's edges, and each node's kind. Trigger.dev reports a parent -> child call tree, so
    its parent node is dropped and its timing-inferred edges replaced by the declared ones."""
    by_step: dict[str, DagNode] = {}
    for node in dag.nodes:
        step = node.label[len(f"flow-{name}-"):] if node.label.startswith(f"flow-{name}-") else node.label
        if step in flow["steps"]:
            by_step[step] = node
    fitted = Dag(model="declared")
    for step, spec in flow["steps"].items():
        node = by_step.get(step) or DagNode(id=f"pending:{step}", label=step, column="pending", status="PENDING")
        node.label, node.step, node.kind = step, step, spec["kind"]
        node.id = f"step:{step}"  # logical identity is stable before/after engine task creation
        node.agent = spec.get("agent") if node.kind == "agent" else None
        fitted.nodes.append(node)
    ids = {n.step: n.id for n in fitted.nodes}
    fitted.edges = [{"from": ids[need], "to": ids[step], "kind": "depends"}
                    for step, spec in flow["steps"].items() for need in spec["needs"]]
    return fitted


async def node_input(request: web.Request) -> web.Response:
    adapter = adapter_for(request)
    if not hasattr(adapter, "node_input"):
        raise NotImplementedError("this backend has no per-step input")
    body = validate_session_input(await json_object(request))
    _, _, node, _ = await flow_node(request)
    if node.kind != "agent":
        raise ValueError("Only agent steps accept chat input")
    await adapter.node_input(request.match_info["run_id"], request.match_info["ref"], body)
    return web.json_response({"ok": True})


async def flow_node(request: web.Request):
    adapter = adapter_for(request)
    info = await adapter.run_info(request.match_info["run_id"])
    flows = await request.app["flow_host"].flows()
    name = flow_for_task(info["task"], flows)
    if name is None:
        raise ValueError("This run is not a portable flow")
    graph = fit_flow_dag(await adapter.dag(request.match_info["run_id"]), name, flows[name])
    node = next((n for n in graph.nodes if n.ref == request.match_info["ref"]), None)
    if node is None:
        raise web.HTTPNotFound(text="Step does not belong to this run")
    return info, flows[name], node, graph


async def node_transcript(request: web.Request) -> web.Response:
    _, _, node, _ = await flow_node(request)
    if node.kind != "agent":
        raise ValueError("Only agent steps have a transcript")
    session = await node_session(adapter_for(request), node.ref)
    rows, more = page(session.rows, request.query.get("before"), limit_param(request, 100, 1000))
    return web.json_response({"rows": rows, "more": more, "truncated": session.truncated})


async def node_restart(request: web.Request) -> web.Response:
    info, _, node, _ = await flow_node(request)
    adapter = adapter_for(request)
    if info["column"] not in ("done", "stopped"):
        raise web.HTTPConflict(text="Stop or finish the flow before restarting a step")
    if not hasattr(adapter, 'restart_node'):
        if request.match_info['system'] not in ('prefect', 'trigger-dev'):
            raise NotImplementedError('This backend has no step restart support')
        return web.json_response(await request.app['flow_host'].restart_node(request.match_info['system'], request.match_info['run_id'], node.ref))
    await adapter.restart_node(request.match_info["run_id"], node.ref)
    return web.json_response({"id": request.match_info["run_id"], "replayed": True})


async def flows_list(request: web.Request) -> web.Response:
    return web.json_response(await request.app["flow_host"].catalog(fresh=True))


async def flows_validate(request: web.Request) -> web.Response:
    return web.json_response(await request.app["flow_host"].validate(text_field(await json_object(request), "yaml", 200000)))


async def flows_save(request: web.Request) -> web.Response:
    text = text_field(await json_object(request), "yaml", 200000)
    return web.json_response(await request.app["flow_host"].save(request.match_info["name"], text))


async def flows_delete(request: web.Request) -> web.Response:
    await request.app["flow_host"].delete(request.match_info["name"])
    return web.json_response({"ok": True})


async def node_logs(request: web.Request) -> web.Response:
    return web.json_response(lines_page(request, await adapter_for(request).node_lines(request.match_info["ref"])))


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


@web.middleware
async def poke_after_writes(request: web.Request, handler: Any) -> web.StreamResponse:
    """A new card, an input or an action: refresh that backend's watched boards right away
    instead of waiting for the engine's feed to report it."""
    response = await handler(request)
    parts = request.path.split("/")
    if request.method != "GET" and response.status < 400 and len(parts) > 3 and parts[2] in ("cards", "boards"):
        request.app["hub"].poke(parts[3])
    return response


async def close_adapters(app: web.Application) -> None:
    await app["hub"].close()
    for adapter in [*app["adapters"].values(), app["flow_host"]]:
        if hasattr(adapter, "close"):
            await adapter.close()


async def revalidate_ui(request: web.Request, response: web.StreamResponse) -> None:
    # Without Cache-Control, browsers heuristically reuse the UI files after the board is rebuilt.
    if request.path == "/" or request.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache"


def make_app() -> web.Application:
    app = web.Application(middlewares=[errors_as_json, poke_after_writes])
    app["adapters"] = load_adapters()
    app["flow_host"] = FlowHost()
    app["hub"] = Hub(app["adapters"], decorate=lambda cards: label_flows(app, cards))
    app.on_cleanup.append(close_adapters)
    app.on_response_prepare.append(revalidate_ui)
    auth.install(app)
    app.router.add_get("/", index)
    app.router.add_get("/api/systems", systems)
    app.router.add_get("/api/live", live)
    app.router.add_get("/api/boards/{system}/{board}/cards", list_cards)
    app.router.add_post("/api/boards/{system}/{board}/cards", create_card)
    app.router.add_post("/api/cards/{system}/{run_id}/input", session_input)
    app.router.add_get("/api/cards/{system}/{run_id}/dag", card_dag)
    app.router.add_get("/api/cards/{system}/{run_id}/transcript", card_transcript)
    app.router.add_get("/api/cards/{system}/{run_id}/logs", card_logs)
    app.router.add_get("/api/cards/{system}/{run_id}/events", card_events)
    app.router.add_get("/api/cards/{system}/{run_id}/nodes/{ref}/logs", node_logs)
    app.router.add_post("/api/cards/{system}/{run_id}/nodes/{ref}/input", node_input)
    app.router.add_get("/api/cards/{system}/{run_id}/nodes/{ref}/transcript", node_transcript)
    app.router.add_post("/api/cards/{system}/{run_id}/nodes/{ref}/restart", node_restart)
    app.router.add_get("/api/flows", flows_list)
    app.router.add_post("/api/flows/validate", flows_validate)
    app.router.add_put("/api/flows/{name}", flows_save)
    app.router.add_delete("/api/flows/{name}", flows_delete)
    app.router.add_post("/api/cards/{system}/{run_id}/{action}", card_action)
    app.router.add_post("/api/queues/{system}/{queue}/pause", pause_queue)
    app.router.add_static("/static", STATIC)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # BOARD_HOST: interface to listen on (default all, as the Docker image needs); 127.0.0.1 for a local copy.
    web.run_app(make_app(), host=os.environ.get("BOARD_HOST") or None, port=int(os.environ.get("BOARD_PORT", "8090")))
