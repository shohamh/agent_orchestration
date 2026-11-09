"""Agent board: kanban-style view over Hatchet and Trigger.dev runs.

  GET  /api/systems                                   -> enabled backends + column order
  GET  /api/live?system=&board=[&card=]               -> server-sent events: card changes, plus the
                                                      open card's new transcript rows / log lines
  GET  /api/boards/{system}/{board}/cards             -> cards on a board (no transcripts or logs)
  POST /api/boards/{system}/{board}/cards             {kind, goal, turns, stepSeconds} -> new run
                                                      kind: steerable-agent (default) | research-report |
                                                      agent-session | program {command} | flow {flow, input}
                                                      optional: worker {id?, pool?, pools?, tags?}, context
                                                      {text?, repositories?, files?, confluence?}, schedule
                                                      {at?, cron?, timezone?, utcOffset?, summary?, overlap?, runNow?}
  POST /api/uploads?name=<file name>                  raw file bytes -> {id, name, size} for context.files
  DELETE /api/uploads/{id}
  GET  /api/schedules?system=&board=                  cards that start later or repeat
  POST /api/schedules/preview {at?, cron?, timezone?, utcOffset?} -> {next: [time, ...]}
  POST /api/schedules/{id}/{run|enable|disable}, DELETE /api/schedules/{id}
  GET  /api/cards/{system}/{run_id}/dag               -> nodes + edges of the run's DAG / call tree
  GET  /api/cards/{system}/{run_id}/transcript       ?before=<row key>&limit=100 -> older session rows
  GET  /api/cards/{system}/{run_id}/logs             ?before=<index>&limit=1000 -> older log lines
  GET  /api/cards/{system}/{run_id}/events           -> raw session events (scripts and tests)
  GET  /api/cards/{system}/{run_id}/nodes/{ref}/logs  -> one DAG node's log, paged like /logs
                                                      (&after=<index> returns only newer lines)
  POST /api/cards/{system}/{run_id}/nodes/{ref}/input  agent input for one step of a portable flow
  GET  /api/flows                                     -> the active portable flows (what can be launched)
  POST /api/flows/validate {yaml}                     -> {ok, flow | error}
  PUT|DELETE /api/flows/{name} {yaml}                 save / delete a flow draft (nothing is activated)
  /api/flow-documents[/{name}[/layout|/activation|/activate]], /api/flow-documents/parse|serialize
                                                      workflow builder: drafts, revisions, layout and
                                                      confirmed activation (2026-11-03-workflow-builder-api.md)
  POST /api/launch/validate {flow|document|card, execution?, system?}
                                                      -> ready | wait | blocked, with per-worker reasons
  GET  /api/cards/{system}/{run_id}/execution         -> placement records: executing PC, waiting reason
  POST /api/cards/{system}/{run_id}/steer             {instruction}
  POST /api/cards/{system}/{run_id}/{stop|cancel|replay}
  POST /api/queues/{system}/{queue}/pause             {paused: bool}   (Trigger.dev only)

A backend is enabled when its credentials are present (HATCHET_CLIENT_TOKEN / TRIGGER_SECRET_KEY).
No external assets: the UI in ./static works with no internet.
"""

import asyncio
import logging
import json
import base64
import binascii
import hashlib
import os
from pathlib import Path
from typing import Any

from aiohttp import web

import auth
import moves
import relay_proxy
from cards import COLUMNS, Dag, DagNode, is_final
from flows_client import FlowHost, flow_for_task
from live import Hub
from transcript import page
from steps import node_session
from backend_links import backend_url, tenant_id

STATIC = Path(__file__).parent / "static"
log = logging.getLogger("board")


def load_adapters() -> dict[str, Any]:
    import engine_admin
    engine_admin.prepare_environment()
    adapters: dict[str, Any] = {}
    if os.environ.get("HATCHET_CLIENT_TOKEN"):
        try:
            from hatchet_adapter import HatchetAdapter
        except ImportError:  # the other engines still work
            log.error("HATCHET_CLIENT_TOKEN is set but this image has no Hatchet SDK: rebuild it with ENGINES=hatchet")
        else:
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
    if os.environ.get("BOARD_LOADTEST_CARDS"):
        from loadtest_adapter import LoadTestAdapter

        adapters["loadtest"] = LoadTestAdapter()
    return adapters


def adapter_of(app: web.Application, system: str) -> Any:
    adapter = app["adapters"].get(system)
    if adapter is None:
        raise web.HTTPNotFound(text=f"backend {system!r} is not configured")
    return adapter


def adapter_for(request: web.Request) -> Any:
    return adapter_of(request.app, request.match_info["system"])


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


async def admin_engines(request: web.Request) -> web.Response:
    import engine_admin
    records = await asyncio.gather(*(engine_admin.record(name, check=True, app=request.app) for name in engine_admin.ENGINES))
    for item in records:
        if item["enabled"]:
            request.app.setdefault("engine_status", {})[item["id"]] = (item["status"], item["checked_at"], item["error"])
    return web.json_response({"engines": records})


async def admin_engine_save(request: web.Request) -> web.Response:
    import engine_admin
    engine = request.match_info["engine"]
    if engine not in engine_admin.ENGINES:
        raise web.HTTPNotFound(text="unknown engine")
    config = engine_admin.save(engine, await json_object(request))
    engine_admin.apply_adapter_url(engine, config["api_url"], config["dashboard_url"], request.app)
    if engine == "prefect" and config["api_url"] and engine not in request.app["adapters"]:
        from prefect_adapter import PrefectAdapter
        request.app["adapters"][engine] = PrefectAdapter()
    request.app.setdefault("engine_status", {}).pop(engine, None)
    return web.json_response(await engine_admin.record(engine, app=request.app))


async def admin_engine_check(request: web.Request) -> web.Response:
    import engine_admin
    engine = request.match_info["engine"]
    if engine not in engine_admin.ENGINES:
        raise web.HTTPNotFound(text="unknown engine")
    result = await engine_admin.check(engine, request.app)
    return web.json_response(result, status=200 if result["status"] != "disabled" else 409)


async def label_flows(app: web.Application, cards: list[Any]) -> None:
    flows = await app["flow_host"].flows()
    for card in cards:
        card.flow = flow_for_task(card.task, flows)
        card.backend_url = backend_url(card.system, card.id, tenant=tenant_id(app['adapters'].get(card.system)))
    moves.annotate(cards, sort=False)  # the UI sorts each column; no need to sort here too
    await label_workers(app, [c for c in cards if c.flow or c.task == "agent-session"])


async def label_workers(app: web.Application, cards: list[Any]) -> None:
    """The execution workers of each flow's steps and each placed session, with their state: the board
    searches them, and tells work in progress on an offline PC from work that is really live."""
    try:
        found = await asyncio.wait_for(app["flow_host"].executions({c.id: is_final(c.column) for c in cards}), 1)
    except asyncio.TimeoutError:
        return  # labels are extras: the next refresh tries again
    for card in cards:
        card.workers = [{"step": step, "state": execution.get("state"), **execution["worker"]} for step, execution in (found.get(card.id) or {}).items()
                        if isinstance(execution, dict) and isinstance(execution.get("worker"), dict)]


async def fleet_worker_jobs(request: web.Request) -> web.Response:
    """What ran on a PC, from the flow host's records. The board adds what it knows of each run: its
    title and where it is, for the boards someone is watching. Others are listed by run id."""
    status, body = await request.app["flow_host"].worker_jobs(request.match_info["worker_id"])
    if status != 200:
        return web.json_response(body, status=status)
    known = {}
    for (system, board), watcher in request.app["hub"].watchers.items():
        for card in (watcher.cards or {}).values():
            known[card["id"]] = {"title": card.get("title"), "column": card.get("column"), "system": system, "board": board}
    return web.json_response({"jobs": [{**job, **known.get(job["run"], {})} for job in body.get("jobs", [])]})


async def move_card(request: web.Request) -> web.Response:
    """Drag and drop: {board, to, above, below} (ids of the cards around the drop point, or null).
    The action follows from the card's column and the target (moves.py); returns the card's id,
    which is new when the engine had to re-create the run."""
    adapter = adapter_for(request)
    body = await json_object(request)
    board, to = text_field(body, "board", 200), text_field(body, "to", 20)
    run_id = request.match_info["run_id"]
    # Not the light pass: a coding-agent session's column and allowed moves come from its session
    # state, which the light pass doesn't read (it would look like a plain running card).
    cards = await adapter.list_cards(board)
    await label_flows(request.app, cards)
    card = next((c for c in cards if c.id == run_id), None)
    if card is None:
        raise web.HTTPNotFound(text="that card is no longer on this board")
    target = [c for c in cards if c.column == to]
    above, below = moves.placement(target, run_id, body.get("above"), body.get("below"))
    action = moves.validate(card, to, above, below)
    new_id = run_id
    if action in ("reorder", "restart"):
        new_id = await adapter.place(card, above, below)
    elif action == "start_now":
        await adapter.start_now(card)
    elif action == "cancel":
        await cancel_run(request, adapter, run_id)
    elif action == "stop":
        await adapter.steer(run_id, {"action": "stop"})
    elif action == "mark_done":
        await adapter.input(run_id, {"type": "done"})
    elif action == "continue":
        await adapter.input(run_id, {"type": "message", "text": "continue"})
    return web.json_response({"id": new_id, "action": action})


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
            if request.query.get("compact") == "1" and message.get("type") == "cards" and message.get("reset"):
                cards = message["cards"]
                fields = tuple(cards[0]) if cards else ()
                if all(tuple(card) == fields for card in cards):
                    message = {"type": "cards", "reset": True, "fields": fields,
                               "rows": [list(card.values()) for card in cards]}
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


SESSION_AGENTS = ("codex", "claude", "opencode", "opencode2", "omp", "pi", "demo")
PROGRAM = "program"  # a program card runs as a session whose `agent` is this and whose goal is the command
SCHEDULE_POLL_SECONDS = 10
MAX_UPLOAD_BYTES = 64 * 1024 * 1024  # the transfer limit of a run (agents/flowkit/artifacts.py)


async def create_card(request: web.Request) -> web.Response:
    body = await json_object(request)
    system, board = request.match_info["system"], request.match_info["board"]
    when = body.pop("schedule", None)
    status, result = await (launch_card(request.app, system, board, body) if when is None
                            else schedule_card(request.app, system, board, body, when))
    return web.json_response(result, status=status)


def card_title(body: dict[str, Any]) -> str:
    if body.get("kind") == "flow":
        first = next((v for v in (body.get("input") or {}).values() if isinstance(v, str) and v.strip()), "")
        return f"{body.get('flow')}: {first}"[:200] if first else str(body.get("flow"))[:200]
    return str(body.get("command") or body.get("goal") or body.get("kind") or "card").strip()[:200]


async def schedule_card(app: web.Application, system: str, board: str, body: dict[str, Any], when: Any) -> tuple[int, dict]:
    """Store a card that starts later or repeats, after checking that it could start at all. A card
    that also starts now is the schedule's first run, so a repeat can't overlap it."""
    if not isinstance(when, dict):
        raise ValueError("schedule must be an object")
    timing = {k: v for k, v in when.items() if k != "runNow"}
    status, checked = await launch_card(app, system, board, body, dry_run=True)
    if status >= 400:
        return status, checked
    host = app["flow_host"]
    saved = await host.create_schedule({"system": system, "board": board, "title": card_title(body), "kind": str(body.get("kind") or ""),
                                        "request": body, "schedule": timing})
    if not when.get("runNow"):
        return 201, {"schedule": saved}
    try:
        status, started = await launch_card(app, system, board, body)
    except Exception:
        await host.delete_schedule(saved["id"])
        raise
    if status >= 400:
        await host.delete_schedule(saved["id"])
        return status, started
    await host.record_schedule(saved["id"], run=started["id"])
    return 201, {**started, "schedule": saved}


async def launch_card(app: web.Application, system: str, board: str, body: dict[str, Any], dry_run: bool = False) -> tuple[int, dict]:
    """Check a card and, unless `dry_run`, start it. Returns (HTTP status, body). New card and the
    schedules go through here, so a scheduled card is held to what a card started by hand is."""
    adapter = adapter_of(app, system)
    kind = body.get("kind", "steerable-agent")
    if kind == "flow":
        return await launch_flow(app, adapter, system, board, body, dry_run)
    program = kind == PROGRAM
    if program:
        if body.get("resumeSessionId"):
            raise ValueError("a program card cannot resume an agent session")
        body = {**body, "goal": text_field(body, "command", 20000), "agent": PROGRAM}
        kind = "agent-session"
    goal = text_field(body, "goal", 20000).strip()
    if kind not in ("steerable-agent", "research-report", "agent-session"):
        raise web.HTTPBadRequest(text=f"unknown kind {kind!r}")
    options = {}
    if kind == "agent-session":
        agent = body.get("agent", "codex")
        if agent not in SESSION_AGENTS and not program:
            raise ValueError("agent must be codex, claude, opencode, opencode2, omp, pi or demo")
        workspace = body.get("workspace", "")
        if not isinstance(workspace, str) or len(workspace) > 4096 or "\x00" in workspace:
            raise ValueError("workspace must be a path of at most 4096 characters")
        options["session_options"] = {"agent": agent, "workspace": workspace.strip(),
                                      "simulateFailures": bounded_int(body, "simulateFailures", 0, 0, 20)}
        if body.get('resumeSessionId'):
            if not workspace.strip(): raise ValueError('resuming a session requires its original workspace')
            options['session_options']['resumeSessionId'] = text_field(body, 'resumeSessionId', 512)
        if program:
            text = (body.get("context") or {}).get("text") if isinstance(body.get("context"), dict) else None
            if text:
                options["session_options"]["programInput"] = text
        attached = await app["flow_host"].normalize_context(body["context"]) if body.get("context") else None
        if attached:
            options["session_options"]["context"] = {k: v for k, v in attached.items() if k != "text"}
        worker = worker_selector(body.get('worker'))
        if not worker and not workspace.strip() and not body.get('resumeSessionId') and system in ('hatchet', 'prefect'):
            worker = {'tags': []}  # default to any compatible fleet worker for new disposable sessions
        if worker:  # placed on the fleet by the engine adapter (agents/flowkit/distributed.py)
            if system not in ('hatchet', 'prefect'):
                raise ValueError('fleet placement of standalone cards is supported on Hatchet and Prefect')
            if workspace.strip() or body.get('resumeSessionId'):
                raise ValueError('a card placed on a fleet worker runs in a new workspace there; clear the workspace')
            # As for flows: a confirmed mismatch blocks launch; offline, busy or unchecked matches wait.
            launch = await app['flow_host'].validate_card(agent, worker, engine=SYSTEM_ENGINES.get(system), **({'command': goal} if program else {}))
            if launch['status'] == 'blocked':
                card = launch['card']
                return 409, {'error': f"Cannot launch: {card['message']} {card['resolution']}", 'launch': launch}
            options['session_options']['worker'] = worker
            options['launch'] = launch
    launch = options.pop('launch', None)
    turns, step_seconds = bounded_int(body, "turns", 6, 1, 1000), bounded_int(body, "stepSeconds", 5, 1, 3600)
    if dry_run:
        return 200, ({'launch': launch} if launch else {})
    run_id = await adapter.create(board, kind, goal, turns, step_seconds, **options)
    return 201, {"id": run_id, **({'launch': launch} if launch else {})}


def worker_selector(value: Any) -> dict[str, Any]:
    """A card's fleet requirement: {id?, pool?, pools? (any of them), tags? (all of them)}; empty or
    absent runs the card as before."""
    if value is None or value == {}:
        return {}
    if not isinstance(value, dict) or set(value) - {'id', 'pool', 'pools', 'tags'}:
        raise ValueError('worker must contain id, pool, pools and/or tags')
    for key in ('id', 'pool'):
        if key in value and (not isinstance(value[key], str) or not value[key] or len(value[key]) > 100):
            raise ValueError(f'worker {key} must be nonempty text (max 100)')
    for key in ('pools', 'tags'):
        items = value.get(key, [])
        if not isinstance(items, list) or len(items) > 50 or any(not isinstance(t, str) or not t or len(t) > 100 for t in items):
            raise ValueError(f'worker {key} must be a list of at most 50 nonempty strings')
    return dict(value)


async def upload_file(request: web.Request) -> web.Response:
    """One file attached in New card, kept by the flow host until the run starts. The body is read
    as a stream: the application's request size limit is for JSON calls."""
    data = bytearray()
    async for chunk in request.content.iter_chunked(1024 * 1024):
        data += chunk
        if len(data) > MAX_UPLOAD_BYTES:
            raise web.HTTPRequestEntityTooLarge(max_size=MAX_UPLOAD_BYTES, actual_size=len(data), text="A file can be at most 64 MiB")
    saved = await request.app["flow_host"].save_upload(request.query.get("name"), base64.b64encode(bytes(data)).decode())
    return web.json_response(saved, status=201)


async def delete_upload(request: web.Request) -> web.Response:
    return web.json_response(await request.app["flow_host"].delete_upload(request.match_info["id"]))


async def schedules_list(request: web.Request) -> web.Response:
    host = request.app["flow_host"]
    return web.json_response({"schedules": await host.schedules(request.query.get("system"), request.query.get("board"))})


async def schedules_preview(request: web.Request) -> web.Response:
    return web.json_response(await request.app["flow_host"].preview_schedule(await json_object(request)))


async def schedule_delete(request: web.Request) -> web.Response:
    return web.json_response(await request.app["flow_host"].delete_schedule(request.match_info["id"]))


async def schedule_action(request: web.Request) -> web.Response:
    host, schedule_id, action = request.app["flow_host"], request.match_info["id"], request.match_info["action"]
    if action in ("enable", "disable"):
        return web.json_response(await host.set_schedule_enabled(schedule_id, action == "enable"))
    if action != "run":
        raise web.HTTPNotFound()
    item = await host.schedule(schedule_id)
    outcome = await start_scheduled(request.app, item, by_hand=True)
    if outcome.get("error"):
        return web.json_response({"error": outcome["error"]}, status=409)
    return web.json_response(outcome)


async def start_scheduled(app: web.Application, item: dict[str, Any], by_hand: bool = False) -> dict[str, Any]:
    """Start one schedule's card and record the outcome on the schedule, where the board shows it.
    Unless the schedule's `overlap` is "start", a repeat is skipped while its previous run is active:
    an agent that waits for a human would otherwise collect a new run every hour."""
    host, system, board = app["flow_host"], item["system"], item["board"]
    try:
        if not by_hand and item.get("overlap") != "start" and item.get("last_run") and item.get("cron"):
            cards = await adapter_of(app, system).list_cards(board)
            previous = next((c for c in cards if c.id == item["last_run"]), None)
            if previous is not None and not is_final(previous.column):
                outcome = {"skipped": "The previous run is still active"}
                await host.record_schedule(item["id"], **outcome)
                return outcome
        status, result = await launch_card(app, system, board, dict(item["request"]))
        outcome = {"run": result["id"]} if status < 400 else {"error": result.get("error") or f"status {status}"}
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # the card can't start now; the schedule shows why and the next time still comes
        outcome = {"error": str(getattr(exc, "text", None) or exc)}
    await host.record_schedule(item["id"], **outcome)
    app["hub"].poke(system)
    return outcome


async def run_due_schedules(app: web.Application) -> None:
    systems = list(app["adapters"])
    if not systems:
        return
    for item in await app["flow_host"].claim_schedules(systems):
        await start_scheduled(app, item)


async def scheduler(app: web.Application) -> None:
    while True:
        await asyncio.sleep(SCHEDULE_POLL_SECONDS)
        try:
            await run_due_schedules(app)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # the flow host may be restarting; due schedules wait for the next pass
            log.debug("could not check schedules: %s", exc)


async def start_scheduler(app: web.Application) -> None:
    app["scheduler"] = asyncio.create_task(scheduler(app))


async def card_execution(request: web.Request) -> web.Response:
    """Where a standalone card actually runs (`execution`, or null) and every placed flow step's
    record (`steps`): executing PC, engine, state and waiting reason. The drawer polls it."""
    execution = await request.app['flow_host'].execution(request.match_info['run_id'])
    return web.json_response({'execution': execution.get('card'), 'steps': execution})


async def launch_flow(app: web.Application, adapter: Any, system: str, board: str, body: dict[str, Any], dry_run: bool) -> tuple[int, dict]:
    name = text_field(body, "flow", 100)
    flow = (await app["flow_host"].flows()).get(name)
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
    if 'execution' in body:
        params['flowExecution'] = await app['flow_host'].validate_routing(body['execution'], flow['steps'])
    if body.get('context'):
        attached = await app['flow_host'].normalize_context(body['context'])
        if attached:
            params['flowContext'] = attached
    # A confirmed mismatch blocks launch; offline, busy or unchecked matches launch and wait.
    launch = await app['flow_host'].validate_launch(flow, params.get('flowExecution'), engine=SYSTEM_ENGINES.get(system))
    if launch['status'] == 'blocked':
        problems = [f"{sid}: {d['message']} {d['resolution']}" for sid, d in launch['steps'].items() if d['status'] == 'blocked']
        return 409, {'error': 'Cannot launch: ' + ' | '.join(problems), 'launch': launch}
    if dry_run:
        return 200, {'launch': launch}
    run_id = await adapter.create_flow(board, name, params, title)
    return 201, {"id": run_id, 'launch': launch}


async def launch_validate(request: web.Request) -> web.Response:
    """Pre-launch check for the card and flow creation dialogs; saving is never blocked.
    One of: `flow` (an active flow's name), `document` (the flow builder's unsaved draft) or
    `card` ({agent, worker}: a standalone card placed on the fleet)."""
    body = await json_object(request)
    system = body.get('system')
    if system is not None and system not in SYSTEM_ENGINES:
        raise ValueError('unknown system')
    engine = SYSTEM_ENGINES.get(system)
    if sum(key in body for key in ('flow', 'document', 'card')) != 1:
        raise ValueError('send one of flow, document or card')
    if 'card' in body:
        card = body['card']
        if not isinstance(card, dict) or set(card) - {'agent', 'worker', 'command'}:
            raise ValueError('card must contain agent, worker and for a program its command')
        if card.get('agent') not in (*SESSION_AGENTS, PROGRAM):
            raise ValueError('agent must be codex, claude, opencode, opencode2, omp, pi or demo')
        worker = worker_selector(card.get('worker'))
        if not worker:
            raise ValueError('choose a worker, a pool or tags to place the card')
        if system not in ('hatchet', 'prefect'):
            raise ValueError('fleet placement of standalone cards is supported on Hatchet and Prefect')
        return web.json_response(await request.app['flow_host'].validate_card(card['agent'], worker, engine=engine, **({'command': card['command']} if card.get('command') else {})))
    if 'document' in body:
        flow = body['document']
        if not isinstance(flow, dict) or not isinstance(flow.get('steps'), dict) or len(json.dumps(flow)) > 200000:
            raise ValueError('document must be a flow mapping with steps (at most 200000 characters)')
    else:
        name = text_field(body, 'flow', 100)
        flow = (await request.app['flow_host'].flows()).get(name)
        if flow is None:
            raise ValueError(f'unknown flow {name!r}')
    execution = None
    if 'execution' in body:
        execution = await request.app['flow_host'].validate_routing(body['execution'], list(flow['steps']))
    return web.json_response(await request.app['flow_host'].validate_launch(flow, execution, engine=engine))


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
              "approval": {"type", "id", "optionId"}, "answer": {"type", "id", "answers"},
              "config": {"type", "id", "value"}, "mode": {"type", "id"}}
    allowed = fields.get(kind) if isinstance(kind, str) else None
    if kind == 'message' and set(body) == {'type', 'text', 'attachments'}:
        allowed = {'type', 'text', 'attachments'}
    if not allowed or set(body) != allowed:
        raise ValueError("input must be message, interrupt, done, approval, answer, config or mode with its required fields")
    if kind == "message":
        if body.get('attachments'):
            if not isinstance(body.get('text'), str) or len(body['text']) > 20000 or '\x00' in body['text']:
                raise ValueError('message text must be at most 20000 characters')
            items = body['attachments']
            if not isinstance(items, list) or len(items) > 5: raise ValueError('at most five attachments are allowed')
            total = 0
            for item in items:
                if not isinstance(item, dict) or set(item) != {'name', 'mimeType', 'data'}: raise ValueError('invalid attachment')
                text_field(item, 'name', 255); text_field(item, 'mimeType', 255)
                if '/' in item['name'] or '\\' in item['name']: raise ValueError('attachment name must be a file name')
                if not isinstance(item['data'], str) or len(item['data']) > 132000: raise ValueError('attachment data exceeds limit')
                try: total += len(base64.b64decode(item['data'], validate=True))
                except (ValueError, TypeError, binascii.Error): raise ValueError('attachment data must be base64') from None
            if total > 96 * 1024: raise ValueError('attachments exceed 96 KiB')
        else:
            text_field(body, "text", 20000)
    if kind in ("approval", "answer"):
        text_field(body, "id", 512)
    if kind in ("config", "mode"):
        text_field(body, "id", 512)
    if kind == "config" and not (type(body["value"]) is bool or isinstance(body["value"], str) and 0 < len(body["value"]) <= 512):
        raise ValueError("config value must be a boolean or nonempty text")
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
        try:
            execution = await request.app['flow_host'].execution(request.match_info['run_id'])
            for node in dag.nodes: node.execution = execution.get(node.step)
        except (RuntimeError, ValueError): pass
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
    info, flow, node, _ = await flow_node(request)
    adapter = adapter_for(request)
    if info["column"] not in ("done", "stopped"):
        raise web.HTTPConflict(text="Stop or finish the flow before restarting a step")
    body = await json_object(request)
    if 'worker' in body:  # the flow host checks the step's software too, as at launch
        await request.app['flow_host'].set_selection(request.match_info['run_id'], node.step, body['worker'], flow)
    if not hasattr(adapter, 'restart_node'):
        if request.match_info['system'] not in ('prefect', 'trigger-dev'):
            raise NotImplementedError('This backend has no step restart support')
        return web.json_response(await request.app['flow_host'].restart_node(request.match_info['system'], request.match_info['run_id'], node.ref))
    await adapter.restart_node(request.match_info["run_id"], node.ref)
    return web.json_response({"id": request.match_info["run_id"], "replayed": True})


async def flows_list(request: web.Request) -> web.Response:
    return web.json_response(await request.app["flow_host"].catalog(fresh=True))


async def workers_list(request: web.Request) -> web.Response:
    return web.json_response(await request.app['flow_host'].workers())


# ---------------------------------------------------------------- fleet tab
# The flow host owns the registry and talks to the workers; the board only passes requests on.
# Its answers are filtered once more here, so a fleet listing in the browser can never carry an
# endpoint, a token name or a token, whatever a flow host version returns.
SYSTEM_ENGINES = {'hatchet': 'hatchet', 'prefect': 'prefect', 'trigger-dev': 'trigger', 'trigger': 'trigger'}
FLEET_FIELDS = {'id', 'pools', 'tags', 'engines', 'slots', 'cpu_limit', 'ram_limit', 'url', 'token_env'}
PUBLIC_WORKER = {'id', 'uuid', 'pools', 'pool', 'tags', 'engines', 'slots', 'cpu_limit', 'ram_limit', 'connection', 'online',
                 'status', 'hostname', 'active', 'warnings'}
PUBLIC_CAPACITY = {'state', 'reason', 'cpu_percent', 'ram_percent', 'sample_age_seconds', 'window_seconds', 'ram_basis',
                   'leases', 'settings'}
PUBLIC_RESULT = {'status', 'checked_at', 'auth_status'}


def public_inventory(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    section = lambda name: {k: {f: v[f] for f in PUBLIC_RESULT if f in v} for k, v in (value.get(name) or {}).items()
                            if isinstance(k, str) and isinstance(v, dict)} if isinstance(value.get(name), dict) else {}
    return {'agents': section('agents'), 'commands': section('commands'), 'refreshed_at': value.get('refreshed_at'),
            'cached': bool(value.get('cached'))}


def public_worker(worker: Any) -> dict[str, Any]:
    if not isinstance(worker, dict):
        return {}
    result = {k: worker[k] for k in PUBLIC_WORKER if k in worker}
    if isinstance(worker.get('capacity'), dict):
        result['capacity'] = {k: worker['capacity'][k] for k in PUBLIC_CAPACITY if k in worker['capacity']}
    if 'inventory' in worker:
        result['inventory'] = public_inventory(worker['inventory'])
    if isinstance(worker.get('engine_status'), dict):
        result['engine_status'] = {e: {k: s[k] for k in ('route', 'coordinator', 'state', 'connected') if k in s}
                                   for e, s in worker['engine_status'].items() if isinstance(s, dict)}
    if isinstance(worker.get('settings_sync'), dict):
        result['settings_sync'] = {k: str(worker['settings_sync'][k])[:300] for k in ('state', 'message') if k in worker['settings_sync']}
    return result


def fleet_answer(result: tuple[int, Any]) -> web.Response:
    status, body = result
    if status < 400 and isinstance(body, dict):
        body = dict(body)
        if isinstance(body.get('workers'), list):
            body['workers'] = [public_worker(w) for w in body['workers']]
        if 'worker' in body:
            body['worker'] = public_worker(body['worker'])
        if 'inventory' in body:
            body['inventory'] = public_inventory(body['inventory'])
        if isinstance(body.get('settings_sync'), dict):
            body['settings_sync'] = public_worker({'settings_sync': body['settings_sync']}).get('settings_sync')
    return web.json_response(body, status=status)


async def fleet_worker_body(request: web.Request) -> dict[str, Any]:
    body = await json_object(request)
    unknown = set(body) - FLEET_FIELDS
    if unknown:
        raise ValueError('unknown worker fields: ' + ', '.join(sorted(unknown)))
    return body


async def fleet_list(request: web.Request) -> web.Response:
    return fleet_answer(await request.app['flow_host'].fleet())


async def fleet_metrics(request: web.Request) -> web.Response:
    return fleet_answer(await request.app['flow_host'].fleet_metrics())


async def fleet_tags(request: web.Request) -> web.Response:
    return passed_through(await request.app['flow_host'].fleet_tags())


async def fleet_add_tag(request: web.Request) -> web.Response:
    return passed_through(await request.app['flow_host'].add_fleet_tag(text_field(await json_object(request), 'name', 100)))


async def fleet_register(request: web.Request) -> web.Response:
    return fleet_answer(await request.app['flow_host'].register_worker(await fleet_worker_body(request)))


async def fleet_update(request: web.Request) -> web.Response:
    return fleet_answer(await request.app['flow_host'].update_worker(request.match_info['worker_id'], await fleet_worker_body(request)))

async def fleet_unregister(request: web.Request) -> web.Response:
    return fleet_answer(await request.app['flow_host'].unregister_worker(request.match_info['worker_id']))

INSTALL_FIELDS = {'id', 'os', 'engines', 'pool', 'tags', 'slots', 'cpu_limit', 'ram_limit', 'server', 'autostart', 'auto_restart', 'hatchet_token'}


async def fleet_updates(request: web.Request) -> web.Response:
    return passed_through(await request.app['flow_host'].fleet_updates())


async def fleet_update_start(request: web.Request) -> web.Response:
    return passed_through(await request.app['flow_host'].start_fleet_update())


async def fleet_update_stop(request: web.Request) -> web.Response:
    return passed_through(await request.app['flow_host'].stop_fleet_update())


async def fleet_update_link(request: web.Request) -> web.Response:
    body = await json_object(request)
    unknown = set(body) - {'worker', 'os', 'server'}
    if unknown:
        raise ValueError('unknown update link fields: ' + ', '.join(sorted(unknown)))
    return passed_through(await request.app['flow_host'].update_link(body))


INSTALL_PRIVATE_FIELDS = {
    'hatchet_token', 'hatchet_api_url', 'hatchet_worker_grpc_host_port',
    'hatchet_worker_tls_strategy', 'prefect_worker_api_url', 'prefect_api_url',
}


def public_install_answer(result: tuple[int, Any]) -> web.Response:
    status, body = result
    def scrub(value: Any) -> Any:
        if isinstance(value, dict):
            return {key: scrub(item) for key, item in value.items() if key not in INSTALL_PRIVATE_FIELDS}
        if isinstance(value, list):
            return [scrub(item) for item in value]
        return value
    return web.json_response(scrub(body), status=status)


async def fleet_installs(request: web.Request) -> web.Response:
    return public_install_answer(await request.app['flow_host'].fleet_installs())


async def fleet_install_create(request: web.Request) -> web.Response:
    body = await json_object(request)
    unknown = set(body) - INSTALL_FIELDS
    if unknown:
        raise ValueError('unknown install fields: ' + ', '.join(sorted(unknown)))
    import engine_admin
    engines = body.get('engines', [])
    if 'hatchet' in engines:
        config = engine_admin.config_for('hatchet')
        body['hatchet_api_url'] = config['api_url']
        body['hatchet_worker_grpc_host_port'] = config['hatchet_worker_grpc_host_port']
        body['hatchet_worker_tls_strategy'] = config['hatchet_worker_tls_strategy']
    if 'prefect' in engines:
        config = engine_admin.config_for('prefect')
        body['prefect_worker_api_url'] = config['prefect_worker_api_url']
    return public_install_answer(await request.app['flow_host'].create_install(body))


async def fleet_install_cancel(request: web.Request) -> web.Response:
    return public_install_answer(await request.app['flow_host'].cancel_install(request.match_info['code']))


async def fleet_refresh(request: web.Request) -> web.Response:
    return fleet_answer(await request.app['flow_host'].refresh_capabilities(request.match_info['worker_id']))


async def flows_validate(request: web.Request) -> web.Response:
    return web.json_response(await request.app["flow_host"].validate(text_field(await json_object(request), "yaml", 200000)))


async def flows_save(request: web.Request) -> web.Response:
    body = await json_object(request)
    text = text_field(body, "yaml", 200000)
    expected = body.get("expectedRevision", ...)
    if expected is not ... and expected is not None and not isinstance(expected, str):
        raise ValueError("expectedRevision must be a revision or null")
    return passed_through(await request.app["flow_host"].save(request.match_info["name"], text, expected))


async def flows_delete(request: web.Request) -> web.Response:
    await request.app["flow_host"].delete(request.match_info["name"])
    return web.json_response({"ok": True})


# Workflow builder: docs/superpowers/specs/2026-11-03-workflow-builder-api.md. The flow host owns
# drafts and activation; the board adds what only it can see, the active runs on every engine.
ENGINE_SYSTEMS = {"hatchet": "hatchet", "prefect": "prefect", "trigger": "trigger-dev"}


def passed_through(result: tuple[int, Any]) -> web.Response:
    status, body = result
    return web.json_response(body, status=status)


def yaml_field(body: dict) -> str:
    text = body.get("yaml")
    if not isinstance(text, str) or len(text) > 200000:
        raise ValueError("yaml must be text of at most 200000 characters")
    return text


async def flow_documents(request: web.Request) -> web.Response:
    return passed_through(await request.app["flow_host"].documents())


async def flow_document(request: web.Request) -> web.Response:
    return passed_through(await request.app["flow_host"].document(request.match_info["name"]))


async def flow_document_parse(request: web.Request) -> web.Response:
    return passed_through(await request.app["flow_host"].parse_document(yaml_field(await json_object(request))))


async def flow_document_serialize(request: web.Request) -> web.Response:
    return passed_through(await request.app["flow_host"].serialize_document((await json_object(request)).get("document")))


async def flow_document_save(request: web.Request) -> web.Response:
    body = await json_object(request)
    expected = body.get("expectedRevision")
    if expected is not None and not isinstance(expected, str):
        raise ValueError("expectedRevision must be a revision or null")
    return passed_through(await request.app["flow_host"].save_document(request.match_info["name"], yaml_field(body), expected))


async def flow_document_layout(request: web.Request) -> web.Response:
    return passed_through(await request.app["flow_host"].save_layout(request.match_info["name"], (await json_object(request)).get("layout")))


async def flow_document_delete(request: web.Request) -> web.Response:
    return passed_through(await request.app["flow_host"].delete_document(request.match_info["name"], request.query.get("revision", "")))


async def engine_runs(app: web.Application, engine: str, flows: dict[str, dict]) -> list[dict] | None:
    """Active runs an activation could interrupt on one engine, across all boards; None if unknown."""
    adapter = app["adapters"].get(ENGINE_SYSTEMS[engine])
    if adapter is None or not hasattr(adapter, "active_runs"):
        return None
    try:
        return await asyncio.wait_for(adapter.active_runs(flows), 20)
    except Exception as exc:  # unknown, never "no runs"
        log.warning("active runs on %s unavailable: %s: %s", engine, type(exc).__name__, exc)
        return None


async def activation_preview(app: web.Application, name: str, disable: bool = False) -> tuple[int, dict]:
    status, host = await app["flow_host"].activation(name, disable=disable)
    if status != 200:
        return status, host
    # Every flow an engine serves (a removal that failed somewhere included), with its definition.
    flows = {**(host.get("served") or {}), **({} if name in (host.get("served") or {}) else {name: {}})}
    runs = await asyncio.gather(*(engine_runs(app, e["engine"], flows) for e in host["engines"]))
    engines = [{"engine": e["engine"], "action": e["action"], "available": bool(e.get("enabled", True)) and found is not None,
                "activeRuns": found} for e, found in zip(host["engines"], runs)]
    disruptive = any(e["action"] != "none" and (e["activeRuns"] is None or e["activeRuns"]) for e in engines)
    impact = {"revision": host["revision"], "activeRevision": host["activeRevision"], "change": host["change"],
              "engines": [[e["engine"], e["action"], None if e["activeRuns"] is None else sorted(r["id"] for r in e["activeRuns"])]
                          for e in engines]}
    digest = hashlib.sha256(json.dumps(impact, sort_keys=True).encode()).hexdigest()
    return 200, {"name": name, "revision": host["revision"], "activeRevision": host["activeRevision"], "change": host["change"],
                 "engines": engines, "disruptive": disruptive, "impact": f"sha256:{digest}", "errors": host.get("errors", [])}


async def flow_document_activation(request: web.Request) -> web.Response:
    return passed_through(await activation_preview(request.app, request.match_info["name"], request.query.get("disable") == "1"))


async def flow_document_activate(request: web.Request) -> web.Response:
    """Recheck the impact right before applying: confirmation is enforced here, not in the browser."""
    body, name = await json_object(request), request.match_info["name"]
    status, preview = await activation_preview(request.app, name)
    if status != 200:
        return web.json_response(preview, status=status)
    if body.get("revision") != preview["revision"]:
        return web.json_response({"error": "conflict", "preview": preview}, status=409)
    if body.get("impact") != preview["impact"]:
        return web.json_response({"error": "impact changed", "preview": preview}, status=409)
    if preview["disruptive"] and body.get("confirm") is not True:
        return web.json_response({"error": "confirmation required", "preview": preview}, status=409)
    status, result = await request.app["flow_host"].activate(name, preview["revision"])
    if status == 409:  # the draft changed in between: show the board's view of the new draft
        result = {"error": "conflict", "preview": (await activation_preview(request.app, name))[1]}
    return web.json_response(result, status=status)


async def flow_document_deactivate(request: web.Request) -> web.Response:
    """Stop serving a flow's active version; its draft stays. The same recheck as activating."""
    body, name = await json_object(request), request.match_info["name"]
    status, preview = await activation_preview(request.app, name, disable=True)
    if status != 200:
        return web.json_response(preview, status=status)
    if body.get("revision") != preview["activeRevision"]:
        return web.json_response({"error": "conflict", "preview": preview}, status=409)
    if body.get("impact") != preview["impact"]:
        return web.json_response({"error": "impact changed", "preview": preview}, status=409)
    if preview["disruptive"] and body.get("confirm") is not True:
        return web.json_response({"error": "confirmation required", "preview": preview}, status=409)
    status, result = await request.app["flow_host"].deactivate(name, preview["activeRevision"])
    if status == 409:
        result = {"error": "conflict", "preview": (await activation_preview(request.app, name, disable=True))[1]}
    return web.json_response(result, status=status)


async def node_logs(request: web.Request) -> web.Response:
    return web.json_response(lines_page(request, await adapter_for(request).node_lines(request.match_info["ref"])))


async def cancel_run(request: web.Request, adapter: Any, run_id: str) -> None:
    """A user cancellation: tell the flow host first so a waiting step records `cancelled`
    rather than an engine timeout or interruption. Never blocks the cancellation itself."""
    try:
        await request.app['flow_host'].mark_cancelled(run_id)
    except Exception as exc:  # flow host down, or not a portable flow
        log.info('could not mark %s cancelled on the flow host: %s', run_id, exc)
    await adapter.cancel(run_id)


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
        await cancel_run(request, adapter, run_id)
    elif action == "replay":
        await adapter.replay(run_id)
    else:
        raise web.HTTPNotFound()
    return web.json_response({"ok": True})


async def pause_queue(request: web.Request) -> web.Response:
    paused = bool((await request.json()).get("paused"))
    await adapter_for(request).set_queue_paused(request.match_info["queue"], paused)
    return web.json_response({"ok": True})


async def index(request: web.Request) -> web.StreamResponse:
    if not request.app["dev_reload"]:
        return web.FileResponse(STATIC / "index.html")
    document = (STATIC / "index.html").read_text(encoding="utf-8")
    document = document.replace("</body>", '<script src="/static/dev-reload.js" defer></script>\n</body>', 1)
    return web.Response(text=document, content_type="text/html")


async def dev_revision(_: web.Request) -> web.Response:
    entries = []
    for path in sorted(STATIC.rglob("*")):
        if path.is_file():
            stat = path.stat()
            entries.append(f"{path.relative_to(STATIC).as_posix()}:{stat.st_mtime_ns}:{stat.st_size}")
    revision = hashlib.sha256("\n".join(entries).encode()).hexdigest()
    return web.json_response({"revision": revision}, headers={"Cache-Control": "no-store"})


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
    if "scheduler" in app:
        app["scheduler"].cancel()
        await asyncio.gather(app["scheduler"], return_exceptions=True)
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
    app.on_startup.append(start_scheduler)
    app.on_cleanup.append(close_adapters)
    app.on_response_prepare.append(revalidate_ui)
    auth.install(app)
    app["dev_reload"] = os.environ.get("BOARD_DEV_RELOAD") == "1"
    if app["dev_reload"]:
        app.router.add_get("/api/dev/revision", dev_revision)
    relay_proxy.install(app)
    app.router.add_get("/", index)
    app.router.add_get("/api/systems", systems)
    app.router.add_get("/api/admin/engines", admin_engines)
    app.router.add_put("/api/admin/engines/{engine}", admin_engine_save)
    app.router.add_post("/api/admin/engines/{engine}/check", admin_engine_check)
    app.router.add_get("/api/live", live)
    app.router.add_get("/api/boards/{system}/{board}/cards", list_cards)
    app.router.add_post("/api/boards/{system}/{board}/cards", create_card)
    app.router.add_post("/api/cards/{system}/{run_id}/input", session_input)
    app.router.add_get("/api/cards/{system}/{run_id}/dag", card_dag)
    app.router.add_get("/api/cards/{system}/{run_id}/transcript", card_transcript)
    app.router.add_get("/api/cards/{system}/{run_id}/logs", card_logs)
    app.router.add_get("/api/cards/{system}/{run_id}/events", card_events)
    app.router.add_get("/api/cards/{system}/{run_id}/execution", card_execution)
    app.router.add_get("/api/cards/{system}/{run_id}/nodes/{ref}/logs", node_logs)
    app.router.add_post("/api/cards/{system}/{run_id}/nodes/{ref}/input", node_input)
    app.router.add_get("/api/cards/{system}/{run_id}/nodes/{ref}/transcript", node_transcript)
    app.router.add_post("/api/cards/{system}/{run_id}/nodes/{ref}/restart", node_restart)
    app.router.add_get("/api/flows", flows_list)
    app.router.add_get("/api/flow-documents", flow_documents)
    app.router.add_post("/api/flow-documents/parse", flow_document_parse)
    app.router.add_post("/api/flow-documents/serialize", flow_document_serialize)
    app.router.add_get("/api/flow-documents/{name}", flow_document)
    app.router.add_put("/api/flow-documents/{name}", flow_document_save)
    app.router.add_delete("/api/flow-documents/{name}", flow_document_delete)
    app.router.add_put("/api/flow-documents/{name}/layout", flow_document_layout)
    app.router.add_get("/api/flow-documents/{name}/activation", flow_document_activation)
    app.router.add_post("/api/flow-documents/{name}/activate", flow_document_activate)
    app.router.add_post("/api/flow-documents/{name}/deactivate", flow_document_deactivate)
    app.router.add_get('/api/workers', workers_list)
    app.router.add_get('/api/fleet', fleet_list)
    app.router.add_get('/api/fleet/metrics', fleet_metrics)
    app.router.add_get('/api/fleet/tags', fleet_tags)
    app.router.add_post('/api/fleet/tags', fleet_add_tag)
    app.router.add_get('/api/fleet/updates', fleet_updates)
    app.router.add_post('/api/fleet/updates', fleet_update_start)
    app.router.add_delete('/api/fleet/updates', fleet_update_stop)
    app.router.add_post('/api/fleet/updates/link', fleet_update_link)
    app.router.add_get('/api/fleet/installs', fleet_installs)
    app.router.add_post('/api/fleet/installs', fleet_install_create)
    app.router.add_delete('/api/fleet/installs/{code}', fleet_install_cancel)
    app.router.add_post('/api/fleet/workers', fleet_register)
    app.router.add_put('/api/fleet/workers/{worker_id}', fleet_update)
    app.router.add_delete('/api/fleet/workers/{worker_id}', fleet_unregister)
    app.router.add_post('/api/fleet/workers/{worker_id}/refresh', fleet_refresh)
    app.router.add_get('/api/fleet/workers/{worker_id}/jobs', fleet_worker_jobs)
    app.router.add_post('/api/launch/validate', launch_validate)
    app.router.add_post('/api/uploads', upload_file)
    app.router.add_delete('/api/uploads/{id}', delete_upload)
    app.router.add_get('/api/schedules', schedules_list)
    app.router.add_post('/api/schedules/preview', schedules_preview)
    app.router.add_delete('/api/schedules/{id}', schedule_delete)
    app.router.add_post('/api/schedules/{id}/{action}', schedule_action)
    app.router.add_post("/api/flows/validate", flows_validate)
    app.router.add_put("/api/flows/{name}", flows_save)
    app.router.add_delete("/api/flows/{name}", flows_delete)
    app.router.add_post("/api/cards/{system}/{run_id}/move", move_card)
    app.router.add_post("/api/cards/{system}/{run_id}/{action}", card_action)
    app.router.add_post("/api/queues/{system}/{queue}/pause", pause_queue)
    app.router.add_static("/static", STATIC)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # BOARD_HOST: interface to listen on (default all, as the Docker image needs); 127.0.0.1 for a local copy.
    web.run_app(make_app(), host=os.environ.get("BOARD_HOST") or None, port=int(os.environ.get("BOARD_PORT", "8090")))
