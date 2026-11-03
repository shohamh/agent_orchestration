"""End-to-end smoke test of the board API against a running board + backends.

  python smoke_test.py [hatchet|trigger ...]     (default: every enabled backend)

Creates cards on a throwaway board, then checks: cards show up, a card reaches Running with
intermediate results, steering changes its goal, a graceful stop finishes it early, a cancel
stops another card, and replay re-runs a finished card.
"""

import json
import sys
import time
import urllib.request

import board_http
from typing import Any

BASE = "http://localhost:8090"


def call(method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    request = urllib.request.Request(
        BASE + path,
        method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"},
    )
    with board_http.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def wait_for(system: str, board: str, run_id: str, check: Any, what: str, timeout: float = 120) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        cards = {c["id"]: c for c in call("GET", f"/api/boards/{system}/{board}/cards")["cards"]}
        card = cards.get(run_id)
        if card:
            # Cards carry only their latest line; the log itself is paged separately.
            card["progress"] = call("GET", f"/api/cards/{system}/{run_id}/logs?limit=5000")["lines"]
        if card and check(card):
            return card
        time.sleep(1.5)
    raise AssertionError(f"[{system}] timed out waiting for {what}: {card}")


def run(system: str) -> None:
    board = f"smoke-{int(time.time())}"
    path = f"/api/boards/{system}/{board}/cards"
    steered = call("POST", path, {"goal": "summarize logs", "turns": 40, "stepSeconds": 3})["id"]
    victim = call("POST", path, {"goal": "long job", "turns": 100, "stepSeconds": 3})["id"]

    card = wait_for(system, board, steered, lambda c: c["column"] in ("running", "waiting") and c["progress"], "running with progress")
    print(f"[{system}] running, progress: {card['progress'][-1]!r}, queue: {card['queue']}")

    call("POST", f"/api/cards/{system}/{steered}/steer", {"instruction": "focus on errors only"})
    wait_for(system, board, steered, lambda c: any("focus on errors only" in line for line in c["progress"]), "steer to land")
    print(f"[{system}] steered")

    call("POST", f"/api/cards/{system}/{steered}/stop")
    card = wait_for(system, board, steered, lambda c: c["column"] == "done", "graceful stop")
    print(f"[{system}] stopped gracefully -> {card['status']}, last: {card['progress'][-1]!r}")

    wait_for(system, board, victim, lambda c: c["column"] in ("running", "waiting"), "victim running")
    call("POST", f"/api/cards/{system}/{victim}/cancel")
    card = wait_for(system, board, victim, lambda c: c["column"] == "stopped", "cancel")
    print(f"[{system}] cancelled -> {card['status']}")

    before = call("GET", path)["cards"]
    first_finish = next(c for c in before if c["id"] == steered)["finished_at"]
    call("POST", f"/api/cards/{system}/{steered}/replay")
    # Trigger.dev replays as a new run on the same board. Hatchet re-runs the same run id, and a
    # durable task replays from its recorded event history, so it can finish again within a second.
    if system == "trigger":
        deadline = time.time() + 60
        while len(call("GET", path)["cards"]) == len(before) and time.time() < deadline:
            time.sleep(1.5)
        assert len(call("GET", path)["cards"]) > len(before), "replay did not add a card"
    else:
        wait_for(system, board, steered, lambda c: c["finished_at"] != first_finish, "replay to re-run")
    print(f"[{system}] replayed")
    # DAG: a research-report card exposes its graph, and every node is clickable (has logs endpoint).
    report = call("POST", path, {"kind": "research-report", "goal": "smoke dag"})["id"]
    wait_for(system, board, report, lambda c: c["column"] == "done", "research report to finish")
    dag = call("GET", f"/api/cards/{system}/{report}/dag")
    labels = {n["label"].replace("-", "_") for n in dag["nodes"]}
    assert {"fetch_sources", "summarize", "extract_entities", "assess_risk"} <= labels, labels
    assert all(n["column"] == "done" for n in dag["nodes"]), [(n["label"], n["status"]) for n in dag["nodes"]]
    kinds = {e["kind"] for e in dag["edges"]}
    assert kinds == ({"depends"} if system == "hatchet" else {"calls", "then"}), kinds
    first = dag["nodes"][0]
    call("GET", f"/api/cards/{system}/{report}/nodes/{first['ref']}/logs")
    print(f"[{system}] dag: {len(dag['nodes'])} nodes, {len(dag['edges'])} edges ({dag['model']})")

    # Clean up anything still running from the replay.
    for c in call("GET", path)["cards"]:
        if c["column"] not in ("done", "stopped"):
            call("POST", f"/api/cards/{system}/{c['id']}/cancel")
    print(f"[{system}] OK (board {board})")


if __name__ == "__main__":
    systems = sys.argv[1:] or call("GET", "/api/systems")["systems"]
    for name in systems:
        run(name)
