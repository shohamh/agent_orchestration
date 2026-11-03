"""Exercise the full chat contract against live engines, with explicitly simulated agents.

  python board/session_smoke_test.py hatchet trigger-dev

Creates test cards on a separate board; sends scoped permissions and form answers, then marks done.
"""

import json
import sys
import time
import urllib.request

import board_http

BASE = "http://localhost:8090"


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with board_http.urlopen(req, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {path}: {exc.code} {exc.read().decode()[:1000]}") from exc


def run(system):
    board = f"chat-smoke-{int(time.time())}"
    path = f"/api/boards/{system}/{board}/cards"
    run_id = call("POST", path, {"kind": "agent-session", "goal": "Exercise the chat controls", "agent": "demo", "simulateFailures": 3})["id"]

    def send(**body):
        return call("POST", f"/api/cards/{system}/{run_id}/input", body)

    def wait(state):
        deadline = time.time() + 90
        while time.time() < deadline:
            card = next((c for c in call("GET", path)["cards"] if c["id"] == run_id), None)
            if card and card["session_state"] == state:
                return card
            if card and card["column"] == "stopped":
                raise AssertionError(card)
            time.sleep(.6)
        raise AssertionError(f"{system} expected {state}, last card: {card}")

    def latest(card, kind):
        events = call("GET", f"/api/cards/{system}/{run_id}/events")["events"]
        return next(e for e in reversed(events) if e["type"] == kind)

    wait("stuck")
    send(type="message", text="continue")
    approval = latest(wait("approval"), "approval")
    send(type="approval", id=approval["id"], optionId="deny")
    card = wait("awaiting_review")
    assert card["column"] == "waiting", "Turn completion must not finish the run"
    send(type="message", text="continue")
    approval = latest(wait("approval"), "approval")
    send(type="approval", id=approval["id"], optionId="allow")
    question = latest(wait("question"), "question")
    send(type="answer", id=question["id"], answers={"style": "brief", "checks": ["tests", "changes"], "other": "Use plain language"})
    wait("working")
    send(type="interrupt")
    wait("awaiting_input")
    send(type="message", text="continue after interruption")
    approval = latest(wait("approval"), "approval")
    send(type="approval", id=approval["id"], optionId="allow")
    question = latest(wait("question"), "question")
    send(type="answer", id=question["id"], answers={"style": "detailed", "checks": ["tests"]})
    wait("awaiting_review")
    send(type="done")
    wait("done")
    print(f"[{system}] PASS: stuck/continue, deny, review, allow, typed question, interrupt, resume, explicit done ({run_id})", flush=True)


if __name__ == "__main__":
    for backend in sys.argv[1:] or ["hatchet", "trigger-dev"]:
        run(backend)
