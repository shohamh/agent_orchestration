"""Drive one live coding-agent card through the board API the way a person would, and print the chat.

  python live_demo_driver.py <system> <board> <run_id> [--continue-text "..."]

Approves each permission request with its allow-once option (the card is a disposable demo
project), sends one "continue" after the first review to show the done-vs-continue loop, then
marks the session done. Everything else (questions, stuck) is printed and left to the person.
"""

import argparse
import json
import time
import urllib.request

import board_http

BASE = "http://localhost:8090"


def call(method, path, body=None):
    request = urllib.request.Request(BASE + path, method=method,
        data=json.dumps(body).encode() if body is not None else None, headers={"Content-Type": "application/json"})
    with board_http.urlopen(request, timeout=60) as response:
        return json.loads(response.read())


def card(system, board, run_id):
    return next((c for c in call("GET", f"/api/boards/{system}/{board}/cards")["cards"] if c["id"] == run_id), None)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("system")
    parser.add_argument("board")
    parser.add_argument("run_id")
    parser.add_argument("--continue-text", default="continue: also add a test for an empty cart, then rerun the tests")
    args = parser.parse_args()
    send = lambda body: call("POST", f"/api/cards/{args.system}/{args.run_id}/input", body)
    answered, continued, printed = set(), False, 0
    deadline = time.time() + 900
    while time.time() < deadline:
        current = card(args.system, args.board, args.run_id)
        if not current:
            time.sleep(2)
            continue
        events = current["events"]
        for event in events[printed:]:
            kind = event["type"]
            if kind in ("user", "status", "approval", "approval_resolved", "question", "error") or (kind == "tool" and event.get("status") != "running"):
                detail = event.get("text") or event.get("summary") or event.get("reason") or event.get("message") or event.get("decision") or ""
                print(f"{event['seq']:>4} {kind:<17} {event.get('state', '')} {str(detail)[:140]}".rstrip(), flush=True)
        printed = len(events)
        resolved = {e["id"] for e in events if e["type"] == "approval_resolved"}
        for event in events:
            if event["type"] == "approval" and event["id"] not in resolved and event["id"] not in answered:
                option = next(o for o in event["options"] if o.get("kind") == "allow_once")
                send({"type": "approval", "id": event["id"], "optionId": option["id"]})
                answered.add(event["id"])
        state = current.get("session_state")
        if current["column"] in ("done", "stopped"):
            print(f"FINAL column={current['column']} state={state}")
            return
        if state == "awaiting_review":
            if not continued:
                send({"type": "message", "text": args.continue_text})
                continued = True
            else:
                send({"type": "done"})
        if state in ("stuck", "question"):
            print(f"Waiting for a person: {state}")
            return
        time.sleep(2)
    print("timed out")


if __name__ == "__main__":
    main()
