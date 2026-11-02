"""Run a real isolated agent session, or a simulated interaction, outside an orchestrator.

Use --approve-demo only for this throwaway demo: it automatically selects allow_once.
The dashboard always waits for a human to choose permissions and answer questions.
"""

import argparse
import asyncio
import json

from agentdriver.session import run_session


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent", choices=["codex", "claude", "demo", "opencode", "omp", "pi"], default="demo")
    parser.add_argument("--approve-demo", action="store_true")
    args = parser.parse_args()
    inbox = asyncio.Queue()

    async def sink(event):
        print(json.dumps(event, ensure_ascii=True), flush=True)
        if event["type"] == "approval" and args.approve_demo:
            option = next((o for o in event["options"] if o["kind"] == "allow_once"), None)
            if option:
                await inbox.put({"type": "approval", "id": event["id"], "optionId": option["id"]})
        elif event["type"] == "question" and args.agent == "demo":
            await inbox.put({"type": "answer", "id": event["id"], "answers": {"style": "brief", "checks": ["tests"]}})
        elif event["type"] == "status" and event["state"] == "awaiting_review":
            await inbox.put({"type": "done"})
        elif event["type"] == "status" and event["state"] == "stuck":
            raise RuntimeError("Agent stuck; see error events above")

    await run_session({"agent": args.agent, "goal":
        "This is an isolated demo project. Inspect cart.py and test_cart.py, reproduce the failing test with python -m unittest -v, "
        "fix only cart.py, rerun the tests, and report the result. Do not install packages, access other folders, or run git commands."}, inbox.get, sink)


if __name__ == "__main__":
    asyncio.run(main())
