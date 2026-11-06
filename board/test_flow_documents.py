"""Workflow builder endpoints: activation impact, confirmation and the engines' active-run inventory."""
import asyncio
import copy
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent))
import aiohttp
from aiohttp.test_utils import TestClient, TestServer
import server

PREVIEW = {"name": "hello-flow", "revision": "sha256:new", "activeRevision": "sha256:old", "change": "update", "errors": [],
           "served": {"hello-flow": {"steps": {}}, "other-flow": {"steps": {}}},
           "engines": [{"engine": "hatchet", "action": "reload worker", "enabled": True},
                       {"engine": "prefect", "action": "reload worker", "enabled": True},
                       {"engine": "trigger", "action": "none", "enabled": True}]}


class FakeHost:
    def __init__(self):
        self.preview, self.activated, self.deactivated = copy.deepcopy(PREVIEW), [], []
        self.result = {"name": "hello-flow", "activeRevision": "sha256:new",
                       "engines": [{"engine": e, "state": "active", "error": None} for e in ("hatchet", "prefect", "trigger")]}

    async def deactivate(self, name, revision):
        self.deactivated.append((name, revision))
        return 200, {"name": name, "activeRevision": None, "engines": []}

    async def activation(self, name, disable=False):
        return (200, copy.deepcopy(self.preview)) if name == "hello-flow" else (404, {"error": "no such flow"})

    async def activate(self, name, revision):
        self.activated.append((name, revision))
        return 200, self.result

    async def flows(self):
        return {"hello-flow": {}, "other-flow": {}}

    async def save_document(self, name, text, expected):
        return 409, {"error": "conflict", "current": {"name": name, "revision": "sha256:theirs"}}

    async def parse_document(self, text):
        return 200, {"document": None, "errors": [{"message": "Not valid YAML", "step": None, "field": None}], "lossless": False}


class Engine:
    def __init__(self, system, runs=()):
        self.system, self.runs, self.asked = system, list(runs), []

    async def active_runs(self, flows):
        self.asked.append(flows)
        if isinstance(self.runs, Exception):
            raise self.runs
        return list(self.runs)


def run(system, run_id, status="RUNNING"):
    return {"system": system, "board": "other-board", "id": run_id, "title": run_id, "status": status}


class ActivationAPI(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engines = {"hatchet": Engine("hatchet"), "prefect": Engine("prefect"), "trigger-dev": Engine("trigger-dev")}
        with patch.object(server, "load_adapters", return_value=self.engines):
            self.app = server.make_app()
        self.host = self.app["flow_host"] = FakeHost()
        self.client = TestClient(TestServer(self.app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def preview(self):
        response = await self.client.get("/api/flow-documents/hello-flow/activation")
        self.assertEqual(response.status, 200, await response.text())
        return await response.json()

    async def activate(self, preview, **extra):
        response = await self.client.post("/api/flow-documents/hello-flow/activate",
                                          json={"revision": preview["revision"], "impact": preview["impact"], **extra})
        return response.status, await response.json()

    async def test_disabling_rechecks_the_impact_and_keeps_to_the_active_revision(self):
        self.host.preview["activeRevision"] = "sha256:old"
        preview = await (await self.client.get("/api/flow-documents/hello-flow/activation?disable=1")).json()
        post = lambda **body: self.client.post("/api/flow-documents/hello-flow/deactivate", json=body)
        self.assertEqual((await post(revision="sha256:stale", impact=preview["impact"], confirm=True)).status, 409)
        self.assertEqual((await post(revision="sha256:old", impact="sha256:stale", confirm=True)).status, 409)
        self.assertEqual(self.host.deactivated, [])
        response = await post(revision="sha256:old", impact=preview["impact"], confirm=True)
        self.assertEqual(response.status, 200)
        self.assertEqual(self.host.deactivated, [("hello-flow", "sha256:old")])

    async def test_quiet_engines_activate_without_confirmation(self):
        preview = await self.preview()
        self.assertFalse(preview["disruptive"])
        self.assertEqual([(e["engine"], e["available"], e["activeRuns"]) for e in preview["engines"]],
                         [("hatchet", True, []), ("prefect", True, []), ("trigger", True, [])])
        self.assertEqual(set(self.engines["hatchet"].asked[0]), {"hello-flow", "other-flow"})  # every board, every served flow
        status, result = await self.activate(preview)
        self.assertEqual((status, result["activeRevision"]), (200, "sha256:new"))
        self.assertEqual(self.host.activated, [("hello-flow", "sha256:new")])

    async def test_disruptive_activation_requires_confirmation(self):
        self.engines["hatchet"].runs = [run("hatchet", "h-1")]
        preview = await self.preview()
        self.assertTrue(preview["disruptive"])
        self.assertEqual(preview["engines"][0]["activeRuns"], [run("hatchet", "h-1")])
        status, body = await self.activate(preview)
        self.assertEqual((status, body["error"]), (409, "confirmation required"))
        status, body = await self.activate(preview, confirm="yes")
        self.assertEqual((status, body["error"]), (409, "confirmation required"))
        self.assertEqual(self.host.activated, [])
        status, _ = await self.activate(preview, confirm=True)
        self.assertEqual(status, 200)
        self.assertEqual(self.host.activated, [("hello-flow", "sha256:new")])

    async def test_run_appearing_between_preview_and_apply_changes_the_impact(self):
        preview = await self.preview()
        self.engines["prefect"].runs = [run("prefect", "p-1", "PENDING")]
        status, body = await self.activate(preview, confirm=True)
        self.assertEqual((status, body["error"]), (409, "impact changed"))
        self.assertEqual(body["preview"]["engines"][1]["activeRuns"][0]["id"], "p-1")
        self.assertEqual(self.host.activated, [])
        status, _ = await self.activate(body["preview"], confirm=True)
        self.assertEqual(status, 200)

    async def test_unknown_engine_state_is_disruptive_and_unaffected_engines_are_not(self):
        self.engines["trigger-dev"].runs = [run("trigger-dev", "t-1")]  # its tasks do not change
        self.assertFalse((await self.preview())["disruptive"])
        self.engines["prefect"].runs = RuntimeError("Prefect is down")
        preview = await self.preview()
        self.assertEqual((preview["engines"][1]["available"], preview["engines"][1]["activeRuns"]), (False, None))
        self.assertTrue(preview["disruptive"])
        self.assertEqual((await self.activate(preview))[1]["error"], "confirmation required")
        del self.engines["hatchet"]  # not configured on this board
        preview = await self.preview()
        self.assertEqual((preview["engines"][0]["available"], preview["engines"][0]["activeRuns"]), (False, None))

    async def test_stale_revision_and_missing_flow(self):
        preview = await self.preview()
        self.host.preview["revision"] = "sha256:newer"
        status, body = await self.activate(preview, confirm=True)
        self.assertEqual((status, body["error"], body["preview"]["revision"]), (409, "conflict", "sha256:newer"))
        response = await self.client.get("/api/flow-documents/nope/activation")
        self.assertEqual(response.status, 404)
        self.assertEqual(self.host.activated, [])

    async def test_host_responses_pass_through(self):
        response = await self.client.put("/api/flow-documents/hello-flow", json={"yaml": "name: hello-flow", "expectedRevision": "sha256:mine"})
        self.assertEqual((response.status, (await response.json())["current"]["revision"]), (409, "sha256:theirs"))
        response = await self.client.post("/api/flow-documents/parse", json={"yaml": "name: ["})
        self.assertEqual((response.status, (await response.json())["document"]), (200, None))
        response = await self.client.put("/api/flow-documents/hello-flow", json={"yaml": "x", "expectedRevision": 3})
        self.assertEqual(response.status, 400)


class InventoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefect_lists_unfinished_portable_flow_runs(self):
        from prefect_adapter import PrefectAdapter
        adapter = PrefectAdapter()
        adapter._request = AsyncMock(return_value=[{"id": "r1", "name": "Flow run", "tags": ["portable-flow", "board:b"], "state_name": "Running"}])
        self.assertEqual(await adapter.active_runs({"x": {}}), [{"system": "prefect", "board": "b", "id": "r1", "title": "Flow run", "status": "Running"}])
        body = adapter._request.await_args.args[2]
        self.assertEqual(body["flow_runs"]["tags"], {"all_": ["portable-flow"]})
        self.assertIn("RUNNING", body["flow_runs"]["state"]["type"]["any_"])

    async def test_trigger_dev_lists_every_active_run_across_pages(self):
        from trigger_adapter import TriggerAdapter
        with patch.dict(os.environ, {"TRIGGER_DEV_SECRET_KEY": "test", "TRIGGER_SECRET_KEY": "test"}):
            adapter, production = TriggerAdapter(development=True), TriggerAdapter()
        pages = [{"data": [{"id": "a", "taskIdentifier": "agent-session", "status": "EXECUTING", "tags": ["board:x"], "metadata": {"title": "Chat"}}],
                  "pagination": {"next": "c1"}},
                 {"data": [{"id": "b", "taskIdentifier": "flow-hello-flow", "status": "QUEUED", "tags": []}], "pagination": {}}]
        adapter._request = AsyncMock(side_effect=pages)
        found = await adapter.active_runs({})
        self.assertEqual([(r["id"], r["board"], r["title"]) for r in found], [("a", "x", "Chat"), ("b", None, "flow-hello-flow")])
        statuses = adapter._request.await_args_list[0].kwargs["params"]["filter[status]"].split(",")
        self.assertTrue({"EXECUTING", "QUEUED", "DELAYED", "WAITING"} <= set(statuses))
        self.assertEqual(adapter._request.await_args_list[1].kwargs["params"]["page[after]"], "c1")
        self.assertEqual(await production.active_runs({}), [])



class HatchetInventoryTests(unittest.TestCase):
    def test_hatchet_filters_to_flow_workflows(self):  # sync: importing the Hatchet SDK is slow
        import asyncio
        from types import SimpleNamespace
        from hatchet_adapter import HatchetAdapter, V1TaskStatus
        rows = [SimpleNamespace(workflow_name=name, display_name=name, workflow_run_external_id=name + "-run", status=V1TaskStatus.RUNNING,
                                additional_metadata={"board": "b", "title": "T"}) for name in ("hello-flow", "steerable-agent")]
        asked = []

        def listing(**kw):
            asked.append(kw)
            return SimpleNamespace(rows=rows)
        adapter = HatchetAdapter.__new__(HatchetAdapter)
        adapter.hatchet = SimpleNamespace(runs=SimpleNamespace(list=listing))
        short = {"steps": {"a": {"needs": [], "retries": 0, "timeout_minutes": 60}}}
        self.assertEqual(asyncio.run(adapter.active_runs({"hello-flow": short})),
                         [{"system": "hatchet", "board": "b", "id": "hello-flow-run", "title": "T", "status": "RUNNING"}])
        from datetime import datetime, timedelta, timezone
        self.assertLess(datetime.now(timezone.utc) - asked[0]["since"], timedelta(days=7))  # no SDK RuntimeWarning
        long = {"steps": {"a": {"needs": [], "retries": 5, "timeout_minutes": 1440},
                          "b": {"needs": ["a"], "retries": 0, "timeout_minutes": 1440}}}
        self.assertIsNone(asyncio.run(adapter.active_runs({"hello-flow": short, "slow-flow": long})))  # could hide older runs
        rows[:] = rows * 250  # 500 rows: the limit may hide more
        self.assertIsNone(asyncio.run(adapter.active_runs({"hello-flow": short})))


class ClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        from aiohttp import web
        self.seen = []

        async def handler(request):
            self.seen.append((request.method, request.path, request.content_type))
            if request.path == "/slow":
                await asyncio.sleep(2)
            if request.path == "/flows":
                return web.json_response({"error": "boom"}, status=500)
            return web.json_response({"error": "conflict", "current": None}, status=409)
        app = web.Application()
        app.router.add_route("*", "/{tail:.*}", handler)
        self.server = TestServer(app)
        await self.server.start_server()
        with patch.dict(os.environ, {"FLOW_HOST_URL": str(self.server.make_url("")).rstrip("/")}):
            from flows_client import FlowHost
            self.host = FlowHost()

    async def asyncTearDown(self):
        await self.host.close()
        await self.server.close()

    async def test_timeouts_say_busy_and_errors_never_break_card_labels(self):
        with self.assertRaisesRegex(RuntimeError, "busy"):
            await self.host._call("GET", "/slow", timeout=0.3)
        self.assertEqual(await self.host.flows(), {})  # a 500 from the flow host

    async def test_a_connection_timeout_says_not_reachable_not_busy(self):
        # Docker Desktop leaves connections to a closed host port hanging, so a stopped flow host looks like this.
        with patch.object(aiohttp.ClientSession, "request", side_effect=aiohttp.ConnectionTimeoutError("no connection")):
            with self.assertRaisesRegex(RuntimeError, "not reachable.*Start it"):
                await self.host._call("GET", "/flows")

    async def test_writes_are_json_and_legacy_save_passes_conflicts_through(self):
        self.assertEqual((await self.host.save("a-b", "x", "sha256:old"))[0], 409)
        await self.host.delete_document("a-b", "sha256:old")
        self.assertEqual([c for _, _, c in self.seen], ["application/json"] * 2)


class LegacySaveTests(unittest.IsolatedAsyncioTestCase):
    async def test_board_forwards_the_revision_and_the_conflict(self):
        with patch.object(server, "load_adapters", return_value={}):
            app = server.make_app()
        app["flow_host"].save = AsyncMock(return_value=(409, {"error": "conflict", "current": None}))
        async with TestClient(TestServer(app)) as client:
            response = await client.put("/api/flows/hello-flow", json={"yaml": "name: hello-flow", "expectedRevision": "sha256:x"})
            self.assertEqual(response.status, 409)
            app["flow_host"].save.assert_awaited_once_with("hello-flow", "name: hello-flow", "sha256:x")
            await client.put("/api/flows/hello-flow", json={"yaml": "name: hello-flow"})
            self.assertIs(app["flow_host"].save.await_args.args[2], ...)


if __name__ == "__main__":
    unittest.main()
