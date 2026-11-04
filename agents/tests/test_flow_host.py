import json
import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import flow_host
from flowkit import trigger_codegen
from flowkit.activation import Store, revision
from flowkit.definition import FLOWS_DIR

HELLO = (FLOWS_DIR / "hello-flow.yaml").read_text(encoding="utf-8")
NEW = """name: new-flow
steps:
  only:
    kind: program
    run: echo hi
"""


class FakeSupervisor:
    """Records restarts and the revisions each restarted worker would serve."""

    def __init__(self, store):
        self.store, self.fail, self.alive, self.restarts, self.during = store, set(), set(), [], None

    def restart(self):
        self.restarts.append(("all", None))
        self.alive |= {"hatchet", "prefect"}

    def running(self, backend):
        return backend in self.alive

    def restart_many(self, backends, wait):
        if self.during:
            self.during(backends)
        result = {}
        for backend in backends:
            self.restarts.append((backend, self.store.active_revisions(backend)))
            if backend in self.fail:
                self.alive.discard(backend)
                result[backend] = f"the {backend} flow worker exited with code 1"
            else:
                self.alive.add(backend)
                result[backend] = None
        return result


class HostCase(unittest.TestCase):
    backends = ["hatchet", "prefect", "trigger"]

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.drafts, self.state = self.tmp / "flows", self.tmp / "state"
        self.drafts.mkdir()
        (self.drafts / "hello-flow.yaml").write_text(HELLO, encoding="utf-8", newline="\n")
        (self.drafts / "broken-flow.yaml").write_text("name: [unclosed", encoding="utf-8")
        self.target = self.tmp / "flows.generated.ts"
        self.start()

    def start(self):
        self.store = Store(self.drafts, self.state)
        self.supervisor = FakeSupervisor(self.store)
        self.host = flow_host.Host(self.store, self.supervisor, self.backends, ready_seconds=0, trigger_target=self.target)
        self.host.startup()
        self.server = flow_host.serve(self.host, "127.0.0.1", 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def stop(self):
        self.server.shutdown()
        self.server.server_close()

    def tearDown(self):
        self.stop()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def call(self, method, path, body=None):
        request = urllib.request.Request(f"http://127.0.0.1:{self.server.server_port}{path}", method=method,
                                         data=None if body is None else json.dumps(body).encode(),
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status, json.loads(response.read())
        except HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def doc(self, name="hello-flow"):
        status, body = self.call("GET", f"/documents/{name}")
        self.assertEqual(status, 200, body)
        return body

    def save(self, text, expected, name="hello-flow"):
        return self.call("PUT", f"/documents/{name}", {"yaml": text, "expectedRevision": expected})

    def activate(self, name="hello-flow", rev=...):
        return self.call("POST", f"/documents/{name}/activate", {"revision": self.doc(name)["revision"] if rev is ... else rev})

    def active_yaml(self, name="hello-flow"):
        return next((f["yaml"] for f in self.call("GET", "/flows")[1]["flows"] if f["name"] == name), None)


class DraftTests(HostCase):
    def test_migration_activates_valid_drafts_once(self):
        doc = self.doc()
        self.assertEqual((doc["state"], doc["activeRevision"]), ("active", revision(HELLO)))
        self.assertEqual(self.doc("broken-flow")["state"], "broken")
        self.assertEqual(self.active_yaml(), HELLO)
        self.assertEqual(self.supervisor.restarts, [("all", None)])  # the workers' normal start
        self.assertFalse(self.target.exists())  # startup never rewrites the Trigger.dev tasks
        self.assertEqual(self.store.load_active("prefect")[0]["hello-flow"].name, "hello-flow")

    def test_draft_and_layout_saves_have_no_engine_side_effects(self):
        with patch.object(trigger_codegen, "generate", side_effect=AssertionError("codegen")), \
             patch.object(self.supervisor, "restart_many", side_effect=AssertionError("restart")):
            edited = HELLO.replace("Two-step smoke test", "Edited")
            status, doc = self.save(edited, revision(HELLO))
            self.assertEqual((status, doc["state"], doc["revision"]), (200, "changed", revision(edited)))
            layout = {"version": 1, "positions": {"greet": {"x": 1, "y": 2.5}, "stale": {"x": 0, "y": 0}}}
            self.assertEqual(self.call("PUT", "/documents/hello-flow/layout", {"layout": layout}), (200, {"ok": True}))
            self.assertEqual((self.doc()["layout"], self.doc()["revision"]), (layout, revision(edited)))
            self.assertEqual(self.call("POST", "/documents/parse", {"yaml": edited})[0], 200)
            self.assertEqual(self.call("POST", "/documents/serialize", {"document": doc["document"]})[1]["yaml"], edited)
            self.assertEqual(self.call("GET", "/documents/hello-flow/activation")[0], 200)
            self.assertEqual(self.call("PUT", "/flows/hello-flow", {"yaml": HELLO})[1]["ok"], True)  # old API: draft only
            self.assertEqual(self.call("POST", "/validate", {"yaml": HELLO})[1]["ok"], True)
        self.assertEqual(self.supervisor.restarts, [("all", None)])
        self.assertFalse(self.target.exists())
        self.assertEqual(self.active_yaml(), HELLO)

    def test_two_editors_stale_revision_conflicts(self):
        base = self.doc()["revision"]
        first = HELLO.replace("world", "first")
        self.assertEqual(self.save(first, base)[0], 200)
        status, body = self.save(HELLO.replace("world", "second"), base)
        self.assertEqual((status, body["error"], body["current"]["revision"]), (409, "conflict", revision(first)))
        self.assertEqual(self.store.draft("hello-flow")[0], first)
        status, body = self.save(HELLO, None)  # null = create, but it exists
        self.assertEqual((status, body["error"]), (409, "conflict"))

    def test_simultaneous_saves_of_one_revision_accept_exactly_one(self):
        base, results = revision(HELLO), []
        threads = [threading.Thread(target=lambda i=i: results.append(self.save(HELLO.replace("world", f"tab {i}"), base)[0]))
                   for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sorted(results), [200] + [409] * 7)

    def test_external_edit_becomes_a_changed_draft(self):
        edited = HELLO.replace("world", "hand edit")
        (self.drafts / "hello-flow.yaml").write_text(edited, encoding="utf-8", newline="\n")
        doc = self.doc()
        self.assertEqual((doc["state"], doc["revision"], doc["activeRevision"]), ("changed", revision(edited), revision(HELLO)))
        self.assertEqual(self.active_yaml(), HELLO)
        self.assertEqual(self.supervisor.restarts, [("all", None)])

    def test_malformed_yaml_never_replaces_the_draft_or_active_snapshot(self):
        for text in ("name: [unclosed", HELLO.replace("kind: agent", "kind: agent\n    agent: gpt")):
            status, body = self.save(text, revision(HELLO))
            self.assertEqual(status, 400)
            self.assertTrue(body["errors"])
        self.assertEqual(self.store.draft("hello-flow")[0], HELLO)
        (self.drafts / "hello-flow.yaml").write_text("name: [broken by hand", encoding="utf-8")
        doc = self.doc()
        self.assertEqual((doc["state"], doc["document"]), ("broken", None))
        self.assertEqual(self.activate()[0], 400)
        self.assertEqual(self.active_yaml(), HELLO)
        self.assertIn("hello-flow.yaml", self.call("GET", "/flows")[1]["broken"])

    def test_name_must_match_and_layout_needs_a_draft(self):
        status, body = self.save(NEW, None, name="other-flow")
        self.assertEqual((status, body["errors"][-1]["field"]), (400, "name"))
        self.assertEqual(self.call("PUT", "/documents/nope/layout", {"layout": {"version": 1, "positions": {}}})[0], 404)
        self.assertEqual(self.call("PUT", "/documents/hello-flow/layout", {"layout": {"version": 2}})[0], 400)
        self.assertEqual(self.call("GET", "/documents/nope")[0], 404)

    def test_host_restart_keeps_active_snapshots(self):
        edited = HELLO.replace("world", "draft only")
        self.save(edited, revision(HELLO))
        self.stop()
        self.start()
        self.assertEqual(self.active_yaml(), HELLO)
        self.assertEqual(self.doc()["state"], "changed")


class ActivationTests(HostCase):
    def test_activation_applies_each_engine_once(self):
        edited = HELLO.replace("world", "activated")
        self.save(edited, revision(HELLO))
        status, preview = self.call("GET", "/documents/hello-flow/activation")
        self.assertEqual((preview["change"], [e["action"] for e in preview["engines"]]),
                         ("update", ["reload worker", "reload worker", "regenerate tasks"]))
        status, result = self.activate()
        self.assertEqual(status, 200, result)
        self.assertEqual([(e["engine"], e["state"]) for e in result["engines"]],
                         [("hatchet", "active"), ("prefect", "active"), ("trigger", "pending")])
        self.assertEqual(self.supervisor.restarts[1:], [(b, {"hello-flow": revision(edited)}) for b in ("hatchet", "prefect")])
        self.assertIn("activated", self.target.read_text(encoding="utf-8"))
        self.assertEqual((self.active_yaml(), self.doc()["state"]), (edited, "active"))
        before = self.target.stat().st_mtime_ns
        status, again = self.activate()  # idempotent retry
        self.assertEqual([e["state"] for e in again["engines"]], ["active", "active", "pending"])
        self.assertEqual(len(self.supervisor.restarts), 3)
        self.assertEqual(self.target.stat().st_mtime_ns, before)
        self.assertEqual([e["action"] for e in self.call("GET", "/documents/hello-flow/activation")[1]["engines"]], ["none"] * 3)

    def test_stale_activation_is_a_conflict(self):
        status, body = self.activate(rev="sha256:old")
        self.assertEqual((status, body["error"], body["preview"]["revision"]), (409, "conflict", revision(HELLO)))
        self.assertEqual(self.supervisor.restarts, [("all", None)])

    def test_partial_failure_keeps_the_old_version_and_retry_completes(self):
        edited = HELLO.replace("world", "v2")
        self.save(edited, revision(HELLO))
        self.supervisor.fail = {"prefect"}
        result = self.activate()[1]
        self.assertEqual([(e["engine"], e["state"]) for e in result["engines"]],
                         [("hatchet", "active"), ("prefect", "failed"), ("trigger", "pending")])
        self.assertIn("exited", result["engines"][1]["error"])
        self.assertEqual(self.store.active_revisions("prefect"), {"hello-flow": revision(HELLO)})
        self.assertEqual(self.supervisor.restarts[-1], ("prefect", {"hello-flow": revision(HELLO)}))  # back on the old version
        preview = self.call("GET", "/documents/hello-flow/activation")[1]
        self.assertEqual([e["action"] for e in preview["engines"]], ["none", "reload worker", "none"])
        self.supervisor.fail = set()
        restarts = len(self.supervisor.restarts)
        result = self.activate()[1]
        self.assertEqual([e["state"] for e in result["engines"]], ["active", "active", "pending"])
        self.assertEqual([b for b, _ in self.supervisor.restarts[restarts:]], ["prefect"])
        self.assertEqual(self.store.active_revisions("prefect"), {"hello-flow": revision(edited)})

    def test_create_activate_and_remove(self):
        self.assertEqual(self.save(NEW, None, name="new-flow")[1]["state"], "inactive")
        self.assertIsNone(self.active_yaml("new-flow"))
        self.assertEqual(self.call("GET", "/documents/new-flow/activation")[1]["change"], "add")
        self.assertEqual(self.activate("new-flow")[0], 200)
        self.assertEqual(self.active_yaml("new-flow"), NEW)
        self.assertEqual(self.call("DELETE", "/documents/new-flow?revision=sha256:stale")[0], 409)
        self.assertEqual(self.call("DELETE", f"/documents/new-flow?revision={revision(NEW)}"), (200, {"ok": True}))
        doc = self.doc("new-flow")
        self.assertEqual((doc["state"], doc["revision"], doc["yaml"]), ("removed", None, NEW))
        self.assertEqual(self.active_yaml("new-flow"), NEW)  # still launchable until the removal is activated
        self.assertEqual(self.call("GET", "/documents/new-flow/activation")[1]["change"], "remove")
        status, result = self.activate("new-flow", None)
        self.assertEqual((status, result["activeRevision"]), (200, None))
        self.assertIsNone(self.active_yaml("new-flow"))
        self.assertEqual(self.call("GET", "/documents/new-flow")[0], 404)
        self.assertNotIn("new-flow", self.target.read_text(encoding="utf-8"))

    def test_old_delete_is_draft_only(self):
        self.assertEqual(self.call("DELETE", "/flows/hello-flow"), (200, {"ok": True}))
        self.assertEqual(self.active_yaml(), HELLO)
        self.assertEqual(self.doc()["state"], "removed")
        self.assertEqual(self.supervisor.restarts, [("all", None)])


class RegressionTests(HostCase):
    def test_non_utf8_draft_is_broken_and_never_activated(self):
        (self.drafts / "cp-flow.yaml").write_bytes("name: cp-flow\ndescription: café\nsteps:\n  a: {kind: program, run: echo}\n".encode("cp1252"))
        doc = self.doc("cp-flow")
        self.assertEqual(doc["state"], "broken")
        self.assertIn("not UTF-8", doc["errors"][0]["message"])
        status, body = self.activate("cp-flow")
        self.assertEqual((status, "not UTF-8" in body["errors"][0]["message"]), (400, True))
        self.assertNotIn("cp-flow", self.store.manifest()["flows"])
        self.assertEqual(self.call("GET", "/documents")[0], 200)
        self.assertIn("cp-flow.yaml", self.call("GET", "/flows")[1]["broken"])
        other = Store(self.drafts, self.tmp / "fresh")
        self.assertNotIn("cp-flow", other.migrate())  # migration skips it too

    def test_engine_failures_are_reported_not_raised(self):
        self.save(HELLO.replace("world", "boom"), revision(HELLO))
        with patch.object(trigger_codegen, "generate", side_effect=KeyError("x")), \
             patch.object(self.supervisor, "restart_many", side_effect=RuntimeError("no processes")):
            status, result = self.activate()
        self.assertEqual(status, 200, result)
        self.assertEqual([e["state"] for e in result["engines"]], ["failed"] * 3)
        self.assertEqual(self.store.active_revisions("hatchet"), {"hello-flow": revision(HELLO)})
        self.assertEqual(self.store.active_revisions("trigger"), {"hello-flow": revision(HELLO)})

    def test_aliases_are_rejected_quickly_and_parsing_happens_outside_the_lock(self):
        lines = ["name: hello-flow", "a0: &a0 [lol, lol, lol, lol, lol, lol, lol, lol, lol, lol]"]
        lines += [f"a{i}: &a{i} [" + ", ".join([f"*a{i - 1}"] * 10) + "]" for i in range(1, 7)]
        bomb = "\n".join(lines) + "\nsteps:\n  s: {kind: program, run: echo, worker: *a6}\n"
        started = time.monotonic()
        status, body = self.save(bomb, revision(HELLO))
        self.assertEqual(status, 400)
        self.assertIn("aliases", body["errors"][0]["message"])
        self.assertLess(time.monotonic() - started, 5)
        entered, release = threading.Event(), threading.Event()
        real = flow_host.document.analyze

        def slow(*args):
            entered.set()
            release.wait(10)
            return real(*args)
        with patch.object(flow_host.document, "analyze", side_effect=slow):
            saver = threading.Thread(target=self.save, args=(HELLO.replace("world", "slow"), revision(HELLO)))
            saver.start()
            self.assertTrue(entered.wait(5))
            # a slow parse does not hold the draft lock
            self.assertEqual(self.call("PUT", "/documents/hello-flow/layout", {"layout": {"version": 1, "positions": {}}})[0], 200)
            release.set()
            saver.join()

    def test_surrogate_escapes_do_not_break_responses(self):
        text = HELLO.replace("Two-step smoke test", '"smile \\ud83d\\ude00"')
        (self.drafts / "hello-flow.yaml").write_text(text, encoding="utf-8", newline="\n")
        doc = self.doc()
        self.assertEqual(doc["state"], "broken")
        self.assertIn("surrogate", doc["errors"][0]["message"])
        self.assertEqual(self.call("GET", "/flows")[0], 200)
        status, body = self.call("POST", "/documents/parse", {"yaml": "name: \ud83d"})
        self.assertEqual((status, body["document"]), (200, None))

    def test_saves_do_not_wait_for_worker_restarts(self):
        self.save(HELLO.replace("world", "v2"), revision(HELLO))
        restarting, release = threading.Event(), threading.Event()
        trigger_text = []

        def during(backends):
            trigger_text.append(self.target.read_text(encoding="utf-8"))  # Trigger.dev is applied first
            restarting.set()
            release.wait(10)
        self.supervisor.during = during
        worker = threading.Thread(target=self.activate)
        worker.start()
        self.assertTrue(restarting.wait(5))
        status, _ = self.save(HELLO.replace("world", "v3"), revision(HELLO.replace("world", "v2")))
        self.assertEqual(status, 200)
        release.set()
        worker.join()
        self.assertIn("v2", trigger_text[0])
        self.assertEqual([b for b, _ in self.supervisor.restarts[1:]], ["hatchet", "prefect"])

    def test_flows_still_served_after_a_failed_removal_are_in_the_preview(self):
        self.save(NEW, None, name="new-flow")
        self.activate("new-flow")
        self.call("DELETE", f"/documents/new-flow?revision={revision(NEW)}")
        self.supervisor.fail = {"prefect"}
        result = self.activate("new-flow", None)[1]
        self.assertEqual(result["engines"][1]["state"], "failed")
        self.assertIsNone(self.active_yaml("new-flow"))  # not launchable any more
        self.assertIn("new-flow", self.store.active_revisions("prefect"))  # but Prefect still serves it
        served = self.call("GET", "/documents/hello-flow/activation")[1]["served"]
        self.assertEqual(set(served), {"hello-flow", "new-flow"})
        self.assertEqual(served["new-flow"]["steps"]["only"]["timeout_minutes"], 60)

    def test_damaged_snapshot_of_a_removed_flow_is_reported_broken(self):
        self.save(NEW, None, name="new-flow")
        self.activate("new-flow")
        self.call("DELETE", f"/documents/new-flow?revision={revision(NEW)}")
        self.store.snapshot_path("new-flow", revision(NEW)).unlink()
        status, body = self.call("GET", "/documents")
        self.assertEqual(status, 200)
        states = {d["name"]: d["state"] for d in body["documents"]}
        self.assertEqual((states["new-flow"], states["hello-flow"]), ("broken", "active"))

    def test_stale_generated_tasks_at_startup_are_pending(self):
        self.assertEqual(self.store.manifest()["flows"]["hello-flow"]["engines"]["trigger"]["state"], "pending")
        self.assertTrue(Store(Path("relative-state")).state.is_absolute())

    def test_legacy_save_validates_and_checks_a_sent_revision(self):
        bad = HELLO.replace("{{ steps.greet.output.stdout }}", "{{ steps.nope.output }}")
        self.assertEqual(self.call("PUT", "/flows/hello-flow", {"yaml": bad})[0], 400)
        edited = HELLO.replace("world", "legacy")
        status, body = self.call("PUT", "/flows/hello-flow", {"yaml": edited, "expectedRevision": "sha256:stale"})
        self.assertEqual((status, body["error"]), (409, "conflict"))
        self.assertEqual(self.call("PUT", "/flows/hello-flow", {"yaml": edited, "expectedRevision": revision(HELLO)})[0], 200)
        self.assertEqual(self.call("PUT", "/flows/hello-flow", {"yaml": HELLO})[0], 200)  # the old dialog sends none

    def test_writes_need_a_json_content_type(self):
        request = urllib.request.Request(f"http://127.0.0.1:{self.server.server_port}/documents/hello-flow/activate", method="POST",
                                         data=json.dumps({"revision": revision(HELLO)}).encode(), headers={"Content-Type": "text/plain"})
        with self.assertRaises(HTTPError) as raised:
            urllib.request.urlopen(request, timeout=10)
        self.assertEqual(raised.exception.code, 415)
        self.assertEqual(self.supervisor.restarts, [("all", None)])


class UnavailableEngineTests(HostCase):
    backends = ["hatchet"]

    def test_disabled_engines_are_reported(self):
        self.save(HELLO.replace("world", "x"), revision(HELLO))
        preview = self.call("GET", "/documents/hello-flow/activation")[1]
        self.assertEqual([(e["engine"], e["action"], e["enabled"]) for e in preview["engines"]],
                         [("hatchet", "reload worker", True), ("prefect", "none", False), ("trigger", "none", False)])
        result = self.activate()[1]
        self.assertEqual([e["state"] for e in result["engines"]], ["active", "unavailable", "unavailable"])
        self.assertFalse(self.target.exists())


class SupervisorTests(unittest.TestCase):
    def test_worker_gets_pinned_revisions_and_readiness_is_checked(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            (tmp / "flows").mkdir()
            (tmp / "flows" / "hello-flow.yaml").write_text(HELLO, encoding="utf-8", newline="\n")
            store = Store(tmp / "flows", tmp / "state")
            store.migrate()
            out = tmp / "seen.json"
            script = tmp / "worker.py"
            script.write_text("import os, sys, time\nfrom pathlib import Path\n"
                              f"Path({str(out)!r}).write_text(os.environ['FLOW_ACTIVE_REVISIONS'])\n"
                              "sys.exit(3) if os.environ.get('FAIL_WORKER') else time.sleep(60)\n", encoding="utf-8")
            supervisor = flow_host.Supervisor(["hatchet"], store)
            with patch.dict(flow_host.WORKERS, {"hatchet": str(script)}):
                try:
                    self.assertEqual(supervisor.restart_many(["hatchet"], 1), {"hatchet": None})
                    self.assertTrue(supervisor.running("hatchet"))
                    for _ in range(300):  # the child may still be starting on a busy machine
                        if out.exists() and out.read_text():
                            break
                        time.sleep(0.1)
                    self.assertEqual(json.loads(out.read_text()), {"hello-flow": revision(HELLO)})
                    with patch.dict(os.environ, {"FAIL_WORKER": "1"}):
                        self.assertIn("exited with code 3", supervisor.restart_one("hatchet", 10))
                    with patch.dict(os.environ, {"FLOW_ACTIVE_REVISIONS": "{}"}):
                        self.assertEqual(store.load_active("hatchet")[0], {})  # a worker serves what it was started with
                finally:
                    supervisor.stop()


if __name__ == "__main__":
    unittest.main()
