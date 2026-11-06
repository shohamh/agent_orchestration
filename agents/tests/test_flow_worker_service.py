"""The flow host driving flow workers that run in their own containers."""
import os
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import flow_host
import flow_worker_service


class FakeSupervisor:
    def __init__(self):
        self.up, self.revisions, self.restarts, self.error = True, {"hatchet": {"a": "sha256:1"}}, [], None

    def running(self, backend):
        return self.up

    def restart_one(self, backend, wait):
        self.restarts.append((backend, wait))
        return self.error


class FakeStore:
    def active_revisions(self, engine):
        return {"a": "sha256:1"}


class RemoteSupervisorTests(unittest.TestCase):
    def setUp(self):
        self.worker = FakeSupervisor()
        self.server = flow_worker_service.serve(self.worker, "hatchet", "secret", "127.0.0.1", 0)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        patcher = patch.dict(os.environ, {"FLOW_HOST_TOKEN": "secret"})
        patcher.start()
        self.addCleanup(patcher.stop)
        url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.remote = flow_host.RemoteSupervisor({"hatchet": url, "prefect": "http://127.0.0.1:9"}, ["hatchet", "prefect", "trigger"], FakeStore())

    def test_only_enabled_engines_with_a_service_are_supervised(self):
        self.assertEqual(self.remote.backends, ["hatchet", "prefect"])

    def test_restart_passes_the_wait_and_returns_the_workers_error(self):
        self.worker.error = "the hatchet flow worker exited with code 1; see the flow host log"
        self.assertEqual(self.remote.restart_many(["hatchet"], 2), {"hatchet": self.worker.error})
        self.assertEqual(self.worker.restarts, [("hatchet", 2)])

    def test_a_stopped_container_is_an_activation_error_not_a_crash(self):
        error = self.remote.restart_many(["prefect"], 0)["prefect"]
        self.assertIn("compose profile", error)
        self.assertFalse(self.remote.running("prefect"))

    def test_running_reads_the_state(self):
        self.assertTrue(self.remote.running("hatchet"))
        self.worker.up = False
        self.assertFalse(self.remote.running("hatchet"))

    def test_the_wrong_token_is_refused(self):
        with patch.dict(os.environ, {"FLOW_HOST_TOKEN": "wrong"}):
            self.assertFalse(self.remote.running("hatchet"))
            self.assertIn("did not answer", self.remote.restart_many(["hatchet"], 0)["hatchet"])
        self.assertEqual(self.worker.restarts, [])


if __name__ == "__main__":
    unittest.main()
