"""SDK inventory and opt-in probes against a disposable Hatchet installation only.

Set FLEET_HATCHET_ISOLATED=1, HATCHET_CLIENT_TOKEN, HATCHET_CLIENT_HOST_PORT,
HATCHET_CLIENT_SERVER_URL and HATCHET_CLIENT_TLS_STRATEGY for an isolated stack.
The live suite creates workflows, runs and workers, and takes over five minutes.
"""
import inspect
import os
import subprocess
import sys
import tempfile
import time
import unittest
import uuid
from datetime import timedelta
from importlib.metadata import version
from pathlib import Path

from hatchet_sdk import Hatchet
from hatchet_sdk.runnables.workflow import Workflow
from hatchet_sdk.types.labels import DesiredWorkerLabel
from hatchet_sdk.types.trigger import TriggerWorkflowOptions
from hatchet_sdk.clients.rest.models.update_worker_request import UpdateWorkerRequest
from hatchet_sdk.worker.worker import Worker as WorkerRuntime


class HatchetSdkSurface(unittest.TestCase):
    def test_pinned_sdk_and_task_scheduling_surface(self):
        self.assertEqual(version("hatchet-sdk"), "1.44.0")
        task = inspect.signature(Workflow.task).parameters
        self.assertEqual(task["schedule_timeout"].default, timedelta(minutes=5))
        self.assertIn("desired_worker_labels", task)
        self.assertIn("labels", inspect.signature(Hatchet.worker).parameters)

    def test_label_matching_is_not_required_by_default(self):
        self.assertFalse(DesiredWorkerLabel(value="pc").required)
        self.assertTrue(DesiredWorkerLabel(value="pc", required=True).required)

    def test_run_selector_surface(self):
        run = inspect.signature(Workflow.aio_run).parameters
        self.assertIn("desired_worker_id", run)
        self.assertIn("desired_worker_labels", run)
        self.assertIn("desired_worker_label", TriggerWorkflowOptions.model_fields)

    def test_no_public_in_place_label_update_in_pinned_sdk(self):
        self.assertEqual(set(UpdateWorkerRequest.model_fields), {"is_paused"})
        self.assertIsNone(WorkerRuntime.labels.fset)
        self.assertFalse(any("label" in name and "update" in name for name in dir(WorkerRuntime)))


def make_workflow(client, name):
    workflow = client.workflow(name=name)

    @workflow.task(name="probe", schedule_timeout=timedelta(minutes=8),
                   desired_worker_labels=[DesiredWorkerLabel(key="kind", value="eligible", required=True)])
    def probe(input, ctx):
        marker = os.environ["FLEET_PROBE_MARKER"]
        with open(marker, "a", encoding="utf-8") as stream:
            stream.write(os.environ["FLEET_PROBE_WORKER"] + "\n")
        return {"worker": os.environ["FLEET_PROBE_WORKER"]}

    return workflow


def make_two_step_workflow(client, name):
    workflow = client.workflow(name=name + "-two")

    def task_for(step, station, parents=None):
        @workflow.task(name=step, parents=parents or [], schedule_timeout=timedelta(minutes=2),
                       desired_worker_labels=[DesiredWorkerLabel(key="station", value=station, required=True)])
        def run(input, ctx):
            actual = os.environ["FLEET_PROBE_WORKER"]
            with open(os.environ["FLEET_PROBE_MARKER"], "a", encoding="utf-8") as stream:
                stream.write(step + ":" + actual + "\n")
            return {"worker": actual}
        return run

    first = task_for("first", "station-a")
    task_for("second", "station-b", [first])
    return workflow


def worker_main():
    if sys.platform == "win32":
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from hatchet_windows import install
        install()
    client = Hatchet()
    workflow = make_workflow(client, os.environ["FLEET_PROBE_NAME"])
    client.worker(os.environ["FLEET_PROBE_NAME"] + "-" + os.environ["FLEET_PROBE_WORKER"], slots=1,
                  labels={"kind": os.environ["FLEET_PROBE_KIND"],
                          "station": os.environ["FLEET_PROBE_WORKER"]},
                  workflows=[workflow, make_two_step_workflow(client, os.environ["FLEET_PROBE_NAME"])]).start()


@unittest.skipUnless(os.environ.get("FLEET_HATCHET_ISOLATED") == "1",
                     "set FLEET_HATCHET_ISOLATED=1 and isolated endpoint credentials to run live tests")
class HatchetLiveFeasibility(unittest.TestCase):
    def setUp(self):
        for key in ("HATCHET_CLIENT_TOKEN", "HATCHET_CLIENT_HOST_PORT", "HATCHET_CLIENT_SERVER_URL"):
            if not os.environ.get(key):
                self.fail(f"{key} must explicitly identify the isolated stack")
        self.name = "fleet-probe-" + uuid.uuid4().hex[:12]
        self.directory = tempfile.TemporaryDirectory(prefix="fleet-probe-")
        self.marker = str(Path(self.directory.name) / "runs.txt")
        self.processes = []
        self.client = Hatchet()
        self.workflow = make_workflow(self.client, self.name)

    def stop_worker(self, process):
        if process.poll() is None:
            if sys.platform == "win32":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
            else:
                process.terminate()
            process.wait(timeout=10)

    def tearDown(self):
        for process in self.processes:
            self.stop_worker(process)
        self.directory.cleanup()

    def worker(self, station, kind):
        env = os.environ.copy()
        env.update(FLEET_PROBE_NAME=self.name, FLEET_PROBE_MARKER=self.marker,
                   FLEET_PROBE_WORKER=station, FLEET_PROBE_KIND=kind)
        process = subprocess.Popen([sys.executable, __file__, "--worker"], env=env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.processes.append(process)
        time.sleep(12)
        self.assertIsNone(process.poll(), "isolated worker exited during startup")
        return process

    def lines(self):
        path = Path(self.marker)
        return path.read_text(encoding="utf-8").splitlines() if path.exists() else []

    def wait_lines(self, count, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if len(self.lines()) >= count:
                return self.lines()
            time.sleep(1)
        self.fail(f"expected {count} executions, observed {self.lines()}")

    def test_runtime_override_preserves_required_task_label(self):
        a = self.worker("station-a", "eligible")
        self.worker("station-b", "ineligible")
        ref = self.workflow.run(wait_for_result=False, desired_worker_labels=[
            DesiredWorkerLabel(key="station", value="station-b", required=True)])
        time.sleep(20)
        self.assertEqual(self.lines(), [], "ineligible worker must not execute required task label")
        self.client.runs.cancel(ref.workflow_run_id)

    def test_required_task_label_disconnection_and_reregistration(self):
        a = self.worker("station-a", "eligible")
        b = self.worker("station-b", "ineligible")
        self.stop_worker(a)
        ref = self.workflow.run(wait_for_result=False)
        time.sleep(20)
        self.assertEqual(self.lines(), [], "offline or ineligible workers must not execute")
        self.stop_worker(b)
        self.worker("station-b", "eligible")
        self.assertEqual(self.wait_lines(1), ["station-b"])
        ref.result()

    def test_distinct_per_step_selectors(self):
        self.worker("station-a", "eligible")
        self.worker("station-b", "eligible")
        flow = make_two_step_workflow(self.client, self.name)
        flow.run(wait_for_result=False)
        self.assertEqual(self.wait_lines(2), ["first:station-a", "second:station-b"])

    def test_per_step_selectors_with_runtime_override(self):
        self.worker("station-a", "eligible")
        self.worker("station-b", "eligible")
        flow = make_two_step_workflow(self.client, self.name)
        flow.run(wait_for_result=False, desired_worker_labels=[
            DesiredWorkerLabel(key="station", value="station-b", required=True)])
        self.assertEqual(self.wait_lines(2), ["first:station-a", "second:station-b"])

    def test_runtime_worker_id_selection(self):
        self.worker("station-a", "eligible")
        self.worker("station-b", "eligible")
        workers = self.client.workers.list().rows or []
        matches = [worker for worker in workers if worker.name == self.name + "-station-b"]
        self.assertEqual(len(matches), 1, "expected one station-b registration")
        self.workflow.run(wait_for_result=False, desired_worker_id=matches[0].metadata.id)
        self.assertEqual(self.wait_lines(1), ["station-b"])

    def test_queued_cancellation(self):
        self.worker("station-a", "ineligible")
        ref = self.workflow.run(wait_for_result=False)
        time.sleep(15)
        self.assertEqual(self.lines(), [])
        self.client.runs.cancel(ref.workflow_run_id)
        time.sleep(10)
        self.worker("station-b", "eligible")
        time.sleep(20)
        self.assertEqual(self.lines(), [], "cancelled queued work executed")

    def test_wait_beyond_default_timeout(self):
        self.worker("station-a", "ineligible")
        ref = self.workflow.run(wait_for_result=False)
        time.sleep(310)
        self.assertEqual(self.lines(), [])
        self.worker("station-b", "eligible")
        self.assertEqual(self.wait_lines(1), ["station-b"])
        ref.result()


if __name__ == "__main__":
    if sys.argv[1:] == ["--worker"]:
        worker_main()
    else:
        unittest.main()
