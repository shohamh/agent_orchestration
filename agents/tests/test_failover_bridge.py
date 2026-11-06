import json
import os
import tempfile
from unittest.mock import patch

import unittest
from datetime import datetime, timezone
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentdriver.failover_bridge import (
    AdapterResult, DesktopProject, FailoverBridge, FailoverConfig, Marker, NoticeSpec,
    Observation, OrcaSchema, ProjectBinding, StateStore, UIASelector,
    build_handoff_payload, load_config, match_marker,
)

from agentdriver.failover_runtime import OrcaCliAdapter, WindowsDesktopAdapter


class DesktopFake:
    def __init__(self, observations):
        self.observations = list(observations)
        self.sent = []
        self.can_send_result = AdapterResult("ok", target_identity="desktop:42")
        self.send_result = AdapterResult("ok", target_identity="desktop:42")

    def observe(self, project):
        return self.observations.pop(0) if self.observations else Observation(
            "ok", f"claude:{project.id}:{project.desktop.process_image}", "window:42",
            working_marker_id="working")

    def can_send(self, project):
        return self.can_send_result

    def send(self, project, payload):
        self.sent.append(payload)
        return self.send_result


class OrcaFake:
    def __init__(self, observations=()):
        self.observations = list(observations)
        self.launches = []
        self.launch_result = AdapterResult("ok", "session-new", "worktree:1")
        self.probe_result = AdapterResult("ok")

    def probe(self):
        return self.probe_result

    def observe(self, project, session_id):
        return self.observations.pop(0) if self.observations else Observation(
            "ok", f"orca:{session_id}", session_id, working_marker_id="working")

    def launch(self, project, payload):
        self.launches.append((project.worktree_selector, payload))
        return self.launch_result


def make_binding(root, *, session="session-old"):
    selector = UIASelector("configured-id", "Text")
    desktop = DesktopProject(
        "desktop-project", "C:/Program Files/Claude/Claude.exe", selector, selector,
        selector,
        NoticeSpec(selector, (Marker("limit", "usage limit reset"),)),
        NoticeSpec(selector, (Marker("working", "ready for your message"),)),
        selector, selector,
    )
    return ProjectBinding("mapping-1", os.path.normcase(str(root.resolve())), "Finish the requested fix", desktop, session)


def make_config(project):
    schema = OrcaSchema("/session", "/worktrees", "/root", "/agents", "/id", "/paneKey",
                        "/terminals", "/id", "/tabId", "/leafId", "/screen")
    return FailoverConfig("orca", "codex", schema, (Marker("limit", "usage limit reset"),),
                          (Marker("working", "agent is working"),), (project,), 1.0)


class FailoverBridgeTests(unittest.TestCase):
    def test_marker_requires_configured_explicit_text_and_rejects_conflicts(self):
        configured = (Marker("quota", "usage limit reset"),)
        self.assertEqual(match_marker(["Account paused: USAGE LIMIT RESET at 3pm"], configured), "quota")
        self.assertIsNone(match_marker(["Something went wrong"], configured))
        with self.assertRaisesRegex(ValueError, "multiple configured markers"):
            match_marker(["usage limit reset; another quota exhausted"],
                         configured + (Marker("other", "quota exhausted"),))

    def test_payload_is_hard_bounded_and_retains_latest_visible_context(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            payload = build_handoff_payload(project, "old-content\n" + "x" * 60_000 + "RECENT")
            self.assertLessEqual(len(payload), 48_000)
            self.assertIn("VISIBLE / PARTIAL", payload)
            self.assertIn("Earlier visible content omitted", payload)
            self.assertTrue(payload.endswith("RECENT\n--- end visible source context ---\n"))
            self.assertIn("Inspect the current project files", payload)

    def test_two_observations_dispatch_once_in_exact_mapped_project(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            identity = f"claude:{project.id}:{project.desktop.process_image}"
            desktop = DesktopFake([
                Observation("ok", identity, "window:42", working_marker_id="working"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="recent task"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="recent task"),
            ])
            orca = OrcaFake()
            with tempfile.TemporaryDirectory() as state_dir:
                state = StateStore(Path(state_dir) / "state.json")
                bridge = FailoverBridge(make_config(project), desktop, orca, state,
                                        clock=lambda: datetime(2025, 1, 1, tzinfo=timezone.utc))
                self.assertEqual(bridge.poll_once()[0]["status"], "armed")
                self.assertEqual(bridge.poll_once()[0]["status"], "confirming_limit")
                self.assertEqual(bridge.poll_once()[0]["status"], "dispatched")
                bridge.poll_once()
                self.assertEqual(len(orca.launches), 1)
                self.assertEqual(orca.launches[0][0], f"path:{project.root}")
                self.assertEqual(orca.launches[0][1].count("recent task"), 1)
                persisted = (Path(state_dir) / "state.json").read_text(encoding="utf-8")
                self.assertNotIn("recent task", persisted)
                self.assertEqual(state.active_session(project.id), "session-new")

    def test_reverse_route_waits_for_working_then_requires_two_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            desktop = DesktopFake([Observation(
                "ok", f"claude:{project.id}:{project.desktop.process_image}", "window:42",
                working_marker_id="working")])
            orca_id = "session-old"
            orca = OrcaFake([
                Observation("ok", f"orca:{orca_id}", orca_id, working_marker_id="working"),
                Observation("ok", f"orca:{orca_id}", orca_id, limit_marker_id="limit", visible_text="visible terminal"),
                Observation("ok", f"orca:{orca_id}", orca_id, limit_marker_id="limit", visible_text="visible terminal"),
            ])
            with tempfile.TemporaryDirectory() as state_dir:
                bridge = FailoverBridge(make_config(project), desktop, orca,
                                        StateStore(Path(state_dir) / "state.json"))
                self.assertEqual(bridge.poll_once()[-1]["status"], "armed")
                self.assertEqual(bridge.poll_once()[-1]["status"], "confirming_limit")
                self.assertEqual(bridge.poll_once()[-1]["status"], "dispatched")
                self.assertEqual(len(desktop.sent), 1)
                self.assertIn("visible terminal", desktop.sent[0])

    def test_unconfirmed_target_and_dispatch_uncertainty_do_not_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            identity = f"claude:{project.id}:{project.desktop.process_image}"
            desktop = DesktopFake([
                Observation("ok", identity, "window:42", working_marker_id="working"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="x"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="x"),
            ])
            orca = OrcaFake()
            orca.launch_result = AdapterResult("dispatch_uncertain", reason="unknown outcome")
            with tempfile.TemporaryDirectory() as state_dir:
                state = StateStore(Path(state_dir) / "state.json")
                bridge = FailoverBridge(make_config(project), desktop, orca, state)
                for _ in range(3):
                    result = bridge.poll_once()
                self.assertEqual(result[0]["status"], "dispatch_uncertain")
                bridge.poll_once()
                self.assertEqual(len(orca.launches), 1)
                self.assertTrue(any("dispatch_uncertain" in str(r) for r in state.routes().values()))

    def test_invalid_project_mapping_is_rejected_before_config_is_accepted(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "project"
            root.mkdir()
            config = {
                "version": 1,
                "desktop_projects": [{
                    "id": "desktop", "process_image": str(Path(tmp) / "Claude.exe"),
                    "window": {"automation_id": "win", "control_type": "Window"},
                    "project_root": {"automation_id": "root", "control_type": "Text"},
                    "conversation": {"automation_id": "conversation", "control_type": "Text"},
                    "limit_notice": {"selector": {"automation_id": "limit", "control_type": "Text"},
                                     "markers": [{"id": "limit", "text": "usage limit reset"}]},
                    "working_notice": {"selector": {"automation_id": "work", "control_type": "Text"},
                                       "markers": [{"id": "work", "text": "ready for your message"}]},
                    "input_control": {"automation_id": "input", "control_type": "Edit"},
                    "send_control": {"automation_id": "send", "control_type": "Button"},
                }],
                "orca": {"executable": "orca", "agent": "codex",
                         "json": {key: "/x" for key in (
                             "launch_session_pointer", "worktree_entries_pointer", "worktree_root_pointer",
                             "worktree_agents_pointer", "agent_session_pointer", "agent_pane_key_pointer",
                             "terminal_entries_pointer", "terminal_id_pointer", "terminal_tab_pointer",
                             "terminal_leaf_pointer", "terminal_screen_pointer")},
                         "limit_markers": [{"id": "limit", "text": "usage limit reset"}],
                         "working_markers": [{"id": "work", "text": "agent working"}]},
                "mappings": [{"id": "m", "desktop_id": "desktop", "root": str(root),
                              "task": "Continue", "orca_session_id": "launched"}],
            }
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(config), encoding="utf-8")
            loaded = load_config(path)
            self.assertEqual(loaded.projects[0].root, os.path.normcase(str(root.resolve())))
            config["mappings"].append(dict(config["mappings"][0], id="duplicate"))
            path.write_text(json.dumps(config), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "maps to more than one"):
                load_config(path)

    def test_state_store_refuses_persisted_transcript_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            state_path = Path(tmp) / "state.json"
            state_path.write_text(json.dumps({"version": 1, "routes": {
                "x": {"phase": "dispatched", "last_handoff": {"details": {"screen": "private"}}}},
                "active_sessions": {}}), encoding="utf-8")
            from agentdriver.failover_bridge import StateError
            with self.assertRaisesRegex(StateError, "contains an invalid route"):
                StateStore(state_path)

    def test_restart_does_not_repeat_a_dispatched_launch(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            identity = f"claude:{project.id}:{project.desktop.process_image}"
            desktop = DesktopFake([
                Observation("ok", identity, "window:42", working_marker_id="working"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="first"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="first"),
            ])
            orca = OrcaFake()
            with tempfile.TemporaryDirectory() as state_dir:
                path = Path(state_dir) / "state.json"
                state = StateStore(path)
                FailoverBridge(make_config(project), desktop, orca, state).poll_once()
                bridge = FailoverBridge(make_config(project), desktop, orca, state)
                bridge.poll_once()
                bridge.poll_once()
                self.assertEqual(len(orca.launches), 1)
                desktop.observations = [
                    Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="first"),
                    Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="first"),
                ]
                restarted = FailoverBridge(make_config(project), desktop, orca, StateStore(path))
                self.assertEqual(restarted.poll_once()[0]["status"], "suppressed")
                self.assertEqual(restarted.poll_once()[0]["status"], "suppressed")
                self.assertEqual(len(orca.launches), 1)


    def test_orca_adapter_uses_explicit_project_and_correlated_terminal(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            calls = []

            def fake_command(argv, timeout=30):
                calls.append(argv)
                if argv[1:3] == ["worktree", "ps"]:
                    return {"worktrees": [{"root": project.root, "agents": [
                        {"id": "session-old", "paneKey": "tab:leaf"}]}]}
                if argv[1:3] == ["terminal", "list"]:
                    return {"terminals": [{"id": "terminal-1", "tabId": "tab", "leafId": "leaf"}]}
                if argv[1:3] == ["terminal", "read"]:
                    self.assertIn("--screen", argv)
                    return {"screen": "usage limit reset"}
                raise AssertionError(argv)

            adapter = OrcaCliAdapter(make_config(project))
            with patch.object(adapter, "_json_command", side_effect=fake_command):
                observed = adapter.observe(project, "session-old")
            self.assertEqual(observed.status, "ok", observed.reason)
            self.assertEqual(observed.limit_marker_id, "limit")
            self.assertEqual(calls[2][1:4], ["terminal", "list", "--worktree"])
            self.assertIn(project.worktree_selector, calls[2])

    def test_orca_adapter_rejects_project_mismatch_before_reading_terminal(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            different = Path(tmp) / "different"
            different.mkdir()
            calls = []

            def fake_command(argv, timeout=30):
                calls.append(argv)
                return {"worktrees": [{"root": str(different), "agents": [
                    {"id": "session-old", "paneKey": "tab:leaf"}]}]}

            adapter = OrcaCliAdapter(make_config(project))
            with patch.object(adapter, "_json_command", side_effect=fake_command):
                observed = adapter.observe(project, "session-old")
            self.assertEqual(observed.status, "root_mismatch", observed.reason)
            self.assertEqual(len(calls), 1)

    def test_unconfirmed_desktop_target_never_receives_handoff(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            desktop = DesktopFake([])
            desktop.can_send_result = AdapterResult(
                "manual_intervention_required", reason="target window is ambiguous")
            orca = OrcaFake([
                Observation("ok", "orca:session-old", "session-old", working_marker_id="working"),
                Observation("ok", "orca:session-old", "session-old", limit_marker_id="limit", visible_text="context"),
                Observation("ok", "orca:session-old", "session-old", limit_marker_id="limit", visible_text="context"),
            ])
            with tempfile.TemporaryDirectory() as state_dir:
                bridge = FailoverBridge(make_config(project), desktop, orca,
                                        StateStore(Path(state_dir) / "state.json"))
                bridge.poll_once()
                bridge.poll_once()
                result = bridge.poll_once()[-1]
            self.assertEqual(result["status"], "manual_intervention_required")
            self.assertEqual(desktop.sent, [])

    def test_unavailable_orca_runtime_disables_desktop_dispatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            identity = f"claude:{project.id}:{project.desktop.process_image}"
            desktop = DesktopFake([
                Observation("ok", identity, "window:42", working_marker_id="working"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="context"),
                Observation("ok", identity, "window:42", limit_marker_id="limit", visible_text="context"),
            ])
            orca = OrcaFake()
            orca.probe_result = AdapterResult("unsupported_surface", reason="orca status timed out")
            with tempfile.TemporaryDirectory() as state_dir:
                bridge = FailoverBridge(make_config(project), desktop, orca,
                                        StateStore(Path(state_dir) / "state.json"))
                results = [bridge.poll_once() for _ in range(3)]
            self.assertTrue(all(any(row["status"] == "unsupported_surface" for row in batch)
                                for batch in results))
            self.assertEqual(len(orca.launches), 0)
            self.assertEqual(len(desktop.observations), 3)

    def test_orca_launch_uses_registered_existing_worktree_without_child_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp), session=None)
            calls = []

            def fake_command(argv, timeout=30):
                calls.append(argv)
                if argv[1:3] == ["worktree", "show"]:
                    return {"root": project.root}
                if argv[1:3] == ["agent", "launch"]:
                    return {"session": "session-new"}
                if argv[1:3] == ["worktree", "ps"]:
                    return {"worktrees": [{"root": project.root, "agents": [
                        {"id": "session-new", "paneKey": "tab:leaf"}]}]}
                raise AssertionError(argv)

            adapter = OrcaCliAdapter(make_config(project))
            with patch.object(adapter, "_json_command", side_effect=fake_command):
                result = adapter.launch(project, "bounded handoff")
            self.assertEqual(result.status, "ok", result.reason)
            self.assertEqual(result.target_session_handle, "session-new")
            launch = next(argv for argv in calls if argv[1:3] == ["agent", "launch"])
            self.assertIn("--agent", launch)
            self.assertIn("codex", launch)
            self.assertIn("--worktree", launch)
            self.assertIn(project.worktree_selector, launch)
            self.assertNotIn("create", launch)

    def test_runtime_redacts_credential_shaped_visible_text(self):
        redacted = WindowsDesktopAdapter._redact_credentials(
            "api_key=super-secret-value ghp_abcdefghijklmnopqrstuvwxyz012345")
        self.assertNotIn("super-secret-value", redacted)
        self.assertNotIn("ghp_abcdefghijklmnopqrstuvwxyz012345", redacted)
        self.assertEqual(redacted.count("[REDACTED CREDENTIAL-LIKE VALUE]"), 2)

    def test_ui_automation_inaccessibility_is_reported_without_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            project = make_binding(Path(tmp))
            adapter = WindowsDesktopAdapter()
            with patch.object(adapter, "_resolve_target", side_effect=RuntimeError("no matching UIA window")):
                observed = adapter.observe(project)
            self.assertEqual(observed.status, "unsupported_surface")
            self.assertIn(observed.reason, {"pywinauto_uia_unavailable", "uia_observation_failed"})



if __name__ == "__main__":
    unittest.main()
