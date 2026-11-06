"""Windows UI Automation and documented Orca CLI adapters for the failover bridge.

The UI adapter intentionally uses only pywinauto's UIA backend. It resolves a
configured executable, top-level window, project-root control, and exact configured
selectors on every operation; anything ambiguous or inaccessible fails closed.
"""
from __future__ import annotations

import ctypes
import json
import os
import re
import subprocess
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from .failover_bridge import (
    MAX_HANDOFF_CHARS,
    AdapterResult,
    FailoverConfig,
    Observation,
    ProjectBinding,
    UIASelector,
    json_pointer,
    match_marker,
)


class _AdapterFailure(RuntimeError):
    def __init__(self, status: str, reason: str):
        super().__init__(reason)
        self.status = status
        self.reason = reason


@dataclass(frozen=True)
class _WindowTarget:
    process_id: int
    handle: int
    window: Any
    root_control: Any
    identity: str


class WindowsDesktopAdapter:
    """Concrete DesktopAdapter backed by exact-process Windows UI Automation."""

    def __init__(self) -> None:
        self._uia_desktop: Any | None = None
        self._uia_error = "pywinauto_uia_unavailable"
        if os.name != "nt":
            self._uia_error = "windows_only"
            return
        try:
            from pywinauto import Desktop

            self._uia_desktop = Desktop(backend="uia")
        except Exception:
            # pywinauto is an optional runtime dependency; never fall back to OCR,
            # Win32 control scraping, browser storage, or another process's window.
            self._uia_error = "pywinauto_uia_unavailable"

    def observe(self, project: ProjectBinding) -> Observation:
        source = self._source_identity(project)
        if self._uia_desktop is None:
            return Observation("unsupported_surface", source, "", reason=self._uia_error)
        try:
            target = self._resolve_target(project)
            conversation = self._unique_descendant(target.window, project.desktop.conversation,
                                                   "conversation_selector")
            if not self._is_visible(conversation):
                raise _AdapterFailure("unsupported_surface", "conversation_not_visible")
            texts = self._visible_conversation_text(conversation)
            if not texts:
                raise _AdapterFailure("unsupported_surface", "conversation_text_unavailable")
            limit_control = self._optional_notice_control(target.window,
                                                          project.desktop.limit_notice.selector)
            working_control = self._optional_notice_control(target.window,
                                                            project.desktop.working_notice.selector)
            if limit_control is None and working_control is None:
                raise _AdapterFailure("unsupported_surface", "notice_selectors_unavailable")
            limit_texts = self._notice_texts(limit_control)
            working_texts = self._notice_texts(working_control)
            try:
                limit_id = match_marker(limit_texts, project.desktop.limit_notice.markers)
                working_id = match_marker(working_texts, project.desktop.working_notice.markers)
            except ValueError as exc:
                raise _AdapterFailure("unsupported_surface", "conflicting_configured_notice_markers") from exc
            return Observation(
                "ok",
                source,
                target.identity,
                limit_marker_id=limit_id,
                visible_text=self._bounded_recent_text(
                    self._redact_credentials(text) for text in texts
                ),
            )
        except _AdapterFailure as exc:
            return Observation(exc.status, source, "", reason=exc.reason)
        except Exception:
            return Observation("unsupported_surface", source, "", reason="uia_observation_failed")

    def can_send(self, project: ProjectBinding) -> AdapterResult:
        if self._uia_desktop is None:
            return AdapterResult("unsupported_surface", reason=self._uia_error)
        if project.desktop.input_control is None or project.desktop.send_control is None:
            return AdapterResult("manual_intervention_required",
                                 reason="desktop_send_controls_not_configured")
        try:
            target, _, input_control, send_control = self._resolve_send_target(project)
            self._value_pattern(input_control)
            self._invoke_pattern(send_control)
            return AdapterResult("ok", target_identity=target.identity)
        except _AdapterFailure as exc:
            return AdapterResult(exc.status, reason=exc.reason)
        except Exception:
            return AdapterResult("unsupported_surface", reason="uia_target_validation_failed")

    def send(self, project: ProjectBinding, payload: str) -> AdapterResult:
        if self._uia_desktop is None:
            return AdapterResult("unsupported_surface", reason=self._uia_error)
        if project.desktop.input_control is None or project.desktop.send_control is None:
            return AdapterResult("manual_intervention_required",
                                 reason="desktop_send_controls_not_configured")
        wrote = False
        try:
            target, _, input_control, send_control = self._resolve_send_target(project)
            value_pattern = self._value_pattern(input_control)
            invoke_pattern = self._invoke_pattern(send_control)
            # SetValue is the single and only write. No typing, focus, Enter, or retry fallback.
            wrote = True
            value_pattern.SetValue(payload)
            if input_control.element_info.value != payload:
                return AdapterResult("dispatch_uncertain", target_identity=target.identity,
                                     reason="desktop_input_value_did_not_match_payload")
            invoke_pattern.Invoke()
            return AdapterResult("ok", target_identity=target.identity)
        except _AdapterFailure as exc:
            return AdapterResult("dispatch_uncertain" if wrote else exc.status,
                                 target_identity=None, reason=exc.reason)
        except Exception:
            return AdapterResult("dispatch_uncertain" if wrote else "unsupported_surface",
                                 reason="desktop_write_or_send_uncertain" if wrote else
                                        "uia_target_validation_failed")

    def _resolve_send_target(self, project: ProjectBinding) -> tuple[_WindowTarget, Any, Any, Any]:
        target = self._resolve_target(project)
        conversation = self._unique_descendant(target.window, project.desktop.conversation,
                                               "conversation_selector")
        if not self._is_visible(conversation):
            raise _AdapterFailure("manual_intervention_required", "target_conversation_not_visible")
        input_control = self._unique_descendant(target.window, project.desktop.input_control,
                                                "input_control")
        send_control = self._unique_descendant(target.window, project.desktop.send_control,
                                               "send_control")
        if not self._is_visible(input_control) or not self._is_visible(send_control):
            raise _AdapterFailure("manual_intervention_required", "target_send_controls_not_visible")
        if not self._is_enabled(input_control) or not self._is_enabled(send_control):
            raise _AdapterFailure("manual_intervention_required", "target_send_controls_not_enabled")
        return target, conversation, input_control, send_control

    def _resolve_target(self, project: ProjectBinding) -> _WindowTarget:
        self._assert_project_root(project)
        process_image = self._canonical_image(project.desktop.process_image)
        candidates = self._windows_for_image(process_image)
        if len(candidates) != 1:
            reason = "configured_process_window_missing" if not candidates else "configured_process_window_ambiguous"
            raise _AdapterFailure("unsupported_surface", reason)
        pid, handle = candidates[0]
        try:
            window = self._uia_desktop.window(handle=handle).wrapper_object()
        except Exception as exc:
            raise _AdapterFailure("unsupported_surface", "configured_window_uia_unavailable") from exc
        if int(window.element_info.process_id) != pid:
            raise _AdapterFailure("unsupported_surface", "configured_window_process_mismatch")
        if not self._selector_matches(window, project.desktop.window):
            raise _AdapterFailure("unsupported_surface", "configured_window_selector_mismatch")
        root_control = self._unique_descendant(window, project.desktop.project_root,
                                               "project_root_selector")
        if not self._is_visible(root_control) or not self._root_control_matches(root_control, project.root):
            raise _AdapterFailure("root_mismatch", "desktop_project_root_selector_mismatch")
        identity = f"process={pid};window={handle};image={process_image}"
        return _WindowTarget(pid, handle, window, root_control, identity)

    @staticmethod
    def _assert_project_root(project: ProjectBinding) -> None:
        try:
            current = Path(project.root).resolve(strict=True)
        except OSError as exc:
            raise _AdapterFailure("root_mismatch", "configured_project_root_unavailable") from exc
        if not current.is_dir() or os.path.normcase(str(current)) != project.root:
            raise _AdapterFailure("root_mismatch", "configured_project_root_changed")

    @staticmethod
    def _canonical_image(path: str) -> str:
        try:
            return os.path.normcase(str(Path(path).resolve(strict=True)))
        except OSError as exc:
            raise _AdapterFailure("unsupported_surface", "configured_process_image_unavailable") from exc

    def _windows_for_image(self, expected_image: str) -> list[tuple[int, int]]:
        """Enumerate top-level windows, inspecting only HWND/PID and exact image path."""
        user32 = ctypes.WinDLL("user32", use_last_error=True)
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        enum_windows = user32.EnumWindows
        get_window_pid = user32.GetWindowThreadProcessId
        is_visible = user32.IsWindowVisible
        open_process = kernel32.OpenProcess
        query_image = kernel32.QueryFullProcessImageNameW
        close_handle = kernel32.CloseHandle
        enum_windows.argtypes = [ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p),
                                 ctypes.c_void_p]
        enum_windows.restype = ctypes.c_bool
        get_window_pid.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)]
        get_window_pid.restype = ctypes.c_uint32
        is_visible.argtypes = [ctypes.c_void_p]
        is_visible.restype = ctypes.c_bool
        open_process.argtypes = [ctypes.c_uint32, ctypes.c_bool, ctypes.c_uint32]
        open_process.restype = ctypes.c_void_p
        query_image.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_wchar_p,
                                ctypes.POINTER(ctypes.c_uint32)]
        query_image.restype = ctypes.c_bool
        close_handle.argtypes = [ctypes.c_void_p]
        close_handle.restype = ctypes.c_bool

        image_by_pid: dict[int, str | None] = {}
        found: list[tuple[int, int]] = []

        def image_for(pid: int) -> str | None:
            if pid not in image_by_pid:
                handle = open_process(0x1000, False, pid)  # PROCESS_QUERY_LIMITED_INFORMATION
                if not handle:
                    image_by_pid[pid] = None
                else:
                    try:
                        buffer = ctypes.create_unicode_buffer(32768)
                        length = ctypes.c_uint32(len(buffer))
                        if query_image(handle, 0, buffer, ctypes.byref(length)):
                            image_by_pid[pid] = os.path.normcase(str(Path(buffer.value).resolve(strict=False)))
                        else:
                            image_by_pid[pid] = None
                    finally:
                        close_handle(handle)
            return image_by_pid[pid]

        callback_type = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_void_p, ctypes.c_void_p)

        @callback_type
        def visit(hwnd: int, _param: int) -> bool:
            pid_value = ctypes.c_uint32()
            get_window_pid(hwnd, ctypes.byref(pid_value))
            pid = int(pid_value.value)
            if pid and is_visible(hwnd) and image_for(pid) == expected_image:
                found.append((pid, int(hwnd)))
            return True

        if not enum_windows(visit, None):
            raise _AdapterFailure("unsupported_surface", "window_enumeration_failed")
        return found

    def _unique_descendant(self, parent: Any, selector: UIASelector, label: str) -> Any:
        try:
            matches = [item for item in parent.descendants()
                       if self._selector_matches(item, selector)]
        except Exception as exc:
            raise _AdapterFailure("unsupported_surface", f"{label}_unavailable") from exc
        if len(matches) != 1:
            reason = f"{label}_missing" if not matches else f"{label}_ambiguous"
            raise _AdapterFailure("unsupported_surface", reason)
        return matches[0]

    @staticmethod
    def _selector_matches(control: Any, selector: UIASelector) -> bool:
        info = control.element_info
        return (
            info.auto_id == selector.automation_id
            and info.control_type == selector.control_type
            and (selector.class_name is None or info.class_name == selector.class_name)
            and (selector.name is None or info.name == selector.name)
        )

    def _optional_notice_control(self, window: Any, selector: UIASelector) -> Any | None:
        try:
            matches = [item for item in window.descendants() if self._selector_matches(item, selector)]
        except Exception as exc:
            raise _AdapterFailure("unsupported_surface", "notice_selector_unavailable") from exc
        if len(matches) > 1:
            raise _AdapterFailure("unsupported_surface", "notice_selector_ambiguous")
        return matches[0] if matches else None

    @staticmethod
    def _is_visible(control: Any) -> bool:
        try:
            return bool(control.element_info.visible)
        except Exception as exc:
            raise _AdapterFailure("unsupported_surface", "uia_visibility_unavailable") from exc

    @staticmethod
    def _is_enabled(control: Any) -> bool:
        try:
            return bool(control.element_info.enabled)
        except Exception as exc:
            raise _AdapterFailure("unsupported_surface", "uia_enabled_state_unavailable") from exc

    @staticmethod
    def _root_control_matches(control: Any, expected_root: str) -> bool:
        try:
            info = control.element_info
            current = info.value or info.name
        except Exception as exc:
            raise _AdapterFailure("unsupported_surface", "project_root_text_unavailable") from exc
        if not isinstance(current, str) or not current.strip():
            raise _AdapterFailure("unsupported_surface", "project_root_text_unavailable")
        try:
            actual = Path(current.strip()).resolve(strict=True)
        except (OSError, ValueError):
            return False
        return actual.is_dir() and os.path.normcase(str(actual)) == expected_root

    @staticmethod
    def _notice_texts(control: Any | None) -> list[str]:
        if control is None:
            return []
        try:
            if not bool(control.element_info.visible):
                return []
            info = control.element_info
            values = [info.value, info.name]
        except Exception as exc:
            raise _AdapterFailure("unsupported_surface", "notice_text_unavailable") from exc
        texts = [value for value in values if isinstance(value, str) and value.strip()]
        if not texts:
            raise _AdapterFailure("unsupported_surface", "notice_text_unavailable")
        return texts

    def _visible_conversation_text(self, conversation: Any) -> list[str]:
        """Read leaf names under the configured conversation only; never read a document range."""
        output: list[str] = []
        stack = list(reversed(conversation.children()))
        visited = 0
        while stack:
            node = stack.pop()
            visited += 1
            if visited > 100_000:
                raise _AdapterFailure("unsupported_surface", "conversation_tree_too_large")
            try:
                if not bool(node.element_info.visible):
                    continue
                children = node.children()
            except Exception as exc:
                raise _AdapterFailure("unsupported_surface", "conversation_tree_unavailable") from exc
            if children:
                stack.extend(reversed(children))
                continue
            try:
                info = node.element_info
                name = info.name
                control_type = info.control_type
            except Exception as exc:
                raise _AdapterFailure("unsupported_surface", "conversation_text_unavailable") from exc
            # Name is the UIA-rendered accessible text. Do not read Edit values or a
            # TextPattern range, either of which may contain non-visible history.
            if control_type in {"Text", "ListItem", "DataItem", "TreeItem", "Custom"}:
                if isinstance(name, str) and name.strip():
                    output.append(name.strip())
        return output
    @staticmethod
    def _redact_credentials(text: str) -> str:
        patterns = (
            r"(?i)\b(?:api[_ -]?key|access[_ -]?token|secret|password|authorization|client[_ -]?secret|private[_ -]?key)\b\s*[:=]\s*[\"']?[^\s\"'<>]+",
            r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}",
            r"\b(?:sk-[A-Za-z0-9_-]{12,}|gh[pousr]_[A-Za-z0-9]{12,}|xox[baprs]-[A-Za-z0-9-]{10,}|AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{30,})\b",
            r"\beyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}\b",
            r"(?s)-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----.*?-----END (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
            r"https?://[^:/\s]+:[^@\s]+@",
        )
        for pattern in patterns:
            text = re.sub(pattern, "[REDACTED CREDENTIAL-LIKE VALUE]", text)
        return text


    @staticmethod
    def _bounded_recent_text(texts: Iterable[str]) -> str:
        limit = MAX_HANDOFF_CHARS + 1
        parts: deque[str] = deque()
        total = 0
        for value in texts:
            part = value.replace("\r\n", "\n").replace("\r", "\n")
            if len(part) > limit:
                part = part[-limit:]
            if parts:
                total += 1
            parts.append(part)
            total += len(part)
            while total > limit and parts:
                excess = total - limit
                first = parts[0]
                if excess >= len(first) + (1 if len(parts) > 1 else 0):
                    parts.popleft()
                    total -= len(first) + (1 if parts else 0)
                else:
                    parts[0] = first[excess:]
                    total -= excess
        return "\n".join(parts)

    @staticmethod
    def _source_identity(project: ProjectBinding) -> str:
        return f"claude:{project.id}:{project.desktop.process_image}"

    @staticmethod
    def _value_pattern(control: Any) -> Any:
        try:
            return control.iface_value
        except Exception as exc:
            raise _AdapterFailure("manual_intervention_required", "input_value_pattern_unavailable") from exc

    @staticmethod
    def _invoke_pattern(control: Any) -> Any:
        try:
            return control.iface_invoke
        except Exception as exc:
            raise _AdapterFailure("manual_intervention_required", "send_invoke_pattern_unavailable") from exc


class OrcaCliAdapter:
    """Orca adapter using only the documented JSON CLI commands and configured pointers."""

    _COMMAND_TIMEOUT_SECONDS = 45

    def __init__(self, config: FailoverConfig) -> None:
        self._config = config
        self._windows_only = os.name != "nt"

    def probe(self) -> AdapterResult:
        if self._windows_only:
            return AdapterResult("unsupported_surface", reason="windows_only")
        try:
            value = self._json_command([self._config.orca_executable, "status", "--json"])
            if not isinstance(value, (dict, list)):
                return AdapterResult("unsupported_surface", reason="orca_status_json_shape_untrusted")
            return AdapterResult("ok")
        except _AdapterFailure as exc:
            return AdapterResult("unsupported_surface", reason=exc.reason)
        except Exception:
            return AdapterResult("unsupported_surface", reason="orca_status_probe_failed")

    def observe(self, project: ProjectBinding, session_id: str) -> Observation:
        source = f"orca:{session_id}"
        if self._windows_only:
            return Observation("unsupported_surface", source, "", reason="windows_only")
        try:
            self._assert_root(project)
            worktree = self._worktree_for_session(project, session_id)
            terminal_id = self._terminal_for_session(project, session_id)
            self._assert_root(project)
            screen_json = self._json_command([
                self._config.orca_executable, "terminal", "read", "--terminal", terminal_id,
                "--screen", "--json",
            ])
            screen = json_pointer(screen_json, self._config.orca_schema.terminal_screen_pointer)
            if not isinstance(screen, str):
                raise _AdapterFailure("unsupported_surface", "orca_terminal_screen_shape_untrusted")
            try:
                limit_id = match_marker([screen], self._config.orca_limit_markers)
                working_id = match_marker([screen], self._config.orca_working_markers)
            except ValueError as exc:
                raise _AdapterFailure("unsupported_surface", "conflicting_configured_terminal_markers") from exc
            return Observation("ok", source,
                               f"worktree={project.worktree_selector};session={session_id};terminal={terminal_id};root={worktree}",
                               limit_marker_id=limit_id, working_marker_id=working_id,
                               visible_text=WindowsDesktopAdapter._redact_credentials(
                                   screen[-(MAX_HANDOFF_CHARS + 1):]))
        except _AdapterFailure as exc:
            return Observation(exc.status, source, "", reason=exc.reason)
        except Exception:
            return Observation("unsupported_surface", source, "", reason="orca_terminal_observation_failed")

    def launch(self, project: ProjectBinding, payload: str) -> AdapterResult:
        if self._windows_only:
            return AdapterResult("unsupported_surface", reason="windows_only")
        try:
            self._assert_root(project)
            # A configured path is not enough: the CLI's own worktree listing must
            # confirm one exact root and, when supplied, the configured source session.
            self._worktree_for_launch(project)
        except _AdapterFailure as exc:
            return AdapterResult(exc.status, reason=exc.reason)
        try:
            result = self._json_command([
                self._config.orca_executable,
                "agent", "launch", "--agent", self._config.orca_agent,
                "--worktree", project.worktree_selector,
                "--prompt", payload,
                "--json",
            ])
            session_id = json_pointer(result, self._config.orca_schema.launch_session_pointer)
            if not isinstance(session_id, str) or not session_id.strip():
                raise _AdapterFailure("dispatch_uncertain", "orca_launch_session_id_untrusted")
            session_id = session_id.strip()
            self._assert_root(project)
            self._worktree_for_session(project, session_id)
            return AdapterResult("ok", target_session_handle=session_id,
                                 target_identity=project.worktree_selector)
        except _AdapterFailure as exc:
            # Once launch was invoked, errors are uncertain and must never be retried blindly.
            return AdapterResult("dispatch_uncertain", reason=exc.reason)
        except Exception:
            return AdapterResult("dispatch_uncertain", reason="orca_launch_result_untrusted")

    def _worktree_for_launch(self, project: ProjectBinding) -> str:
        if project.orca_session_id is not None:
            root, _pane = self._worktree_for_session(project, project.orca_session_id)
            return root
        # A Desktop-originated failover can target a registered worktree with no
        # agent session yet. Resolve that exact path through Orca, not cwd inference.
        response = self._json_command([
            self._config.orca_executable, "worktree", "show",
            "--worktree", project.worktree_selector, "--json",
        ])
        root_value = json_pointer(response, self._config.orca_schema.worktree_root_pointer)
        if not isinstance(root_value, str) or self._canonical_root(root_value) != project.root:
            raise _AdapterFailure("root_mismatch", "orca_worktree_show_root_mismatch")
        return project.root

    def _worktree_for_session(self, project: ProjectBinding, session_id: str) -> tuple[str, str]:
        rows = self._worktree_rows()
        matching = [(root, listed_session, pane_key) for root, listed_session, pane_key in rows
                    if root == project.root and listed_session == session_id]
        if len(matching) != 1:
            raise _AdapterFailure("root_mismatch", "orca_session_project_mapping_mismatch")
        if sum(1 for _, listed_session, _ in rows if listed_session == session_id) != 1:
            raise _AdapterFailure("unsupported_surface", "orca_session_mapping_ambiguous")
        root, _session, pane_key = matching[0]
        if not isinstance(pane_key, str) or pane_key.count(":") < 1:
            raise _AdapterFailure("unsupported_surface", "orca_agent_pane_key_untrusted")
        return root, pane_key

    def _worktree_rows(self) -> list[tuple[str, str, str]]:
        response = self._json_command([
            self._config.orca_executable, "worktree", "ps", "--json",
        ])
        entries = json_pointer(response, self._config.orca_schema.worktree_entries_pointer)
        if not isinstance(entries, list):
            raise _AdapterFailure("unsupported_surface", "orca_worktree_list_shape_untrusted")
        rows: list[tuple[str, str, str]] = []
        for entry in entries:
            root_value = json_pointer(entry, self._config.orca_schema.worktree_root_pointer)
            agents = json_pointer(entry, self._config.orca_schema.worktree_agents_pointer)
            if not isinstance(root_value, str) or not isinstance(agents, list):
                raise _AdapterFailure("unsupported_surface", "orca_worktree_entry_shape_untrusted")
            root = self._canonical_root(root_value)
            for agent in agents:
                session_value = json_pointer(agent, self._config.orca_schema.agent_session_pointer)
                pane_key = json_pointer(agent, self._config.orca_schema.agent_pane_key_pointer)
                if not isinstance(session_value, str) or not session_value or not isinstance(pane_key, str):
                    raise _AdapterFailure("unsupported_surface", "orca_agent_entry_shape_untrusted")
                rows.append((root, session_value, pane_key))
        return rows

    def _terminal_for_session(self, project: ProjectBinding, session_id: str) -> str:
        root, pane_key = self._worktree_for_session(project, session_id)
        tab_id, leaf_id = pane_key.split(":", 1)
        if not tab_id or not leaf_id:
            raise _AdapterFailure("unsupported_surface", "orca_agent_pane_key_untrusted")
        response = self._json_command([
            self._config.orca_executable, "terminal", "list",
            "--worktree", f"path:{root}", "--json",
        ])
        entries = json_pointer(response, self._config.orca_schema.terminal_entries_pointer)
        if not isinstance(entries, list):
            raise _AdapterFailure("unsupported_surface", "orca_terminal_list_shape_untrusted")
        matches: list[str] = []
        for entry in entries:
            terminal_id = json_pointer(entry, self._config.orca_schema.terminal_id_pointer)
            terminal_tab = json_pointer(entry, self._config.orca_schema.terminal_tab_pointer)
            terminal_leaf = json_pointer(entry, self._config.orca_schema.terminal_leaf_pointer)
            if not all(isinstance(item, str) and item for item in (terminal_id, terminal_tab, terminal_leaf)):
                raise _AdapterFailure("unsupported_surface", "orca_terminal_entry_shape_untrusted")
            if terminal_tab == tab_id and terminal_leaf == leaf_id:
                matches.append(terminal_id)
        if len(matches) != 1:
            reason = "orca_session_terminal_missing" if not matches else "orca_session_terminal_ambiguous"
            raise _AdapterFailure("unsupported_surface", reason)
        # Reconfirm session-to-root mapping immediately before the terminal read.
        self._worktree_for_session(project, session_id)
        return matches[0]

    def _json_command(self, argv: list[str]) -> Any:
        try:
            completed = subprocess.run(
                argv,
                shell=False,
                check=False,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self._COMMAND_TIMEOUT_SECONDS,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except subprocess.TimeoutExpired as exc:
            raise _AdapterFailure("unsupported_surface", "orca_command_timed_out") from exc
        except OSError as exc:
            raise _AdapterFailure("unsupported_surface", "orca_cli_unavailable") from exc
        if completed.returncode != 0:
            raise _AdapterFailure("unsupported_surface", "orca_command_failed")
        try:
            return json.loads(completed.stdout)
        except (json.JSONDecodeError, TypeError) as exc:
            raise _AdapterFailure("unsupported_surface", "orca_json_output_untrusted") from exc

    @staticmethod
    def _assert_root(project: ProjectBinding) -> None:
        try:
            resolved = Path(project.root).resolve(strict=True)
        except OSError as exc:
            raise _AdapterFailure("root_mismatch", "configured_project_root_unavailable") from exc
        if not resolved.is_dir() or os.path.normcase(str(resolved)) != project.root:
            raise _AdapterFailure("root_mismatch", "configured_project_root_changed")
        if project.worktree_selector != f"path:{project.root}":
            raise _AdapterFailure("root_mismatch", "orca_worktree_selector_mismatch")

    @staticmethod
    def _canonical_root(value: str) -> str:
        path = Path(value)
        if not path.is_absolute():
            raise _AdapterFailure("unsupported_surface", "orca_worktree_root_not_absolute")
        try:
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise _AdapterFailure("unsupported_surface", "orca_worktree_root_unavailable") from exc
        if not resolved.is_dir():
            raise _AdapterFailure("unsupported_surface", "orca_worktree_root_not_directory")
        return os.path.normcase(str(resolved))


def build_runtime(config: FailoverConfig, state_path: str | os.PathLike[str]):
    """Construct the concrete adapters and the core polling bridge."""
    from .failover_bridge import FailoverBridge, StateStore

    if os.name != "nt":
        raise RuntimeError("the failover runtime is Windows-only")
    return FailoverBridge(config, WindowsDesktopAdapter(), OrcaCliAdapter(config), StateStore(state_path))
