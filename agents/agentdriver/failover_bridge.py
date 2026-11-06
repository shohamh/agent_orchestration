"""Pure configuration, failover state, payload, and polling logic for the Windows bridge.

The bridge deliberately treats each framework adapter as an untrusted surface: only explicit,
configured limit markers and exact project/window/session identities can trigger a handoff.
"""
from __future__ import annotations

import json
import os
import tempfile
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence

MAX_HANDOFF_CHARS = 48_000
STATE_VERSION = 1


class ConfigError(ValueError):
    """The external bridge configuration is incomplete or unsafe."""


class StateError(RuntimeError):
    """The local bridge state cannot be trusted for deduplication."""


@dataclass(frozen=True)
class UIASelector:
    automation_id: str
    control_type: str
    class_name: str | None = None
    name: str | None = None


@dataclass(frozen=True)
class Marker:
    id: str
    text: str


@dataclass(frozen=True)
class NoticeSpec:
    selector: UIASelector
    markers: tuple[Marker, ...]


@dataclass(frozen=True)
class DesktopProject:
    id: str
    process_image: str
    window: UIASelector
    project_root: UIASelector
    conversation: UIASelector
    limit_notice: NoticeSpec
    working_notice: NoticeSpec
    input_control: UIASelector | None
    send_control: UIASelector | None


@dataclass(frozen=True)
class OrcaSchema:
    launch_session_pointer: str
    worktree_entries_pointer: str
    worktree_root_pointer: str
    worktree_agents_pointer: str
    agent_session_pointer: str
    agent_pane_key_pointer: str
    terminal_entries_pointer: str
    terminal_id_pointer: str
    terminal_tab_pointer: str
    terminal_leaf_pointer: str
    terminal_screen_pointer: str


@dataclass(frozen=True)
class ProjectBinding:
    id: str
    root: str
    task: str
    desktop: DesktopProject
    orca_session_id: str | None

    @property
    def worktree_selector(self) -> str:
        return f"path:{self.root}"


@dataclass(frozen=True)
class FailoverConfig:
    orca_executable: str
    orca_agent: str
    orca_schema: OrcaSchema
    orca_limit_markers: tuple[Marker, ...]
    orca_working_markers: tuple[Marker, ...]
    projects: tuple[ProjectBinding, ...]
    poll_interval_seconds: float


@dataclass(frozen=True)
class Observation:
    status: str
    source_identity: str
    runtime_identity: str
    limit_marker_id: str | None = None
    working_marker_id: str | None = None
    visible_text: str = ""
    reason: str = ""


@dataclass(frozen=True)
class AdapterResult:
    status: str
    target_session_handle: str | None = None
    target_identity: str | None = None
    reason: str = ""


class DesktopAdapter(Protocol):
    def observe(self, project: ProjectBinding) -> Observation: ...

    def can_send(self, project: ProjectBinding) -> AdapterResult: ...

    def send(self, project: ProjectBinding, payload: str) -> AdapterResult: ...


class OrcaAdapter(Protocol):
    def probe(self) -> AdapterResult: ...

    def observe(self, project: ProjectBinding, session_id: str) -> Observation: ...

    def launch(self, project: ProjectBinding, payload: str) -> AdapterResult: ...


class StateStore:
    """Atomic, transcript-free local dedupe state. A single process owns a store instance."""

    def __init__(self, path: str | os.PathLike[str]):
        self.path = Path(path)
        self._lock = threading.RLock()
        self._data: dict[str, Any] = {"version": STATE_VERSION, "routes": {}, "active_sessions": {}}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StateError("bridge state is unreadable; refusing to risk a duplicate handoff") from exc
        if not isinstance(data, dict) or data.get("version") != STATE_VERSION:
            raise StateError("bridge state version is unsupported; refusing to risk a duplicate handoff")
        routes = data.get("routes")
        sessions = data.get("active_sessions")
        if not isinstance(routes, dict) or not isinstance(sessions, dict):
            raise StateError("bridge state shape is invalid; refusing to risk a duplicate handoff")
        # Persisted state must never contain visible or transcript content at any depth.
        for record in routes.values():
            if not isinstance(record, dict) or _contains_transcript_field(record):
                raise StateError("bridge state contains an invalid route record")
        self._data = data

    def route(self, key: str) -> dict[str, Any]:
        with self._lock:
            record = self._data["routes"].get(key)
            return dict(record) if isinstance(record, dict) else {}

    def routes(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {key: dict(value) for key, value in self._data["routes"].items()}

    def active_session(self, binding_id: str) -> str | None:
        with self._lock:
            value = self._data["active_sessions"].get(binding_id)
            return value if isinstance(value, str) and value else None

    def put_route(self, key: str, record: Mapping[str, Any]) -> None:
        with self._lock:
            clean = dict(record)
            if _contains_transcript_field(clean):
                raise StateError("route state cannot persist conversation or screen text")
            self._data["routes"][key] = clean
            self._save_locked()

    def set_active_session(self, binding_id: str, session_id: str) -> None:
        if not session_id:
            raise StateError("an empty Orca session handle cannot become active")
        with self._lock:
            self._data["active_sessions"][binding_id] = session_id
            self._save_locked()

    def _save_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=f".{self.path.name}.", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as stream:
                json.dump(self._data, stream, ensure_ascii=False, separators=(",", ":"))
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
            # Persist the rename when the platform supports directory fsync.
            if os.name != "nt":
                directory_fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
        except Exception:
            try:
                os.unlink(name)
            except OSError:
                pass
            raise


def _contains_transcript_field(value: Any) -> bool:
    forbidden = {"context", "payload", "transcript", "conversation", "visible_text", "screen"}
    if isinstance(value, dict):
        return any(key in forbidden or _contains_transcript_field(item)
                   for key, item in value.items())
    if isinstance(value, list):
        return any(_contains_transcript_field(item) for item in value)
    return False

def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ConfigError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _object(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{label} must be an object")
    return value


def _string(value: Any, label: str, *, nonempty: bool = True) -> str:
    if not isinstance(value, str) or (nonempty and not value.strip()):
        raise ConfigError(f"{label} must be a {'non-empty ' if nonempty else ''}string")
    return value


def _absolute_existing_root(value: Any, label: str) -> str:
    raw = _string(value, label)
    path = Path(raw)
    if not path.is_absolute():
        raise ConfigError(f"{label} must be an absolute path")
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ConfigError(f"{label} does not exist") from exc
    if not resolved.is_dir():
        raise ConfigError(f"{label} must be a directory")
    return os.path.normcase(str(resolved))


def _selector(value: Any, label: str) -> UIASelector:
    obj = _object(value, label)
    automation_id = _string(obj.get("automation_id"), f"{label}.automation_id")
    control_type = _string(obj.get("control_type"), f"{label}.control_type")
    class_name = obj.get("class_name")
    name = obj.get("name")
    if class_name is not None:
        class_name = _string(class_name, f"{label}.class_name")
    if name is not None:
        name = _string(name, f"{label}.name", nonempty=False)
    return UIASelector(automation_id, control_type, class_name, name)


def _markers(value: Any, label: str) -> tuple[Marker, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{label} must be a non-empty array")
    result: list[Marker] = []
    ids: set[str] = set()
    for index, item in enumerate(value):
        obj = _object(item, f"{label}[{index}]")
        marker_id = _string(obj.get("id"), f"{label}[{index}].id")
        text = _string(obj.get("text"), f"{label}[{index}].text")
        if marker_id in ids:
            raise ConfigError(f"{label} contains duplicate marker id {marker_id!r}")
        ids.add(marker_id)
        result.append(Marker(marker_id, text))
    return tuple(result)


def _notice(value: Any, label: str) -> NoticeSpec:
    obj = _object(value, label)
    return NoticeSpec(_selector(obj.get("selector"), f"{label}.selector"),
                      _markers(obj.get("markers"), f"{label}.markers"))


def _pointer(value: Any, label: str) -> str:
    pointer = _string(value, label)
    if pointer != "" and not pointer.startswith("/"):
        raise ConfigError(f"{label} must be a JSON Pointer beginning with '/'")
    return pointer


def load_config(path: str | os.PathLike[str]) -> FailoverConfig:
    """Load and fully validate the external JSON configuration."""
    config_path = Path(path)
    if not config_path.is_absolute():
        raise ConfigError("--config must name an absolute path outside the repository")
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"), object_pairs_hook=_unique_json_object)
    except ConfigError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read configuration: {exc}") from exc
    obj = _object(raw, "configuration")
    if obj.get("version") != 1:
        raise ConfigError("configuration version must be 1")

    interval = obj.get("poll_interval_seconds", 10)
    if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not 0.5 <= float(interval) <= 300:
        raise ConfigError("poll_interval_seconds must be between 0.5 and 300")

    desktop_items = obj.get("desktop_projects")
    if not isinstance(desktop_items, list) or not desktop_items:
        raise ConfigError("desktop_projects must be a non-empty array")
    desktops: dict[str, DesktopProject] = {}
    for index, item in enumerate(desktop_items):
        label = f"desktop_projects[{index}]"
        entry = _object(item, label)
        desktop_id = _string(entry.get("id"), f"{label}.id")
        if desktop_id in desktops:
            raise ConfigError(f"duplicate desktop project id {desktop_id!r}")
        image = _string(entry.get("process_image"), f"{label}.process_image")
        image_path = Path(image)
        if not image_path.is_absolute():
            raise ConfigError(f"{label}.process_image must be an absolute executable path")
        image = os.path.normcase(str(image_path.resolve(strict=False)))
        input_value = entry.get("input_control")
        send_value = entry.get("send_control")
        if (input_value is None) != (send_value is None):
            raise ConfigError(f"{label} must configure both input_control and send_control, or neither")
        desktops[desktop_id] = DesktopProject(
            id=desktop_id,
            process_image=image,
            window=_selector(entry.get("window"), f"{label}.window"),
            project_root=_selector(entry.get("project_root"), f"{label}.project_root"),
            conversation=_selector(entry.get("conversation"), f"{label}.conversation"),
            limit_notice=_notice(entry.get("limit_notice"), f"{label}.limit_notice"),
            working_notice=_notice(entry.get("working_notice"), f"{label}.working_notice"),
            input_control=_selector(input_value, f"{label}.input_control") if input_value is not None else None,
            send_control=_selector(send_value, f"{label}.send_control") if send_value is not None else None,
        )

    orca_obj = _object(obj.get("orca"), "orca")
    executable = _string(orca_obj.get("executable"), "orca.executable")
    agent = _string(orca_obj.get("agent"), "orca.agent")
    schema_obj = _object(orca_obj.get("json"), "orca.json")
    schema = OrcaSchema(
        launch_session_pointer=_pointer(schema_obj.get("launch_session_pointer"), "orca.json.launch_session_pointer"),
        worktree_entries_pointer=_pointer(schema_obj.get("worktree_entries_pointer"), "orca.json.worktree_entries_pointer"),
        worktree_root_pointer=_pointer(schema_obj.get("worktree_root_pointer"), "orca.json.worktree_root_pointer"),
        worktree_agents_pointer=_pointer(schema_obj.get("worktree_agents_pointer"), "orca.json.worktree_agents_pointer"),
        agent_session_pointer=_pointer(schema_obj.get("agent_session_pointer"), "orca.json.agent_session_pointer"),
        agent_pane_key_pointer=_pointer(schema_obj.get("agent_pane_key_pointer"), "orca.json.agent_pane_key_pointer"),
        terminal_entries_pointer=_pointer(schema_obj.get("terminal_entries_pointer"), "orca.json.terminal_entries_pointer"),
        terminal_id_pointer=_pointer(schema_obj.get("terminal_id_pointer"), "orca.json.terminal_id_pointer"),
        terminal_tab_pointer=_pointer(schema_obj.get("terminal_tab_pointer"), "orca.json.terminal_tab_pointer"),
        terminal_leaf_pointer=_pointer(schema_obj.get("terminal_leaf_pointer"), "orca.json.terminal_leaf_pointer"),
        terminal_screen_pointer=_pointer(schema_obj.get("terminal_screen_pointer"), "orca.json.terminal_screen_pointer"),
    )
    orca_limits = _markers(orca_obj.get("limit_markers"), "orca.limit_markers")
    orca_working = _markers(orca_obj.get("working_markers"), "orca.working_markers")

    mappings = obj.get("mappings")
    if not isinstance(mappings, list) or not mappings:
        raise ConfigError("mappings must be a non-empty array")
    projects: list[ProjectBinding] = []
    used_desktop: set[str] = set()
    used_roots: set[str] = set()
    used_sessions: set[str] = set()
    ids: set[str] = set()
    for index, item in enumerate(mappings):
        label = f"mappings[{index}]"
        mapping = _object(item, label)
        binding_id = _string(mapping.get("id"), f"{label}.id")
        if binding_id in ids:
            raise ConfigError(f"duplicate mapping id {binding_id!r}")
        ids.add(binding_id)
        desktop_id = _string(mapping.get("desktop_id"), f"{label}.desktop_id")
        if desktop_id not in desktops:
            raise ConfigError(f"{label}.desktop_id does not name a configured desktop project")
        if desktop_id in used_desktop:
            raise ConfigError(f"desktop project {desktop_id!r} maps to more than one Orca project")
        used_desktop.add(desktop_id)
        root = _absolute_existing_root(mapping.get("root"), f"{label}.root")
        if root in used_roots:
            raise ConfigError("project roots must be unique to keep the mapping bijective")
        used_roots.add(root)
        task = _string(mapping.get("task"), f"{label}.task")
        if "orca_session_id" not in mapping:
            raise ConfigError(f"{label}.orca_session_id must be an exact session handle or 'launched'")
        session_value = mapping.get("orca_session_id")
        if session_value == "launched":
            session_id = None
        else:
            session_id = _string(session_value, f"{label}.orca_session_id")
            if session_id in used_sessions:
                raise ConfigError("configured Orca session handles must map to only one Desktop project")
            used_sessions.add(session_id)
        projects.append(ProjectBinding(binding_id, root, task, desktops[desktop_id], session_id))
    if used_desktop != set(desktops):
        raise ConfigError("every configured desktop project must map to exactly one Orca project")

    return FailoverConfig(executable, agent, schema, orca_limits, orca_working,
                          tuple(projects), float(interval))


def _normalise_text(value: str) -> str:
    return " ".join(value.casefold().split())


def match_marker(texts: Sequence[str], markers: Sequence[Marker]) -> str | None:
    """Return one configured literal marker id; conflicting matches fail closed."""
    normalized_texts = tuple(_normalise_text(text) for text in texts if text)
    matches = {
        marker.id for marker in markers
        if any(_normalise_text(marker.text) in text for text in normalized_texts)
    }
    if len(matches) > 1:
        raise ValueError("multiple configured markers match the same UIA/terminal observation")
    return next(iter(matches), None)


def build_handoff_payload(project: ProjectBinding, visible_text: str) -> str:
    """Build a bounded, visibly partial handoff, retaining the most recent source text."""
    prefix = (
        "AUTOMATED FAILOVER HANDOFF — VISIBLE / PARTIAL CONTEXT\n"
        f"Project root: {project.root}\n"
        f"Current user task: {project.task}\n"
        "The source capture below contains only visible UI/terminal text and may be incomplete.\n"
        "Inspect the current project files and continue the task; do not assume earlier tool results are complete.\n"
        "--- recent visible source context (possibly truncated) ---\n"
    )
    suffix = "\n--- end visible source context ---\n"
    if len(prefix) + len(suffix) > MAX_HANDOFF_CHARS:
        raise ConfigError("project root and current task leave no room for a handoff payload")
    text = visible_text or "[No visible source text was exposed by the configured UIA/terminal surface.]"
    available = MAX_HANDOFF_CHARS - len(prefix) - len(suffix)
    if len(text) <= available:
        return prefix + text + suffix
    label = "[Earlier visible content omitted; recent content retained.]\n"
    content_capacity = available - len(label)
    if content_capacity <= 0:
        raise ConfigError("project root and current task leave no room for the truncation label")
    return prefix + label + text[-content_capacity:] + suffix


def _now_iso(clock: Callable[[], datetime]) -> str:
    value = clock()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _root_still_matches(project: ProjectBinding) -> bool:
    try:
        current = Path(project.root).resolve(strict=True)
    except OSError:
        return False
    return current.is_dir() and os.path.normcase(str(current)) == project.root


class FailoverBridge:
    """Poll both configured directions and dispatch confirmed limits exactly once."""

    def __init__(self, config: FailoverConfig, desktop: DesktopAdapter, orca: OrcaAdapter,
                 state: StateStore, clock: Callable[[], datetime] | None = None):
        self.config = config
        self.desktop = desktop
        self.orca = orca
        self.state = state
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self._streaks: dict[str, tuple[str, int]] = {}
        self._orca_probe: AdapterResult | None = None

    def _desktop_key(self, project: ProjectBinding) -> str:
        return f"claude:{project.id}:{project.desktop.process_image}"

    def _orca_key(self, project: ProjectBinding, session_id: str) -> str:
        return f"orca:{project.id}:{session_id}"

    def _base_record(self, framework: str, source_identity: str, runtime_identity: str,
                     project: ProjectBinding, phase: str) -> dict[str, Any]:
        return {
            "framework": framework,
            "source_identity": source_identity,
            "runtime_identity": runtime_identity,
            "project_root": project.root,
            "phase": phase,
            "suppression": "awaiting_working" if phase == "awaiting_working" else None,
        }

    def _active_orca_session(self, project: ProjectBinding) -> str | None:
        active = self.state.active_session(project.id)
        return active or project.orca_session_id

    def poll_once(self) -> list[dict[str, str]]:
        """Perform one read/poll cycle. This method never sleeps or hides unsupported surfaces."""
        results: list[dict[str, str]] = []
        if self._orca_probe is None:
            try:
                self._orca_probe = self.orca.probe()
            except Exception:
                self._orca_probe = AdapterResult("unsupported_surface", reason="orca_probe_failed")
        orca_ready = self._orca_probe.status == "ok"
        if not orca_ready:
            results.append({"status": self._orca_probe.status,
                            "reason": self._orca_probe.reason or "orca_cli_unavailable"})

        for project in self.config.projects:
            if not _root_still_matches(project):
                results.append({"project": project.id, "status": "root_mismatch",
                                "reason": "configured_project_root_changed"})
                continue

            if not orca_ready:
                results.append({"project": project.id, "status": "unsupported_surface",
                                "reason": self._orca_probe.reason or "orca_cli_unavailable"})
                continue

            try:
                desktop_observation = self.desktop.observe(project)
            except Exception:
                desktop_observation = Observation("unsupported_surface", self._desktop_key(project),
                                                  "", reason="desktop_adapter_failed")
            results.extend(self._consume_observation("claude", project, desktop_observation))
            session_id = self._active_orca_session(project)
            if not session_id:
                results.append({"project": project.id, "status": "waiting_for_orca_session",
                                "reason": "no_configured_or_bridge_launched_session"})
                continue
            try:
                orca_observation = self.orca.observe(project, session_id)
            except Exception:
                orca_observation = Observation("unsupported_surface", self._orca_key(project, session_id),
                                               session_id, reason="orca_adapter_failed")
            results.extend(self._consume_observation("orca", project, orca_observation))
        return results

    def _consume_observation(self, framework: str, project: ProjectBinding,
                             observation: Observation) -> list[dict[str, str]]:
        result: list[dict[str, str]] = []
        if observation.status != "ok":
            self._streaks.pop(observation.source_identity, None)
            return [{"project": project.id, "framework": framework,
                     "status": observation.status,
                     "reason": observation.reason or observation.status}]
        if not _root_still_matches(project):
            self._streaks.pop(observation.source_identity, None)
            return [{"project": project.id, "framework": framework, "status": "root_mismatch",
                     "reason": "project_root_changed_before_observation"}]
        if observation.limit_marker_id and observation.working_marker_id:
            self._streaks.pop(observation.source_identity, None)
            return [{"project": project.id, "framework": framework, "status": "unsupported_surface",
                     "reason": "conflicting_working_and_limit_markers"}]

        route_key = (self._desktop_key(project) if framework == "claude" else
                     self._orca_key(project, observation.source_identity.removeprefix("orca:")))
        record = self.state.route(route_key)
        phase = record.get("phase", "awaiting_working")
        if observation.working_marker_id:
            self._streaks.pop(route_key, None)
            updated = self._base_record(framework, observation.source_identity,
                                       observation.runtime_identity, project, "armed")
            if "last_handoff" in record:
                updated["last_handoff"] = record["last_handoff"]
            self.state.put_route(route_key, updated)
            return [{"project": project.id, "framework": framework, "status": "armed",
                     "reason": "configured_non_limit_working_marker"}]

        marker_id = observation.limit_marker_id
        if marker_id is None:
            self._streaks.pop(route_key, None)
            return [{"project": project.id, "framework": framework,
                     "status": "waiting_for_working" if phase != "armed" else "watching",
                     "reason": "no_configured_marker"}]
        if phase != "armed":
            self._streaks.pop(route_key, None)
            return [{"project": project.id, "framework": framework, "status": "suppressed",
                     "reason": "requires_fresh_working_to_limit_transition"}]

        prior = self._streaks.get(route_key)
        count = prior[1] + 1 if prior and prior[0] == marker_id else 1
        self._streaks[route_key] = (marker_id, count)
        if count < 2:
            return [{"project": project.id, "framework": framework, "status": "confirming_limit",
                     "reason": "one_consecutive_explicit_limit_observation"}]

        if not _root_still_matches(project):
            self._streaks.pop(route_key, None)
            return [{"project": project.id, "framework": framework, "status": "root_mismatch",
                     "reason": "project_root_changed_before_dispatch"}]
        payload = build_handoff_payload(project, observation.visible_text)
        target_framework = "orca" if framework == "claude" else "claude"
        target_key = None
        target_identity = project.worktree_selector if target_framework == "orca" else self._desktop_key(project)
        if target_framework == "claude":
            check = self.desktop.can_send(project)
            if check.status != "ok":
                self._streaks.pop(route_key, None)
                return [{"project": project.id, "framework": framework,
                         "status": check.status, "reason": check.reason or "target_desktop_not_confirmed"}]
            target_identity = check.target_identity or target_identity
            target_key = self._desktop_key(project)

        timestamp = _now_iso(self.clock)
        started = self._base_record(framework, observation.source_identity,
                                    observation.runtime_identity, project, "dispatch_started")
        started["last_handoff"] = {
            "status": "dispatch_started",
            "source_identity": observation.source_identity,
            "source_runtime_identity": observation.runtime_identity,
            "project_root": project.root,
            "target_framework": target_framework,
            "target_identity": target_identity,
            "target_session_handle": None,
            "timestamp": timestamp,
            "reason": f"explicit_limit_marker:{marker_id}",
        }
        self.state.put_route(route_key, started)
        if target_key:
            target_record = self._base_record("claude", self._desktop_key(project),
                                              "configured target not yet observed", project,
                                              "awaiting_working")
            self.state.put_route(target_key, target_record)

        try:
            if framework == "claude":
                dispatch = self.orca.launch(project, payload)
            else:
                dispatch = self.desktop.send(project, payload)
        except Exception:
            dispatch = AdapterResult("dispatch_uncertain", reason="adapter_exception_after_dispatch_started")
        if dispatch.status != "ok":
            uncertain = dict(started)
            uncertain["phase"] = "dispatch_uncertain"
            handoff = dict(uncertain["last_handoff"])
            handoff["status"] = "dispatch_uncertain"
            uncertain["last_handoff"] = handoff
            self.state.put_route(route_key, uncertain)
            self._streaks.pop(route_key, None)
            return [{"project": project.id, "framework": framework,
                     "status": "dispatch_uncertain", "reason": dispatch.reason or "check_target_before_retry"}]

        completed = dict(started)
        completed["phase"] = "dispatched"
        handoff = dict(completed["last_handoff"])
        handoff["status"] = "dispatched"
        handoff["target_identity"] = dispatch.target_identity or target_identity
        handoff["target_session_handle"] = dispatch.target_session_handle
        completed["last_handoff"] = handoff
        self.state.put_route(route_key, completed)
        if framework == "claude":
            if not dispatch.target_session_handle:
                # A successful-looking response without an exact handle is not a safe dispatch.
                uncertain = dict(completed)
                uncertain["phase"] = "dispatch_uncertain"
                handoff = dict(uncertain["last_handoff"])
                handoff["status"] = "dispatch_uncertain"
                uncertain["last_handoff"] = handoff
                self.state.put_route(route_key, uncertain)
                return [{"project": project.id, "framework": framework,
                         "status": "dispatch_uncertain", "reason": "launch_result_missing_session_handle"}]
            self.state.set_active_session(project.id, dispatch.target_session_handle)
            target_key = self._orca_key(project, dispatch.target_session_handle)
            target_record = self._base_record("orca", dispatch.target_session_handle,
                                              dispatch.target_identity or dispatch.target_session_handle,
                                              project, "awaiting_working")
            self.state.put_route(target_key, target_record)
        self._streaks.pop(route_key, None)
        return [{"project": project.id, "framework": framework, "status": "dispatched",
                 "reason": f"explicit_limit_marker:{marker_id}"}]


def json_pointer(value: Any, pointer: str) -> Any:
    """Read a validated RFC 6901 pointer without inferring Orca response field names."""
    if pointer == "":
        return value
    current = value
    for raw_part in pointer.lstrip("/").split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            if not part.isdecimal():
                raise KeyError(pointer)
            current = current[int(part)]
        elif isinstance(current, dict) and part in current:
            current = current[part]
        else:
            raise KeyError(pointer)
    return current
