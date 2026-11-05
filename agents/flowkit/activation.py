"""Flow drafts, layout sidecars and immutable active snapshots.

- Draft: flows/<name>.yaml, freely editable (API or by hand). Saving one changes nothing that runs.
- Layout: flows/<name>.layout.json, optional graph positions; never part of a revision.
- Active snapshot: <FLOW_STATE_DIR>/active/<name>/<sha256>.yaml, an immutable copy of an
  activated draft. <FLOW_STATE_DIR>/manifest.json records, per flow, the requested revision and
  the revision each engine actually serves, so an engine whose activation failed keeps its old one.

Workers, Trigger.dev code generation, card labelling and launches load active snapshots only.
FLOW_STATE_DIR defaults to <repo>/.flow-state. A worker started by the flow host gets the exact
revisions it serves in FLOW_ACTIVE_REVISIONS (inherited by Prefect's flow-run subprocesses).
"""

import hashlib
import json
import math
import os
import tempfile
import time
from pathlib import Path
from typing import Any, NamedTuple

from .definition import FLOW_NAME, STEP_ID, Flow, FlowError, parse_text

ROOT = Path(__file__).resolve().parents[2]
ENGINES = ("hatchet", "prefect", "trigger")


class Draft(NamedTuple):
    text: str  # for display; undecodable bytes are replaced when error is set
    revision: str  # of the file's bytes
    error: str | None = None  # the file is not UTF-8 text


def revision(data: str | bytes) -> str:
    return "sha256:" + hashlib.sha256(data.encode("utf-8") if isinstance(data, str) else data).hexdigest()


def atomic_write(path: Path, data: str | bytes) -> None:
    """Readers see the old or the new file, never a partial one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False) as file:
        file.write(data.encode("utf-8") if isinstance(data, str) else data)
        file.flush()
        os.fsync(file.fileno())
    try:
        for attempt in range(40):
            try:
                os.replace(file.name, path)
                break
            except PermissionError:  # Windows: a reader has the target open for a moment
                if attempt == 39:
                    raise
                time.sleep(0.05)
    except BaseException:
        Path(file.name).unlink(missing_ok=True)
        raise


def atomic_write_once(path: Path, data: str) -> None:
    """Publish immutable content exactly once, including across competing activations."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=f'.{path.name}.', suffix='.tmp', delete=False) as file:
        temporary = Path(file.name)
        file.write(data.encode('utf-8'))
        file.flush(); os.fsync(file.fileno())
    try:
        try: os.link(temporary, path)
        except FileExistsError: pass
    finally:
        temporary.unlink(missing_ok=True)


def check_layout(layout: Any) -> dict:
    """{"version": 1, "positions": {step id: {"x", "y"}}, "viewport"?: {"x", "y", "zoom"}}."""
    finite = lambda v: type(v) in (int, float) and math.isfinite(v) and abs(v) < 1e9
    if not isinstance(layout, dict) or layout.get("version") != 1 or set(layout) - {"version", "positions", "viewport"}:
        raise ValueError('layout must be {"version": 1, "positions": {...}}')
    positions = layout.get("positions", {})
    if not isinstance(positions, dict) or len(positions) > 1000:
        raise ValueError("layout positions must map at most 1000 step ids to {x, y}")
    for sid, point in positions.items():
        if not STEP_ID.match(sid) or not isinstance(point, dict) or set(point) != {"x", "y"} or not all(map(finite, point.values())):
            raise ValueError(f"layout position for {sid!r} must be {{x, y}} numbers")
    viewport = layout.get("viewport")
    if viewport is not None and (not isinstance(viewport, dict) or set(viewport) - {"x", "y", "zoom"} or not all(map(finite, viewport.values()))):
        raise ValueError("layout viewport must be {x, y, zoom} numbers")
    return layout


class Store:
    def __init__(self, drafts: Path | None = None, state: Path | None = None) -> None:
        from .definition import FLOWS_DIR

        self.drafts = Path(drafts or FLOWS_DIR).resolve()
        # Absolute: workers get it in their environment and may run from another folder.
        self.state = Path(state or os.environ.get("FLOW_STATE_DIR") or ROOT / ".flow-state").resolve()

    # Drafts and layouts
    def draft_names(self) -> list[str]:
        return sorted(p.stem for p in self.drafts.glob("*.yaml") if FLOW_NAME.match(p.stem))

    def draft(self, name: str) -> Draft | None:
        """The draft file, or None when there is none."""
        try:
            data = (self.drafts / f"{name}.yaml").read_bytes()
        except FileNotFoundError:
            return None
        try:
            return Draft(data.decode("utf-8"), revision(data))
        except UnicodeDecodeError as exc:
            return Draft(data.decode("utf-8", errors="replace"), revision(data),
                         f"{name}.yaml is not UTF-8 text (byte {exc.start}); save it as UTF-8")

    def write_draft(self, name: str, text: str) -> str:
        atomic_write(self.drafts / f"{name}.yaml", text)
        return revision(text)

    def delete_draft(self, name: str) -> None:
        (self.drafts / f"{name}.yaml").unlink(missing_ok=True)
        (self.drafts / f"{name}.layout.json").unlink(missing_ok=True)

    def layout(self, name: str) -> dict | None:
        try:
            return check_layout(json.loads((self.drafts / f"{name}.layout.json").read_text(encoding="utf-8")))
        except (OSError, ValueError):
            return None  # missing or unusable: the editor lays the graph out itself

    def write_layout(self, name: str, layout: dict) -> None:
        atomic_write(self.drafts / f"{name}.layout.json", json.dumps(check_layout(layout), indent=1) + "\n")

    # Active snapshots
    @property
    def manifest_path(self) -> Path:
        return self.state / "manifest.json"

    def manifest(self) -> dict:
        try:
            data = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"version": 1, "flows": {}}
        if not isinstance(data, dict) or not isinstance(data.get("flows"), dict):
            raise ValueError(f"{self.manifest_path} is damaged; restore it or remove the state folder to re-import the drafts")
        return data

    def write_manifest(self, manifest: dict) -> None:
        atomic_write(self.manifest_path, json.dumps(manifest, indent=1, sort_keys=True) + "\n")

    def snapshot_path(self, name: str, rev: str) -> Path:
        return self.state / "active" / name / f"{rev.removeprefix('sha256:')}.yaml"

    def asset_manifest_path(self, name: str, rev: str) -> Path:
        if not FLOW_NAME.fullmatch(name) or not rev.startswith('sha256:') or len(rev) != 71 or any(c not in '0123456789abcdef' for c in rev[7:]):
            raise ValueError('Invalid immutable asset reference')
        return self.state / 'active' / name / (rev[7:] + '.assets.json')

    def asset_reference(self, name: str, rev: str) -> dict:
        path = self.asset_manifest_path(name, rev).with_suffix('.assetref')
        try: digest = path.read_text(encoding='utf-8').strip()
        except OSError as exc:
            raise FlowError('Immutable flow assets are unavailable; reactivate the flow') from exc
        if not isinstance(digest, str) or not digest.startswith('sha256:') or len(digest) != 71 or any(c not in '0123456789abcdef' for c in digest[7:]):
            raise FlowError('Immutable flow asset snapshot has an invalid digest')
        return {'name': name, 'revision': rev, 'digest': digest}

    def snapshot_assets(self, ref: dict) -> dict:
        from .artifacts import validate
        if not isinstance(ref, dict) or set(ref) != {'name', 'revision', 'digest'}:
            raise ValueError('Invalid immutable asset reference')
        path = self.asset_manifest_path(ref['name'], ref['revision'])
        try: data = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc:
            raise FlowError('Immutable flow assets are unavailable; reactivate the flow') from exc
        if not isinstance(data, dict) or set(data) != {'digest', 'files'}:
            raise FlowError('Immutable flow asset snapshot is invalid')
        canonical = json.dumps(data['files'], sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        if revision(canonical) != data['digest'] or data['digest'] != ref['digest']:
            raise FlowError('Immutable flow asset snapshot does not match its digest')
        return validate(data['files'])

    def load_bound_flow(self, ref: dict, definition: dict) -> Flow:
        from .definition import parse
        self.snapshot_assets(ref)
        expected = self.asset_reference(ref['name'], ref['revision'])
        if ref != expected: raise FlowError('Flow asset revision does not match its stored reference')
        stored = parse_text(self.snapshot_text(ref['name'], ref['revision']))
        incoming = parse(definition)
        if incoming.to_json() != stored.to_json():
            raise FlowError('Flow definition does not match its immutable revision')
        stored._asset_snapshot = ref
        return stored

    def current_assets(self) -> tuple[dict, str]:
        from .artifacts import snapshot
        files = {name: value for name, value in snapshot(self.drafts).items()
                 if '__pycache__' not in name.split('/') and not name.endswith(('.pyc', '.pyo'))
                 and ('/' in name or not (name.endswith('.yaml') or name.endswith('.layout.json')))}
        for asset_name in files:
            parts = asset_name.lower().split('/')
            base = parts[-1]
            if (any(part.startswith('.') for part in parts)
                    or base.endswith(('.pem', '.key', '.p12', '.pfx'))
                    or base.startswith(('id_rsa', 'id_ed25519', 'id_ecdsa'))
                    or any(word in base for word in ('credential', 'secret', 'password', 'private_key', 'api_token'))):
                raise FlowError(f'Possible credential or secret file in flow assets: {asset_name}; move it outside the flows directory')
        canonical = json.dumps(files, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
        expected = revision(canonical)
        return files, expected

    def write_snapshot(self, name: str, text: str) -> str:
        rev = revision(text)
        path = self.snapshot_path(name, rev)
        assets_path = self.asset_manifest_path(name, rev)
        files, expected = self.current_assets()
        if not assets_path.exists():
            atomic_write_once(assets_path, json.dumps({'digest': expected, 'files': files}, ensure_ascii=False))
        try: stored = json.loads(assets_path.read_text(encoding='utf-8'))
        except (OSError, ValueError) as exc: raise FlowError('Immutable flow asset snapshot is unavailable') from exc
        if not isinstance(stored, dict) or stored.get('digest') != expected:
            raise FlowError('Flow assets have changed for this revision; change the flow YAML before activating')
        reference_path = assets_path.with_suffix('.assetref')
        atomic_write_once(reference_path, expected)
        if reference_path.read_text(encoding='utf-8').strip() != expected:
            raise FlowError('Flow assets have changed for this revision; change the flow YAML before activating')
        if not path.exists():  # content-addressed and never rewritten
            atomic_write(path, text)
        return rev

    def snapshot_text(self, name: str, rev: str) -> str:
        data = self.snapshot_path(name, rev).read_bytes()
        if revision(data) != rev:
            raise FlowError(f"active snapshot of {name} does not match its revision")
        return data.decode("utf-8")

    def active_revisions(self, engine: str | None = None, manifest: dict | None = None) -> dict[str, str]:
        """{flow: revision} that an engine serves (engine None: the most recently activated revisions)."""
        flows = (manifest or self.manifest())["flows"]
        if engine is None:
            return {name: entry["revision"] for name, entry in flows.items() if entry.get("revision")}
        return {name: entry["engines"][engine]["revision"] for name, entry in flows.items()
                if (entry.get("engines", {}).get(engine) or {}).get("revision")}

    def load(self, revisions: dict[str, str]) -> tuple[dict[str, Flow], dict[str, str]]:
        flows, broken = {}, {}
        for name, rev in sorted(revisions.items()):
            try:
                flow = parse_text(self.snapshot_text(name, rev))
                if flow.name != name:
                    raise FlowError(f"snapshot is named {flow.name!r}")
                ref = self.asset_reference(name, rev)
                self.snapshot_assets(ref)  # fail closed if an old revision has no bound assets
                flow._asset_snapshot = ref
                flows[name] = flow
            except (FlowError, OSError) as exc:
                broken[name] = str(exc)
        return flows, broken

    def load_active(self, engine: str | None = None) -> tuple[dict[str, Flow], dict[str, str]]:
        pinned = os.environ.get("FLOW_ACTIVE_REVISIONS") if engine else None
        return self.load(json.loads(pinned) if pinned else self.active_revisions(engine))

    def migrate(self) -> list[str]:
        """First start (no manifest yet): activate every valid draft. Nothing runs yet, so nothing is interrupted."""
        if self.manifest_path.exists():
            return []
        from .document import analyze

        manifest = {"version": 1, "flows": {}}
        for name in self.draft_names():
            draft = self.draft(name)
            if draft.error or analyze(draft.text, name)[1]:
                continue  # stays a broken draft
            rev = self.write_snapshot(name, draft.text)
            if rev != draft.revision:
                continue
            manifest["flows"][name] = {"revision": rev, "engines": {
                engine: {"revision": rev, "state": "active", "error": None} for engine in ENGINES}}
        self.write_manifest(manifest)
        return list(manifest["flows"])


def load_active(engine: str | None = None) -> tuple[dict[str, Flow], dict[str, str]]:
    return Store().load_active(engine)
