"""Portable, bounded file trees and causal step publications. No engine scheduling lives here."""
import base64
import json
import os
import re
from pathlib import Path, PurePosixPath

from .restart import atomic_json

MAX_BYTES = 64 * 1024 * 1024
MAX_FILES = 10_000


def validate(files):
    if not isinstance(files, dict) or len(files) > MAX_FILES:
        raise ValueError('Workspace exceeds 10000 files')
    seen, total = set(), 0
    for name, entry in files.items():
        if (not isinstance(name, str) or not name or '\\' in name or ':' in name or
                name.startswith('/') or any(p in ('', '.', '..') for p in name.split('/')) or
                any(ord(c) < 32 for c in name) or
                any(p.endswith(('.', ' ')) or re.fullmatch(r'(?i)(con|prn|aux|nul|com[1-9]|lpt[1-9])(\..*)?', p) for p in name.split('/'))):
            raise ValueError(f'Unsafe workspace path: {name!r}')
        key = name.casefold()
        if key in seen: raise ValueError(f'Case collision: {name}')
        seen.add(key)
        if not isinstance(entry, dict) or type(entry.get('mode')) is not int or not 0 <= entry['mode'] <= 0o777:
            raise ValueError(f'Invalid file entry: {name}')
        try: total += len(base64.b64decode(entry['data'], validate=True))
        except (ValueError, KeyError, TypeError): raise ValueError(f'Invalid file content: {name}') from None
        if total > MAX_BYTES: raise ValueError('Workspace exceeds 64 MiB transfer limit')
    for name in seen:
        if any(str(p) in seen for p in PurePosixPath(name).parents if str(p) != '.'):
            raise ValueError(f'File/directory collision: {name}')
    return files


def snapshot(root: Path):
    files, total = {}, 0
    for path in sorted(root.rglob('*')):
        if path.is_symlink(): raise ValueError(f'Symlinks cannot be transferred: {path.relative_to(root)}')
        if not path.is_file(): continue
        name = path.relative_to(root).as_posix()
        if name.split('/')[0].startswith('.flow-'): continue
        total += path.stat().st_size
        if total > MAX_BYTES or len(files) >= MAX_FILES: raise ValueError('Workspace exceeds transfer limits (64 MiB / 10000 files)')
        files[name] = {'data': base64.b64encode(path.read_bytes()).decode(), 'mode': path.stat().st_mode & 0o777}
    return validate(files)


def restore(files, root: Path):
    validate(files)
    root.mkdir(parents=True, exist_ok=True)
    for name, entry in files.items():
        path = root / name
        # A worker workspace is new, but retain this guard for callers using existing folders.
        if any(p.is_symlink() for p in [path, *path.parents] if p != root.parent):
            raise ValueError(f'Symlink destination: {name}')
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(base64.b64decode(entry['data']))
        if os.name != 'nt': path.chmod(entry['mode'])


def publish(root: Path, step: str, before, after):
    validate(after)
    delta = {name: after.get(name) for name in before.keys() | after.keys() if before.get(name) != after.get(name)}
    atomic_json(root / '.flow-artifacts' / f'{step}.json', delta)


def ancestor_ids(flow, step):
    found = set()
    def visit(sid):
        for parent in flow.steps[sid].needs:
            if parent not in found:
                found.add(parent)
                visit(parent)
    visit(step)
    return found


def ancestor_files(flow, step, root: Path):
    ancestors = ancestor_ids(flow, step)
    history = {}
    for sid in flow.steps:  # definitions are topologically ordered
        if sid not in ancestors: continue
        path = root / '.flow-artifacts' / f'{sid}.json'
        if not path.exists(): raise ValueError(f'Files for dependency {sid} are unavailable; restart the whole flow')
        prior = ancestor_ids(flow, sid)
        for name, value in json.loads(path.read_text(encoding='utf-8')).items():
            # Keep causally maximal writers only. A later common descendant can resolve a change.
            history[name] = [(writer, old) for writer, old in history.get(name, []) if writer not in prior] + [(sid, value)]
    result = {}
    for name, writers in history.items():
        value = writers[0][1]
        if any(other != value for _, other in writers):
            raise ValueError(f'Workspace conflict for {name}: ' + ', '.join(w for w, _ in writers))
        if value is not None: result[name] = value
    return validate(result)
