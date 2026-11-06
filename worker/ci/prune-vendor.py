#!/usr/bin/env python3
"""Shrink worker/vendor after `cargo vendor`: drop the import libraries of Windows targets this worker is not built for
(32-bit, ARM, GNU/MinGW). The sources stay, so Cargo.lock still resolves; checksums are rewritten for the files left.
Usage: python3 worker/ci/prune-vendor.py [vendor-dir]"""
import hashlib, json, sys
from pathlib import Path

vendor = Path(sys.argv[1] if len(sys.argv) > 1 else Path(__file__).resolve().parents[1] / 'vendor')
FOREIGN = ('windows_i686_', 'windows_aarch64_', 'windows_x86_64_gnu', 'winapi-i686-', 'winapi-x86_64-pc-windows-gnu')
removed = 0
for crate in sorted(p for p in vendor.iterdir() if p.is_dir() and p.name.startswith(FOREIGN)):
    checksum = crate / '.cargo-checksum.json'
    data = json.loads(checksum.read_text())
    for path in list(crate.rglob('*')):
        if path.is_file() and path.suffix in ('.a', '.lib'):
            data['files'].pop(path.relative_to(crate).as_posix(), None)
            removed += path.stat().st_size; path.unlink()
    checksum.write_text(json.dumps(data, separators=(',', ':')))
print(f'removed {removed // 1024} KiB of import libraries for other Windows targets')
