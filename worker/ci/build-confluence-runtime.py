#!/usr/bin/env python3
"""Build what lets an agent-worker import Confluence pages with no Python on the PC: a relocatable CPython, the pinned
confluence-markdown-exporter and the flowkit code around it, as one archive for dist/worker/.

    python3 worker/ci/build-confluence-runtime.py --os windows
    python3 worker/ci/build-confluence-runtime.py --os linux

It builds for either OS from either OS (needs pip and network). Everything downloaded is pinned: CPython by its
checksum below, the packages by confluence-runtime.txt. Linux uses the glibc build, because the musl one can't load the
exporter's compiled packages. The pinned lxml wheel sets the minimum supported glibc version to 2.26.
"""
import argparse
import hashlib
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
RELEASE = 'https://github.com/astral-sh/python-build-standalone/releases/download/20261026/'
PYTHON_VERSION = '3.12'
CPYTHON = {
    'windows': ('cpython-3.12.16%2B20261026-x86_64-pc-windows-msvc-install_only_stripped.tar.gz', '1f2ab53880fc67ea323398c6388e05af28addc99a8e389209f08a51fdff4b149'),
    'linux': ('cpython-3.12.16%2B20261026-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz', 'ce4f6d0a6cfdb3bed6e44c2f9affd85943c76a42e25ad3a259002f31d6fb8a20'),
}
WHEEL_PLATFORMS = {'windows': ['win_amd64'], 'linux': ['manylinux_2_26_x86_64', 'manylinux_2_17_x86_64', 'manylinux2014_x86_64']}
# What the exporter never needs: most of the size that isn't the interpreter.
DROP = ['test', 'tests', 'idlelib', 'tkinter', 'turtledemo', 'ensurepip', 'lib2to3', 'pydoc_data', 'venv', 'pip', 'setuptools', 'tcl', 'tcl8.6', 'tk8.6', 'include', 'Scripts', '__pycache__', 'share', 'man']
DROP_FILES = ['_tkinter.pyd', 'tcl86t.dll', 'tk86t.dll', 'turtle.py']
FLOWKIT = ['__init__', 'artifacts', 'confluence_export', 'confluence_runner', 'definition', 'executor', 'restart', 'template', 'workers', 'fleet_registry', 'relay', 'capabilities']
AGENTDRIVER = ['__init__', 'events', 'process_tree']


def download(name, sha256, into):
    path = into / 'cpython.tar.gz'
    print(f'Downloading {name}', flush=True)
    with urllib.request.urlopen(RELEASE + name, timeout=300) as response, path.open('wb') as out:
        shutil.copyfileobj(response, out)
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != sha256: raise SystemExit(f'{name} does not match its checksum: {actual}')
    return path


def prune(root):
    for path in sorted(root.rglob('*'), key=lambda p: -len(p.parts)):
        if path.is_dir() and (path.name in DROP or path.name.startswith('config-')): shutil.rmtree(path, ignore_errors=True)
        elif path.is_file() and (path.name in DROP_FILES or path.suffix in ('.a', '.lib', '.pdb', '.pyi')): path.unlink()


def build(os_name, out):
    name, sha256 = CPYTHON[os_name]
    with tempfile.TemporaryDirectory(prefix='confluence-runtime-') as work:
        stage = Path(work) / 'stage'
        stage.mkdir()
        with tarfile.open(download(name, sha256, Path(work))) as archive:
            python_modes = {member.name.rstrip('/'): member.mode for member in archive.getmembers()}
            archive.extractall(stage, filter='tar')
        prune(stage / 'python')
        if os_name == 'linux':
            # Materialize this alias before removing its target: Linux preserves symlinks, while Windows may copy them.
            interpreter = stage / 'python' / 'bin' / 'python3'
            if interpreter.is_symlink():
                executable = interpreter.resolve()
                interpreter.unlink()
                shutil.copyfile(executable, interpreter)
            for extra in ('bin/python', 'bin/python3.12', 'lib/libtcl9.0.so', 'lib/libtcl9tk9.0.so'):
                (stage / 'python' / extra).unlink(missing_ok=True)
        packages = stage / 'packages'
        platforms = [flag for tag in WHEEL_PLATFORMS[os_name] for flag in ('--platform', tag)]
        subprocess.run([sys.executable, '-m', 'pip', 'install', '--quiet', '--no-deps', '--no-compile', '--target', str(packages), '--implementation', 'cp',
                        '--python-version', PYTHON_VERSION, '--abi', 'cp312', '--only-binary=:all:', *platforms, '-r', str(HERE / 'confluence-runtime.txt')], check=True)
        prune(packages)
        for folder, modules in (('flowkit', FLOWKIT), ('agentdriver', AGENTDRIVER)):
            target = stage / 'agents' / folder
            target.mkdir(parents=True)
            for module in modules:
                shutil.copyfile(ROOT / 'agents' / folder / f'{module}.py', target / f'{module}.py')
        out.parent.mkdir(parents=True, exist_ok=True)
        def permissions(member):
            # A Windows staging filesystem loses POSIX execute bits. Use the pinned archive's modes, not host stat().
            executable = python_modes.get(member.name, 0) & 0o111
            member.mode = 0o755 if member.isdir() or executable else 0o644
            return member

        print(f'Writing {out}', flush=True)
        with tarfile.open(out, 'w:gz', compresslevel=9) as archive:
            for part in ('python', 'packages', 'agents'):
                archive.add(stage / part, arcname=part, filter=permissions)
    digest = hashlib.sha256(out.read_bytes()).hexdigest()
    Path(f'{out}.sha256').write_text(f'{digest}  {out.name}\n', encoding='utf-8')
    print(f'{out.name}: {out.stat().st_size / 1e6:.1f} MB, version {digest[:16]}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--os', required=True, choices=sorted(CPYTHON))
    parser.add_argument('--out', default=str(ROOT / 'dist' / 'worker'))
    options = parser.parse_args()
    build(options.os, Path(options.out) / f'confluence-runtime-{options.os}.tar.gz')
