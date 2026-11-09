#!/usr/bin/env python3
"""Build a relocatable native Hatchet/Prefect worker runtime for Windows or Linux x86_64.

    python3 worker/ci/build-native-runtime.py --os windows
    python3 worker/ci/build-native-runtime.py --os linux

Build each target with CPython 3.12 on its native OS so pip evaluates platform and Python-version
markers correctly: Windows on Windows, Linux on Linux. A Linux build can run in Docker from Windows.
The matching Confluence runtime archive is reused as the bundled CPython when available, avoiding
a second interpreter download; otherwise CPython is fetched and checksum-verified.
"""
import argparse
import fnmatch
import gzip
import hashlib
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from runtime_common import CPYTHON, PYTHON_VERSION, download

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
REQUIREMENTS = ROOT / 'agents' / 'requirements.txt'
DROP_DIRS = {
    '.cache', '.git', '.hypothesis', '.mypy_cache', '.pytest_cache', '.ruff_cache', '.tox',
    '.venv', '__pycache__', 'build', 'dist', 'env', 'node_modules', 'secrets', 'target',
    'test', 'tests', 'venv', 'idlelib', 'tkinter', 'turtledemo', 'ensurepip', 'lib2to3',
    'pydoc_data', 'setuptools', 'pip', 'tcl', 'tcl8.6', 'tk8.6', 'share', 'man',
}
DROP_PATTERNS = (
    '*.egg-info', 'test_*.py', '*_test.py', '*_smoke.py', 'smoke_*.py',
    '.env', '.env.*', '*.pyc', '*.pyo', '*.pyi', '*.pdb', '*.lib', '*.a', '*.whl',
    '*.pem', '*.key', '*.p12', '*.pfx', '*.secret', '*.token',
)
DROP_FILES = {'_tkinter.pyd', 'tcl86t.dll', 'tk86t.dll', 'turtle.py'}
PYTHON_MODULES = ROOT / 'agents'
AGENT_TREES = ('flowkit', 'agentdriver')



def excluded(path):
    if path.name == 'cacert.pem' and path.parent.name == 'certifi':
        return False
    if path.name in DROP_DIRS or path.name in DROP_FILES:
        return True
    return any(fnmatch.fnmatchcase(path.name, pattern) for pattern in DROP_PATTERNS)


def prune(root):
    for path in sorted(root.rglob('*'), key=lambda item: -len(item.parts)):
        if not path.exists() or not excluded(path):
            continue
        if path.is_dir():
            shutil.rmtree(path, ignore_errors=True)
        else:
            path.unlink()


def copy_agent_sources(stage):
    target = stage / 'agents'
    target.mkdir(parents=True)
    for source in PYTHON_MODULES.iterdir():
        if source.is_file() and source.suffix == '.py' and not excluded(source):
            shutil.copyfile(source, target / source.name)
    for folder in AGENT_TREES:
        shutil.copytree(PYTHON_MODULES / folder, target / folder,
                        ignore=lambda _directory, names: {name for name in names if excluded(Path(name))})
    prune(target)

def extract_python(archive_path, stage):
    with tarfile.open(archive_path, 'r:*') as archive:
        members = [member for member in archive.getmembers()
                   if member.name.rstrip('/') == 'python' or member.name.startswith('python/')]
        if not members:
            raise RuntimeError(f'{archive_path} does not contain a python/ runtime')
        python_modes = {member.name.rstrip('/'): member.mode for member in members}
        archive.extractall(stage, members=members, filter='tar')
    return python_modes


def confluence_runtime(os_name, out):
    candidates = (
        Path(out) / f'confluence-runtime-{os_name}.tar.gz',
        ROOT / 'dist' / 'worker' / f'confluence-runtime-{os_name}.tar.gz',
    )
    return next((path for path in candidates if path.is_file()), None)


def write_archive(stage, out, python_modes):
    out.parent.mkdir(parents=True, exist_ok=True)

    def normalized(member):
        member.uid = member.gid = 0
        member.uname = member.gname = ''
        member.mtime = 0
        member.pax_headers = {}
        executable = python_modes.get(member.name, 0) & 0o111
        member.mode = 0o755 if member.isdir() or executable else 0o644
        return member

    with out.open('wb') as raw, gzip.GzipFile(filename='', mode='wb', fileobj=raw, compresslevel=9, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as archive:
            for path in sorted(stage.rglob('*'), key=lambda item: item.relative_to(stage).as_posix()):
                archive.add(path, arcname=path.relative_to(stage).as_posix(), recursive=False, filter=normalized)


def build(os_name, out):
    host = 'windows' if sys.platform == 'win32' else 'linux' if sys.platform.startswith('linux') else None
    if host != os_name:
        raise RuntimeError(
            f'Build the {os_name} native runtime on {os_name}; pip evaluates platform markers on the build host. '
            'Use Docker for a Linux build from Windows.'
        )
    if sys.version_info[:2] != tuple(map(int, PYTHON_VERSION.split('.'))):
        raise RuntimeError(
            f'Build with CPython {PYTHON_VERSION}; pip evaluates Python-version markers on the build interpreter.'
        )
    name, sha256 = CPYTHON[os_name]
    with tempfile.TemporaryDirectory(prefix='native-runtime-') as temporary:
        work = Path(temporary)
        stage = work / 'stage'
        stage.mkdir()
        base = confluence_runtime(os_name, out)
        if base:
            print(f'Reusing CPython from {base}', flush=True)
            python_modes = extract_python(base, stage)
        else:
            with tarfile.open(download(name, sha256, work)) as archive:
                python_modes = {member.name.rstrip('/'): member.mode for member in archive.getmembers()}
                archive.extractall(stage, filter='tar')
        prune(stage / 'python')
        if os_name == 'linux':
            interpreter = stage / 'python' / 'bin' / 'python3'
            if interpreter.is_symlink():
                executable = interpreter.resolve()
                interpreter.unlink()
                shutil.copyfile(executable, interpreter)
            for extra in ('bin/python', 'bin/python3.12', 'lib/libtcl9.0.so', 'lib/libtcl9tk9.0.so'):
                (stage / 'python' / extra).unlink(missing_ok=True)

        packages = stage / 'packages'
        subprocess.run([
            sys.executable, '-m', 'pip', 'install', '--quiet', '--disable-pip-version-check', '--no-compile',
            '--target', str(packages), '--implementation', 'cp', '--python-version', PYTHON_VERSION,
            '--abi', 'cp312', '--only-binary=:all:', '-r', str(REQUIREMENTS),
        ], check=True)
        prune(packages)
        if not (packages / 'certifi' / 'cacert.pem').is_file():
            raise RuntimeError('Native worker runtime is missing certifi CA roots')
        copy_agent_sources(stage)

        out = Path(out) / f'native-runtime-{os_name}.tar.gz'
        print(f'Writing {out}', flush=True)
        write_archive(stage, out, python_modes)
    digest = hashlib.sha256(out.read_bytes()).hexdigest()
    Path(f'{out}.sha256').write_text(f'{digest}  {out.name}\n', encoding='utf-8')
    print(f'{out.name}: {out.stat().st_size / 1e6:.1f} MB, version {digest[:16]}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--os', required=True, choices=sorted(CPYTHON))
    parser.add_argument('--out', default=str(ROOT / 'dist' / 'worker'))
    options = parser.parse_args()
    build(options.os, Path(options.out))
