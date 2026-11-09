"""Pinned CPython and cross-target wheel helpers shared by portable worker runtimes."""
import hashlib
import shutil
import time
import urllib.error
import urllib.request
from pathlib import Path

RELEASE = 'https://github.com/astral-sh/python-build-standalone/releases/download/20261026/'
PYTHON_VERSION = '3.12'
CPYTHON = {
    'windows': ('cpython-3.12.16%2B20261026-x86_64-pc-windows-msvc-install_only_stripped.tar.gz', '1f2ab53880fc67ea323398c6388e05af28addc99a8e389209f08a51fdff4b149'),
    'linux': ('cpython-3.12.16%2B20261026-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz', 'ce4f6d0a6cfdb3bed6e44c2f9affd85943c76a42e25ad3a259002f31d6fb8a20'),
}
WHEEL_PLATFORMS = {
    'windows': ['win_amd64'],
    'linux': ['manylinux_2_26_x86_64', 'manylinux_2_17_x86_64', 'manylinux2014_x86_64'],
}


def download(name, sha256, into):
    path = Path(into) / 'cpython.tar.gz'
    url = RELEASE + name
    for attempt in range(3):
        try:
            print(f'Downloading {name}', flush=True)
            with urllib.request.urlopen(url, timeout=300) as response, path.open('wb') as out:
                shutil.copyfileobj(response, out)
            break
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            retryable = not isinstance(exc, urllib.error.HTTPError) or exc.code in (500, 502, 503, 504)
            if not retryable or attempt == 2:
                raise
            path.unlink(missing_ok=True)
            delay = 2 ** (attempt + 1)
            print(f'Download failed ({exc}); retrying in {delay}s', flush=True)
            time.sleep(delay)
    actual = hashlib.sha256(path.read_bytes()).hexdigest()
    if actual != sha256:
        raise SystemExit(f'{name} does not match its checksum: {actual}')
    return path
