"""Issue and revoke per-PC Hatchet API tokens using a server-side admin session."""
from __future__ import annotations

import json
import os
import threading
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlsplit

import aiohttp


def _tenant_from_client_token(token: str) -> str:
    if not token:
        raise HatchetTokenError('Set HATCHET_CLIENT_TOKEN on the Board so it can determine the Hatchet tenant for worker-token issuance.')
    try:
        try:
            from hatchet_sdk.token import get_tenant_id_from_jwt
        except ImportError:
            import base64
            claims = token.split('.')
            if len(claims) != 3:
                raise ValueError('not a JWT')
            payload = claims[1] + '=' * ((4 - len(claims[1]) % 4) % 4)
            tenant_id = json.loads(base64.urlsafe_b64decode(payload)).get('sub')
        else:
            tenant_id = get_tenant_id_from_jwt(token)
        return str(uuid.UUID(tenant_id))
    except Exception:
        raise HatchetTokenError('Could not determine the Hatchet tenant from HATCHET_CLIENT_TOKEN; verify that the Board API token is valid.') from None


class HatchetTokenError(RuntimeError):
    """A safe-to-display explanation of a Hatchet token provisioning failure."""


@dataclass(frozen=True)
class IssuedWorkerToken:
    token: str
    token_id: str
    name: str


class HatchetTokenIssuer:
    def __init__(self, api_url: str, email: str, password: str, tenant_id: str | None = None,
                 session_factory=aiohttp.ClientSession):
        parts = urlsplit(api_url)
        if parts.scheme not in ('http', 'https') or not parts.hostname or parts.username or parts.password or parts.fragment:
            raise HatchetTokenError('Configure a valid Hatchet API URL in Admin before creating a Hatchet worker install.')
        if not email or not password:
            raise HatchetTokenError('Hatchet automatic token issuance is not configured: set HATCHET_ADMIN_EMAIL and HATCHET_ADMIN_PASSWORD on the Board server.')
        tenant_id = _tenant_from_client_token(os.environ.get('HATCHET_CLIENT_TOKEN', '')) if tenant_id is None else tenant_id
        try:
            tenant = str(uuid.UUID(tenant_id))
        except (ValueError, TypeError, AttributeError):
            raise HatchetTokenError('Could not determine the Hatchet tenant from HATCHET_CLIENT_TOKEN; verify that the Board API token is valid.') from None
        self.api_url = api_url.rstrip('/')
        self.email = email
        self.password = password
        self.tenant_id = tenant
        self.session_factory = session_factory

    @classmethod
    def from_environment(cls, api_url: str, tenant_id: str | None = None) -> 'HatchetTokenIssuer':
        email = os.environ.get('HATCHET_ADMIN_EMAIL', '').strip()
        password = os.environ.get('HATCHET_ADMIN_PASSWORD', '')
        return cls(api_url, email, password, tenant_id)

    def _url(self, path: str) -> str:
        return self.api_url + path

    async def _login(self, session: aiohttp.ClientSession) -> None:
        try:
            async with session.post(self._url('/api/v1/users/login'),
                                    json={'email': self.email, 'password': self.password},
                                    allow_redirects=False) as response:
                status = response.status
                await response.read()
        except aiohttp.ClientError:
            raise HatchetTokenError('Could not reach Hatchet for admin-session login; check the Hatchet API URL in Admin.') from None
        if not 200 <= status < 300:
            raise HatchetTokenError(f'Hatchet admin-session login failed (HTTP {status}); check HATCHET_ADMIN_EMAIL, HATCHET_ADMIN_PASSWORD, and the account’s Hatchet access.')

    async def issue_worker_token(self, worker_id: str) -> IssuedWorkerToken:
        name = f'agent-board:{worker_id}:{uuid.uuid4()}'
        tenant_tokens = '/api/v1/tenants/' + quote(self.tenant_id) + '/api-tokens'
        timeout = aiohttp.ClientTimeout(total=20, connect=5)
        async with self.session_factory(timeout=timeout) as session:
            await self._login(session)
            try:
                async with session.post(self._url(tenant_tokens), json={'name': name, 'readOnly': False},
                                        allow_redirects=False) as response:
                    status = response.status
                    payload = await response.json(content_type=None) if 200 <= status < 300 else None
            except (aiohttp.ClientError, ValueError):
                raise HatchetTokenError('Could not create a dedicated Hatchet worker token; check that the session account is an ADMIN or OWNER for the configured tenant.') from None
            if not 200 <= status < 300:
                raise HatchetTokenError(f'Hatchet refused to create a dedicated worker token (HTTP {status}); the session account must be an ADMIN or OWNER for the configured tenant.')
            token = payload.get('token') if isinstance(payload, dict) else None
            if not isinstance(token, str) or not token:
                raise HatchetTokenError('Hatchet returned no worker token; check the Hatchet API version and administrator permissions.')
            try:
                async with session.get(self._url(tenant_tokens), allow_redirects=False) as response:
                    status = response.status
                    listing = await response.json(content_type=None) if 200 <= status < 300 else None
            except (aiohttp.ClientError, ValueError):
                raise HatchetTokenError(f'Hatchet created a worker token but its ID could not be confirmed; revoke the token named {name!r} in Hatchet Admin before retrying.') from None
            if not 200 <= status < 300:
                raise HatchetTokenError(f'Hatchet created a worker token but its ID could not be confirmed (HTTP {status}); revoke the token named {name!r} in Hatchet Admin before retrying.')
            rows = listing.get('rows') if isinstance(listing, dict) else None
            matching = [row for row in rows if isinstance(row, dict) and row.get('name') == name] if isinstance(rows, list) else []
            if len(matching) != 1 or matching[0].get('readOnly') is not False:
                raise HatchetTokenError(f'Hatchet created a worker token but its ID could not be confirmed; revoke the token named {name!r} in Hatchet Admin before retrying.')
            metadata = matching[0].get('metadata')
            token_id = metadata.get('id') if isinstance(metadata, dict) else None
            try:
                token_id = str(uuid.UUID(token_id))
            except (ValueError, TypeError, AttributeError):
                raise HatchetTokenError(f'Hatchet created a worker token but its ID was invalid; revoke the token named {name!r} in Hatchet Admin before retrying.') from None
            return IssuedWorkerToken(token=token, token_id=token_id, name=name)

    async def revoke_worker_token(self, token_id: str) -> None:
        try:
            token_id = str(uuid.UUID(token_id))
        except (ValueError, TypeError, AttributeError):
            raise HatchetTokenError('The stored Hatchet API token ID is invalid; it was not sent to Hatchet.') from None
        timeout = aiohttp.ClientTimeout(total=20, connect=5)
        async with self.session_factory(timeout=timeout) as session:
            await self._login(session)
            path = '/api/v1/api-tokens/' + quote(token_id, safe='')
            try:
                async with session.post(self._url(path), allow_redirects=False) as response:
                    status = response.status
                    await response.read()
            except aiohttp.ClientError:
                raise HatchetTokenError('Could not reach Hatchet to revoke a per-PC worker token.') from None
        if status not in (204, 404):
            raise HatchetTokenError(f'Hatchet refused to revoke a per-PC worker token (HTTP {status}); check the Hatchet administrator session.')



def _store_path() -> Path:
    configured = os.environ.get('BOARD_HATCHET_TOKEN_STORE_PATH')
    if configured:
        return Path(configured)
    settings = Path(os.environ.get('BOARD_ENGINE_SETTINGS_PATH', '/data/engine-settings.json'))
    return settings.with_name('hatchet-worker-tokens.json')


_STORE_LOCK = threading.RLock()


def _read_store(path: Path) -> dict[str, dict[str, str]]:
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        raise HatchetTokenError('The private Hatchet token registry cannot be read; repair its Board data file before changing Fleet installs.') from None
    if not isinstance(value, dict) or any(not isinstance(key, str) or not isinstance(item, dict) for key, item in value.items()):
        raise HatchetTokenError('The private Hatchet token registry is invalid; repair its Board data file before changing Fleet installs.')
    return value


def _write_store(path: Path, records: dict[str, dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8', newline='\n') as handle:
        json.dump(records, handle, indent=2)
        handle.write('\n')
    os.replace(temporary, path)
    try:
        path.chmod(0o600)
    except OSError:
        pass


def remember_install_token(code: str, worker_id: str, credential: IssuedWorkerToken,
                           api_url: str, tenant_id: str) -> None:
    code = str(uuid.UUID(code))
    token_id = str(uuid.UUID(credential.token_id))
    record = {'worker_id': worker_id, 'token_id': token_id, 'name': credential.name,
              'api_url': api_url.rstrip('/'), 'tenant_id': str(uuid.UUID(tenant_id))}
    path = _store_path()
    with _STORE_LOCK:
        records = _read_store(path)
        records[code] = record
        _write_store(path, records)


def install_tokens_for_worker(worker_id: str) -> list[tuple[str, dict[str, str]]]:
    path = _store_path()
    with _STORE_LOCK:
        records = _read_store(path)
        return [(code, dict(record)) for code, record in records.items() if record.get('worker_id') == worker_id]


def install_token(code: str) -> dict[str, str] | None:
    path = _store_path()
    with _STORE_LOCK:
        records = _read_store(path)
        record = records.get(code)
        return dict(record) if record is not None else None


def forget_install_token(code: str) -> None:
    path = _store_path()
    with _STORE_LOCK:
        records = _read_store(path)
        if code not in records:
            return
        del records[code]
        _write_store(path, records)


async def revoke_install_token(code: str) -> bool:
    record = install_token(code)
    if record is None:
        return True
    issuer = HatchetTokenIssuer.from_environment(record['api_url'], record['tenant_id'])
    await issuer.revoke_worker_token(record['token_id'])
    forget_install_token(code)
    return True


async def revoke_worker_tokens(worker_id: str) -> bool:
    for code, _record in install_tokens_for_worker(worker_id):
        await revoke_install_token(code)
    return True
