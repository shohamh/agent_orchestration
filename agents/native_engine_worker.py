"""Native Hatchet and Prefect workers that start only jobs admitted by this PC's service gate.

The Fleet coordinator keeps owning placement, payload transfer, cancellation and fencing. Engine
workers are a scheduler bridge: a one-time engine task asks the local service to start the exact
already-admitted job, and the coordinator continues to monitor that service directly.
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

HATCHET_WORKFLOW = 'agent-worker-native-dispatch'
PREFECT_FLOW = 'agent-worker-native-dispatch'
READY_TIMEOUT = 120


def runtime_paths() -> Path:
    value = os.environ.get('AGENT_WORKER_NATIVE_RUNTIME')
    if not value:
        raise RuntimeError('The bundled native engine runtime path is missing')
    root = Path(value)
    if not (root / 'python').is_dir() or not (root / 'packages').is_dir() or not (root / 'agents').is_dir():
        raise RuntimeError('The bundled native engine runtime is incomplete')
    for path in (root / 'packages', root / 'agents'):
        if str(path) not in sys.path:
            sys.path.insert(0, str(path))
    return root


def worker_uuid() -> str:
    value = os.environ.get('AGENT_WORKER_UUID', '')
    if not value:
        raise RuntimeError('The worker identity is missing')
    return value


def prefect_names(identity: str) -> tuple[str, str, str]:
    """Stable, private-per-PC names make pool, queue and deployment reuse idempotent."""
    suffix = identity.replace('-', '')
    pool = f'agent-{suffix}'
    queue = f'pc-{suffix}'
    deployment = f'pc-{suffix}'
    return pool, queue, deployment


def read_fleet_metadata() -> dict[str, Any]:
    """Read the coordinator's revisioned pool/tag projection for this PC."""
    path = Path(os.environ['AGENT_WORKER_HOME']) / 'runtime' / 'fleet-metadata.json'
    try:
        metadata = json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError:
        return {'revision': 0, 'pools': [], 'tags': []}
    if (not isinstance(metadata, dict) or set(metadata) != {'revision', 'pools', 'tags'}
            or type(metadata['revision']) is not int or metadata['revision'] < 0):
        raise ValueError('The Fleet metadata projection is invalid')
    for field in ('pools', 'tags'):
        values = metadata[field]
        if (not isinstance(values, list) or len(values) > 50
                or any(not isinstance(value, str) or not value or len(value) > 100 or value != value.strip()
                       or ',' in value or any(ord(c) < 32 or ord(c) == 127 for c in value) for value in values)
                or len(set(values)) != len(values)):
            raise ValueError('The Fleet metadata projection is invalid')
    return metadata


def hatchet_fleet_labels(metadata: dict[str, Any]) -> dict[str, str]:
    """Stable namespaced labels; the Board remains authoritative for placement."""
    return {'fleet_pools': ','.join(metadata['pools']), 'fleet_tags': ','.join(metadata['tags'])}


def prefect_fleet_tags(metadata: dict[str, Any]) -> list[str]:
    """Prefect deployment tags are for organization, not worker routing."""
    return sorted([*(f'fleet-pool:{pool}' for pool in metadata['pools']),
                   *(f'fleet-tag:{tag}' for tag in metadata['tags'])])


def apply_hatchet_fleet_labels(client: Any, worker_id: str, metadata: dict[str, Any]) -> None:
    from hatchet_sdk.types.labels import WorkerLabel
    labels = [WorkerLabel(key=key, value=value) for key, value in hatchet_fleet_labels(metadata).items()]
    client.dispatcher.upsert_worker_labels(worker_id, labels)


def prefect_deploy(flow: Any, pool: str, queue: str, deployment: str, metadata: dict[str, Any]) -> None:
    flow.deploy(name=deployment, work_pool_name=pool, work_queue_name=queue, build=False, push=False,
                tags=prefect_fleet_tags(metadata))


def metadata_changed(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    return previous != current


def record_metadata_status(engine: str, status_fields: dict[str, str], metadata: dict[str, Any],
                           state: str, error: str = '') -> None:
    status_fields.update(fleet_metadata_revision=str(metadata['revision']),
                         fleet_metadata_state=state, fleet_metadata_error=error[:300])
    write_status(engine, 'ready', **status_fields)

def status_path(engine: str) -> Path:
    home = Path(os.environ.get('AGENT_WORKER_HOME', ''))
    if not home.is_absolute():
        raise RuntimeError('The worker home is missing')
    path = home / 'runtime' / 'native-engines' / f'{engine}.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def write_status(engine: str, state: str, **fields: Any) -> None:
    safe = {'state': state, 'updated_at': time.time()}
    for key in ('worker_id', 'worker_name', 'work_pool', 'work_queue', 'deployment',
                'fleet_metadata_revision', 'fleet_metadata_state', 'fleet_metadata_error', 'error'):
        value = fields.get(key)
        if isinstance(value, (str, int, float)) and len(str(value)) <= 500:
            safe[key] = value
    path = status_path(engine)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(safe, separators=(',', ':')), encoding='utf-8')
    os.replace(temporary, path)


class _RedactedStream:
    def __init__(self, stream: Any, secret: str):
        self._stream, self._secret = stream, secret

    def write(self, text: str) -> int:
        return self._stream.write(text.replace(self._secret, '[redacted]') if self._secret else text)

    def flush(self) -> None:
        self._stream.flush()

    def __getattr__(self, key: str) -> Any:
        return getattr(self._stream, key)


def _redact_secret(token: str) -> None:
    sys.stdout = _RedactedStream(sys.stdout, token)
    sys.stderr = _RedactedStream(sys.stderr, token)



def _sanitize(message: str, secret: str = '') -> str:
    return message.replace(secret, '[redacted]') if secret else message


class _NativeDispatchFailure(RuntimeError):
    def __init__(self, message: str, *, fatal: bool = False, status: int | None = None):
        super().__init__(message)
        self.fatal = fatal
        self.status = status


def _local_service_token() -> str:
    try:
        home = Path(os.environ['AGENT_WORKER_HOME'])
        token = (home / 'token').read_text(encoding='utf-8').strip()
    except (KeyError, OSError):
        raise RuntimeError('The local worker service token is missing') from None
    if not token:
        raise RuntimeError('The local worker service token is missing')
    return token


def _native_dispatch_url(engine: str) -> str:
    try:
        port = int(os.environ.get('AGENT_WORKER_PORT', '8096'))
    except ValueError:
        raise RuntimeError('The local worker service port is invalid') from None
    if not 1 <= port <= 65535:
        raise RuntimeError('The local worker service port is invalid')
    return f'http://127.0.0.1:{port}/native-dispatch?{urllib.parse.urlencode({"engine": engine})}'


def _native_service_request(url: str, token: str, *, body: dict[str, str] | None = None) -> bytes:
    data = json.dumps(body).encode('utf-8') if body is not None else None
    headers = {'Authorization': f'Bearer {token}', 'Accept': 'application/json'}
    if data is not None:
        headers['Content-Type'] = 'application/json'
    request = urllib.request.Request(url, data=data, headers=headers, method='POST' if data is not None else 'GET')
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        retryable = error.code in (408, 429) or error.code >= 500
        raise _NativeDispatchFailure(
            f'The local worker service refused a native dispatch request (HTTP {error.code})',
            fatal=not retryable, status=error.code,
        ) from None
    except OSError:
        raise _NativeDispatchFailure('The local worker service is unreachable') from None


def _native_dispatch_jobs(url: str, token: str) -> list[tuple[str, str]]:
    try:
        response = json.loads(_native_service_request(url, token))
    except _NativeDispatchFailure:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise _NativeDispatchFailure('The local worker service returned an invalid dispatch response', fatal=True) from None
    if not isinstance(response, dict) or not isinstance(response.get('jobs'), list):
        raise _NativeDispatchFailure('The local worker service returned an invalid dispatch response', fatal=True)
    jobs: list[tuple[str, str]] = []
    for item in response['jobs']:
        if not isinstance(item, dict):
            raise _NativeDispatchFailure('The local worker service returned an invalid dispatch response', fatal=True)
        job_id, nonce = item.get('id'), item.get('nonce')
        if not isinstance(job_id, str) or not job_id or len(job_id) > 128:
            raise _NativeDispatchFailure('The local worker service returned an invalid dispatch job', fatal=True)
        if not isinstance(nonce, str) or not nonce or len(nonce) > 256:
            raise _NativeDispatchFailure('The local worker service returned an invalid dispatch job', fatal=True)
        jobs.append((job_id, nonce))
    return jobs


def _native_run_id(run: Any) -> str:
    if isinstance(run, str):
        value = run
    elif isinstance(run, dict):
        value = run.get('workflow_run_id') or run.get('id') or run.get('run_id')
    else:
        value = (getattr(run, 'workflow_run_id', None) or getattr(run, 'id', None)
                 or getattr(run, 'run_id', None))
    if not isinstance(value, (str, uuid.UUID)) or not str(value):
        raise RuntimeError('The engine did not return a run identifier')
    return str(value)


class _NativeDispatchPoller:
    def __init__(self, engine: str, token: str, submit: Any, status_fields: dict[str, str]):
        self.engine = engine
        self.token = token
        self.submit = submit
        self.status_fields = status_fields
        self.url = _native_dispatch_url(engine)
        self._stop = threading.Event()
        self._initial_cycle = threading.Event()
        self._lock = threading.Lock()
        self._fatal_message: str | None = None
        self._last_error: str | None = None
        self._submitted: dict[tuple[str, str], str] = {}
        self._is_ready = False
        self._thread = threading.Thread(target=self._run, name=f'{engine}-native-dispatch', daemon=True)

    def start(self) -> None:
        self._thread.start()

    def wait_until_ready(self, timeout: float) -> None:
        if not self._initial_cycle.wait(timeout):
            raise RuntimeError('The local worker service did not become ready for native dispatch')
        message = self.fatal_error()
        if message:
            raise RuntimeError(message)

    def fatal_error(self) -> str | None:
        with self._lock:
            return self._fatal_message

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive() and threading.current_thread() is not self._thread:
            self._thread.join(timeout=9)

    def _set_failed(self, message: str) -> None:
        if self._is_ready or self._last_error != message:
            self._last_error = message
            self._is_ready = False
            write_status(self.engine, 'failed', **self.status_fields, error=message)

    def _set_ready(self) -> None:
        if not self._is_ready:
            self._last_error = None
            self._is_ready = True
            write_status(self.engine, 'ready', **self.status_fields)
        self._initial_cycle.set()

    def _fail_fatal(self, message: str) -> None:
        with self._lock:
            self._fatal_message = message
        self._set_failed(message)
        self._initial_cycle.set()

    def _acknowledge(self, job_id: str, nonce: str, run_id: str) -> bool:
        path = f'/jobs/{urllib.parse.quote(job_id, safe="")}/dispatch/ack'
        port = urllib.parse.urlsplit(self.url).port
        url = f'http://127.0.0.1:{port}{path}'
        try:
            _native_service_request(url, self.token, body={'nonce': nonce, 'run_id': run_id})
        except _NativeDispatchFailure as error:
            if error.status in (404, 409):
                return False
            raise
        return True

    def _dispatch_job(self, job_id: str, nonce: str) -> None:
        key = (job_id, nonce)
        run_id = self._submitted.get(key)
        if run_id is None:
            run_id = _native_run_id(self.submit(job_id, nonce))
            self._submitted[key] = run_id
        if self._acknowledge(job_id, nonce, run_id):
            self._submitted.pop(key, None)
        else:
            self._submitted.pop(key, None)

    def _run(self) -> None:
        try:
            while not self._stop.is_set():
                try:
                    jobs = _native_dispatch_jobs(self.url, self.token)
                except _NativeDispatchFailure as error:
                    if error.fatal:
                        self._fail_fatal(str(error))
                        return
                    self._set_failed(str(error))
                    self._stop.wait(1)
                    continue
                seen: set[tuple[str, str]] = set()
                cycle_error: _NativeDispatchFailure | None = None
                for job_id, nonce in jobs:
                    key = (job_id, nonce)
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        self._dispatch_job(job_id, nonce)
                    except _NativeDispatchFailure as error:
                        if error.fatal:
                            self._fail_fatal(str(error))
                            return
                        cycle_error = error
                    except Exception:
                        cycle_error = _NativeDispatchFailure('The engine could not submit a native dispatch')
                for key in tuple(self._submitted):
                    if key not in seen:
                        self._submitted.pop(key, None)
                if cycle_error is None:
                    self._set_ready()
                else:
                    self._set_failed(str(cycle_error))
                self._stop.wait(1)
        except BaseException:
            self._fail_fatal('The native dispatch poller stopped unexpectedly')
        finally:
            self._initial_cycle.set()


def _native_dispatch_poller(engine: str, token: str, submit: Any, status_fields: dict[str, str]) -> _NativeDispatchPoller:
    return _NativeDispatchPoller(engine, token, submit, status_fields)

def _local_job_request(job_id: str, nonce: str) -> dict[str, Any]:
    if not isinstance(job_id, str) or not job_id or len(job_id) > 128:
        raise ValueError('Invalid admitted job id')
    if not isinstance(nonce, str) or not nonce or len(nonce) > 256:
        raise ValueError('Invalid admission nonce')
    home = Path(os.environ['AGENT_WORKER_HOME'])
    token = (home / 'token').read_text(encoding='utf-8').strip()
    if not token:
        raise RuntimeError('The local worker service token is missing')
    port = int(os.environ.get('AGENT_WORKER_PORT', '8096'))
    request = urllib.request.Request(
        f'http://127.0.0.1:{port}/jobs/{job_id}/start',
        data=json.dumps({'nonce': nonce}).encode('utf-8'),
        headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(request, timeout=8) as response:
            return json.loads(response.read())
    except urllib.error.HTTPError as error:
        if error.code in (404, 409):
            # Replayed engine runs are harmless only when the service confirms this exact job is
            # already active or terminal. A deleted/fenced job must never be restarted.
            status_request = urllib.request.Request(
                f'http://127.0.0.1:{port}/jobs/{job_id}',
                headers={'Authorization': f'Bearer {token}'},
            )
            try:
                with urllib.request.urlopen(status_request, timeout=8) as response:
                    state = json.loads(response.read()).get('state')
            except (urllib.error.HTTPError, OSError, ValueError):
                raise RuntimeError('The local service no longer accepts this admitted job') from None
            if state in ('running', 'done'):
                return {'started': state == 'running', 'state': state}
            raise RuntimeError('The local service rejected this admitted job') from None
        raise RuntimeError(f'The local service refused the admitted job (HTTP {error.code})') from None
    except OSError:
        raise RuntimeError('The local worker service is unreachable') from None


def build_hatchet_workflow(client: Any) -> Any:
    """The same task definition is registered on the PC and used by the coordinator to target it."""
    from datetime import timedelta

    workflow = client.workflow(name=HATCHET_WORKFLOW)

    @workflow.task(name='start-admitted-job', schedule_timeout=timedelta(days=30), execution_timeout=timedelta(minutes=1))
    def start_admitted_job(payload: dict[str, str], _ctx: Any) -> dict[str, Any]:
        return _local_job_request(payload['job_id'], payload['nonce'])

    return workflow


def build_prefect_flow() -> Any:
    from prefect import flow

    @flow(name=PREFECT_FLOW)
    def start_admitted_job(job_id: str, nonce: str) -> dict[str, Any]:
        return _local_job_request(job_id, nonce)

    return start_admitted_job


def _hatchet_client_from_environment() -> Any:
    from hatchet_sdk import Hatchet
    return Hatchet()


def _hatchet_main() -> None:
    runtime_paths()
    service_token = _local_service_token()
    _redact_secret(service_token)
    token_file = os.environ.get('HATCHET_TOKEN_FILE')
    token = Path(token_file).read_text(encoding='utf-8').strip() if token_file else ''
    if not token:
        raise RuntimeError('The dedicated Hatchet worker token is missing')
    _redact_secret(token)
    os.environ['HATCHET_CLIENT_TOKEN'] = token
    for key in ('HATCHET_CLIENT_HOST_PORT', 'HATCHET_CLIENT_SERVER_URL', 'HATCHET_CLIENT_TLS_STRATEGY'):
        if not os.environ.get(key):
            raise RuntimeError(f'The Hatchet worker setting {key} is missing')
    if os.name == 'nt':
        from hatchet_windows import install
        install()
    identity = worker_uuid()
    metadata = read_fleet_metadata()
    client = _hatchet_client_from_environment()
    workflow = build_hatchet_workflow(client)
    worker_name = f'aw-{uuid.UUID(identity).hex}'
    worker = client.worker(worker_name, slots=int(os.environ.get('AGENT_WORKER_SLOTS', '1')),
                           labels={'fleet_worker_id': identity, **hatchet_fleet_labels(metadata)}, workflows=[workflow])
    write_status('hatchet', 'starting', worker_name=worker_name)
    failure: list[BaseException] = []
    dispatcher: _NativeDispatchPoller | None = None
    status_fields = {'worker_name': worker_name}

    def run() -> None:
        try:
            worker.start()
        except BaseException as error:
            failure.append(error)

    thread = threading.Thread(target=run, name='hatchet-worker', daemon=True)
    thread.start()
    deadline = time.monotonic() + READY_TIMEOUT
    try:
        worker_id: str | None = None
        while time.monotonic() < deadline:
            if failure:
                raise RuntimeError('The Hatchet SDK worker failed to connect') from None
            if not thread.is_alive():
                raise RuntimeError('The Hatchet SDK worker exited before becoming ready')
            try:
                rows = client.workers.list().rows or []
                current = next((item for item in rows if getattr(item, 'name', None) == worker_name), None)
                current_id = getattr(getattr(current, 'metadata', None), 'id', None) if current else None
                if current_id:
                    worker_id = str(current_id)
                    break
            except Exception:
                # A transient list failure is not readiness; the connected worker keeps retrying.
                pass
            time.sleep(2)
        if not worker_id:
            raise RuntimeError('The Hatchet SDK worker did not register before the readiness timeout')

        status_fields['worker_id'] = worker_id
        record_metadata_status('hatchet', status_fields, metadata, 'applied')

        def submit(job_id: str, nonce: str) -> str:
            run_ref = workflow.run(input={'job_id': job_id, 'nonce': nonce},
                                   wait_for_result=False, desired_worker_id=worker_id)
            return _native_run_id(run_ref)

        dispatcher = _native_dispatch_poller('hatchet', service_token, submit, status_fields)
        dispatcher.start()
        dispatcher.wait_until_ready(max(0.0, deadline - time.monotonic()))
        while thread.is_alive():
            if failure:
                raise RuntimeError('The Hatchet SDK worker disconnected') from None
            if dispatcher.fatal_error():
                raise RuntimeError(dispatcher.fatal_error())
            try:
                current = read_fleet_metadata()
                if metadata_changed(metadata, current):
                    apply_hatchet_fleet_labels(client, worker_id, current)
                    metadata = current
                    record_metadata_status('hatchet', status_fields, metadata, 'applied')
                elif status_fields.get('fleet_metadata_state') == 'failed':
                    apply_hatchet_fleet_labels(client, worker_id, metadata)
                    record_metadata_status('hatchet', status_fields, metadata, 'applied')
            except Exception:
                record_metadata_status('hatchet', status_fields, metadata, 'failed',
                                       'Native engine metadata update failed')
            time.sleep(2)
        raise RuntimeError('The Hatchet SDK worker stopped')
    except BaseException as error:
        if dispatcher:
            dispatcher.stop()
        write_status('hatchet', 'failed', **status_fields, error=_sanitize(str(error), token)[:300])
        raise


def _run_command(args: list[str], env: dict[str, str], timeout: int = 120) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=timeout, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        message = detail[-1][:300] if detail else f'command exited with status {result.returncode}'
        raise RuntimeError(message)
    return result


def _prefect_client():
    from prefect.client.orchestration import get_client
    return get_client


def _prefect_worker_status(pool: str, worker_name: str) -> tuple[str | None, bool]:
    import asyncio
    get_client = _prefect_client()

    async def find() -> tuple[str | None, bool]:
        async with get_client() as client:
            rows = await client.read_workers_for_work_pool(pool)
        for item in rows:
            if getattr(item, 'name', None) == worker_name:
                status = getattr(item, 'status', None)
                status = getattr(status, 'value', status)
                return str(getattr(item, 'id', '') or '') or None, str(status).lower() == 'online'
        return None, False

    return asyncio.run(find())




def _prefect_main() -> None:
    runtime_paths()
    service_token = _local_service_token()
    _redact_secret(service_token)
    api_url = os.environ.get('PREFECT_API_URL', '')
    if not api_url:
        raise RuntimeError('The Prefect worker API URL is missing')
    identity = worker_uuid()
    pool, queue, deployment = prefect_names(identity)
    worker_name = f'agent-worker-{identity}'
    env = os.environ.copy()
    _run_command([sys.executable, '-m', 'prefect', 'work-pool', 'create', pool, '--type', 'process', '--overwrite'], env)
    queue_result = subprocess.run([sys.executable, '-m', 'prefect', 'work-queue', 'create', queue, '--pool', pool],
                                  env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, check=False)
    if queue_result.returncode != 0:
        # The generated UUID names are private to this PC; an existing queue is the idempotent
        # restart case, while a missing queue will be caught by deployment/worker startup.
        if 'already exists' not in (queue_result.stderr + queue_result.stdout).lower():
            raise RuntimeError('Could not create the Prefect per-PC work queue')
    flow = build_prefect_flow()
    metadata = read_fleet_metadata()
    prefect_deploy(flow, pool, queue, deployment, metadata)
    write_status('prefect', 'starting', worker_name=worker_name, work_pool=pool, work_queue=queue,
                 deployment=deployment, fleet_metadata_revision=str(metadata['revision']),
                 fleet_metadata_state='applied')
    process = subprocess.Popen([sys.executable, '-m', 'prefect', 'worker', 'start', '--pool', pool,
                                '--work-queue', queue, '--name', worker_name, '--limit',
                                os.environ.get('AGENT_WORKER_SLOTS', '1')], env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')

    def forward() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            text = line.rstrip()
            if text:
                print(text, flush=True)

    reader = threading.Thread(target=forward, name='prefect-worker-log', daemon=True)
    reader.start()
    deadline = time.monotonic() + READY_TIMEOUT
    dispatcher: _NativeDispatchPoller | None = None
    status_fields = {'worker_name': worker_name, 'work_pool': pool, 'work_queue': queue, 'deployment': deployment,
                     'fleet_metadata_revision': str(metadata['revision']), 'fleet_metadata_state': 'applied'}
    try:
        worker_id: str | None = None
        while time.monotonic() < deadline:
            code = process.poll()
            if code is not None:
                raise RuntimeError(f'The Prefect process worker exited (status {code})')
            try:
                worker_id, online = _prefect_worker_status(pool, worker_name)
                if worker_id and online:
                    break
            except Exception:
                pass
            time.sleep(2)
        if not worker_id:
            raise RuntimeError('The Prefect process worker did not register before the readiness timeout')
        status_fields['worker_id'] = worker_id

        def submit(job_id: str, nonce: str) -> str:
            from prefect.deployments import run_deployment
            idempotency_key = hashlib.sha256(job_id.encode('utf-8') + b'\0' + nonce.encode('utf-8')).hexdigest()
            run = run_deployment(
                f'{PREFECT_FLOW}/{deployment}',
                parameters={'job_id': job_id, 'nonce': nonce},
                work_queue_name=queue,
                timeout=0,
                idempotency_key=idempotency_key,
            )
            return _native_run_id(run)

        dispatcher = _native_dispatch_poller('prefect', service_token, submit, status_fields)
        dispatcher.start()
        dispatcher.wait_until_ready(max(0.0, deadline - time.monotonic()))
        while process.poll() is None:
            if dispatcher.fatal_error():
                raise RuntimeError(dispatcher.fatal_error())
            try:
                current = read_fleet_metadata()
                if metadata_changed(metadata, current):
                    prefect_deploy(flow, pool, queue, deployment, current)
                    metadata = current
                    record_metadata_status('prefect', status_fields, metadata, 'applied')
                elif status_fields.get('fleet_metadata_state') == 'failed':
                    prefect_deploy(flow, pool, queue, deployment, metadata)
                    record_metadata_status('prefect', status_fields, metadata, 'applied')
            except Exception:
                record_metadata_status('prefect', status_fields, metadata, 'failed',
                                       'Native engine metadata update failed')
            time.sleep(2)
        raise RuntimeError('The Prefect process worker stopped')
    except BaseException as error:
        if dispatcher:
            dispatcher.stop()
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
        message = _sanitize(str(error), service_token)
        write_status('prefect', 'failed', **status_fields, error=message[:300])
        raise RuntimeError(message) from None

def main() -> None:
    engine = os.environ.get('AGENT_WORKER_NATIVE_ENGINE')
    if engine == 'hatchet':
        _hatchet_main()
    elif engine == 'prefect':
        _prefect_main()
    else:
        raise RuntimeError("AGENT_WORKER_NATIVE_ENGINE must be 'hatchet' or 'prefect'")
