"""Browser-visible URLs are separate from the board container's engine API addresses."""
import os
from urllib.parse import quote


def backend_url(system, ref, *, node=False, tenant=None):
    if system == 'hatchet':
        if not tenant:
            return None
        base = os.environ.get('HATCHET_DASHBOARD_URL', 'http://localhost:8080').rstrip('/')
        return f"{base}/tenants/{quote(tenant, safe='')}/{'task-runs' if node else 'runs'}/{quote(ref, safe='')}"
    if system in ('trigger', 'trigger-dev'):
        base = os.environ.get('TRIGGER_DASHBOARD_URL', 'http://localhost:8030').rstrip('/')
        return f"{base}/runs/{quote(ref, safe='')}"
    if system == 'prefect':
        base = os.environ.get('PREFECT_DASHBOARD_URL', 'http://localhost:4200').rstrip('/')
        return f"{base}/runs/{'task-run' if node else 'flow-run'}/{quote(ref, safe='')}"
    return None


def tenant_id(adapter):
    return getattr(getattr(getattr(adapter, 'hatchet', None), 'config', None), 'tenant_id', None)


def trigger_queue_url(system, ref):
    key = 'TRIGGER_DEV_DASHBOARD_PATH' if system == 'trigger-dev' else 'TRIGGER_DASHBOARD_PATH'
    path = os.environ.get(key, '').rstrip('/')
    if not path.startswith('/orgs/') or not ref: return None
    base = os.environ.get('TRIGGER_DASHBOARD_URL', 'http://localhost:8030').rstrip('/')
    return f'{base}{path}/concurrency/{quote(ref, safe="")}'
