# Images for the root docker-compose.yml, one per target. They hold dependencies only: the repository
# is mounted at /repo, because flows/ and .flow-state/ live there. Build context: agents/.
FROM python:3.12-slim AS base
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUTF8=1
WORKDIR /opt/requirements
COPY requirements-flow*.txt ./
RUN pip install --no-cache-dir -r requirements-flow.txt
WORKDIR /repo

# Flow API, activation and the PC relay.
FROM base AS flow-host

FROM base AS flow-worker-hatchet
RUN pip install --no-cache-dir -r /opt/requirements/requirements-flow-hatchet.txt

FROM base AS flow-worker-prefect
RUN pip install --no-cache-dir -r /opt/requirements/requirements-flow-prefect.txt
