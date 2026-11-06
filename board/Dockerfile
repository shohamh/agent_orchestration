FROM python:3.12-slim

WORKDIR /app

# Engine SDKs to include: each has a requirements-<engine>.txt. Only Hatchet's tab needs one; the
# board talks to Trigger.dev and Prefect over HTTP.
ARG ENGINES=""

# Offline: if ./wheelhouse has wheels (created by offline/bundle.sh), install from it with no index.
COPY requirements*.txt ./
COPY wheelhouse ./wheelhouse
RUN reqs="-r requirements.txt"; \
    for engine in $(echo "$ENGINES" | tr ',' ' '); do reqs="$reqs -r requirements-$engine.txt"; done; \
    if ls wheelhouse/*.whl >/dev/null 2>&1; then \
      pip install --no-cache-dir --no-index --find-links=wheelhouse $reqs; \
    else \
      pip install --no-cache-dir $reqs; \
    fi && rm -rf wheelhouse

COPY *.py ./
COPY static ./static

EXPOSE 8090
CMD ["python", "-u", "server.py"]
