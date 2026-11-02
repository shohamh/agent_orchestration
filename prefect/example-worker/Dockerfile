ARG PREFECT_VERSION=3.9.1
FROM prefecthq/prefect:${PREFECT_VERSION}-python3.12
ARG PREFECT_VERSION

WORKDIR /app
COPY requirements.txt ./
# Offline: if ./wheelhouse has wheels (copied in by offline/load.sh), install from it with no index.
COPY wheelhouse ./wheelhouse
RUN if ls wheelhouse/*.whl >/dev/null 2>&1; then \
      pip install --no-cache-dir --no-index --find-links=wheelhouse "prefect[otel]==${PREFECT_VERSION}" -r requirements.txt; \
    else \
      pip install --no-cache-dir "prefect[otel]==${PREFECT_VERSION}" -r requirements.txt; \
    fi && rm -rf wheelhouse
COPY *.py ./

CMD ["python", "-u", "serve.py"]
