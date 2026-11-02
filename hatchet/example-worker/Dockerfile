FROM python:3.12-slim

WORKDIR /app

# Offline: if ./wheelhouse exists (created by offline/bundle.sh), install from it with no index.
COPY requirements.txt ./
COPY wheelhouse ./wheelhouse
RUN if ls wheelhouse/*.whl >/dev/null 2>&1; then \
      pip install --no-cache-dir --no-index --find-links=wheelhouse -r requirements.txt; \
    else \
      pip install --no-cache-dir -r requirements.txt; \
    fi && rm -rf wheelhouse

COPY *.py ./

CMD ["python", "-u", "worker.py"]
