ARG PREFECT_VERSION=3.9.1
FROM prefecthq/prefect:${PREFECT_VERSION}-python3.12

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./

CMD ["python", "-u", "serve.py"]
