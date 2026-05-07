FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

# Copy the full workspace so uv can resolve workspace refs
COPY pyproject.toml /opt/dagster/workspace/pyproject.toml
COPY packages/omicidx-parsers /opt/dagster/workspace/packages/omicidx-parsers
COPY packages/omicidx-dagster /opt/dagster/workspace/packages/omicidx-dagster

WORKDIR /opt/dagster/workspace/packages/omicidx-dagster

RUN pip install --no-cache-dir \
    dagster==1.13.3 \
    dagster-postgres==0.29.3 \
    && pip install --no-cache-dir -e /opt/dagster/workspace/packages/omicidx-parsers \
    && pip install --no-cache-dir -e .

EXPOSE 4000

HEALTHCHECK --interval=30s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "import socket; socket.create_connection(('localhost', 4000), timeout=5)"
