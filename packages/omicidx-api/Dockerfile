FROM ghcr.io/astral-sh/uv:0.7-python3.12-bookworm-slim AS builder

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Install workspace dependencies (cache-friendly layer ordering)
COPY pyproject.toml uv.lock ./
COPY packages/omicidx-api/pyproject.toml packages/omicidx-api/pyproject.toml

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --package omicidx-api --no-install-project --frozen

# Copy source and install the project (non-editable so it's in site-packages)
COPY packages/omicidx-api/src packages/omicidx-api/src

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --package omicidx-api --frozen --no-editable


FROM python:3.12-slim-bookworm

WORKDIR /app

COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/v1/health')"

CMD ["uvicorn", "omicidx.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
