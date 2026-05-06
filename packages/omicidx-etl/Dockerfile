
# Install system dependencies (git, build-essential for some wheels)

# Stage 1: Base image and uv installation
FROM python:3.13-slim-bookworm AS base

RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install uv from its official pre-built binaries
FROM base AS builder
COPY --from=ghcr.io/astral-sh/uv:0.4.9 /uv /bin/uv

# Set environment variables for uv optimization
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Set working directory
WORKDIR /app

# Copy lock file and project configuration
COPY uv.lock /app/
COPY pyproject.toml /app/

# Install dependencies in the builder stage (for caching)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev

# Stage 2: Development environment
FROM base AS dev

# Copy uv binary from builder stage
COPY --from=builder /bin/uv /bin/uv

# Set environment variables for uv optimization
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

# Set working directory
WORKDIR /app

# Copy application code (will be bind-mounted in local dev)
COPY ./omicidx_etl /app/omicidx_etl
COPY uv.lock /app/
COPY pyproject.toml /app/


# Install dependencies (will use cached layers and potentially bind-mounted venv)
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# Set PATH to include the virtual environment
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["oidx"]