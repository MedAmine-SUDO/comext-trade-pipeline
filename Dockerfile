# =============================================================================
# COMEXT Trade Flow Pipeline — Dockerfile
#
# Follows official uv Docker integration guide:
# https://docs.astral.sh/uv/guides/integration/docker/
#
# Targets:
#   dev   → live-reloading, test tooling, source mounted via compose override
#   prod  → minimal image, source baked in, no dev dependencies
#
# Build examples:
#   Dev:  docker compose up                    (auto-merges override file)
#   Prod: docker compose -f docker-compose.yml up
# =============================================================================

ARG PYTHON_VERSION=3.11.9
ARG UV_VERSION=0.4.30

# =============================================================================
# STAGE: base
# =============================================================================
FROM python:${PYTHON_VERSION}-slim-bookworm AS base

# Install uv by copying from the official distroless image — the recommended
# approach per docs. No curl, no install script, no PATH guesswork.
COPY --from=ghcr.io/astral-sh/uv:0.4.30 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    # Silence warnings about hardlinks across filesystems
    UV_LINK_MODE=copy \
    # Pre-compile .pyc files for faster startup
    UV_COMPILE_BYTECODE=1 \
    # Use the system Python (already in the image) — don't let uv download one
    UV_PYTHON_DOWNLOADS=0

# p7zip-full provides the `7z` CLI required to decompress .7z COMEXT archives
RUN apt-get update && apt-get install -y --no-install-recommends \
        p7zip-full \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# =============================================================================
# STAGE: builder
# Installs dependencies only (not the project itself) into .venv.
# Separating this from the source copy means deps are cached until
# pyproject.toml / uv.lock change, even if source files change.
# =============================================================================
FROM base AS builder

# Install prod deps — bind-mount manifests so they don't become a layer
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

# Install dev deps into a separate venv so prod image can skip them
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    UV_PROJECT_ENVIRONMENT=/venv-dev uv sync --frozen --no-install-project --extra dev

# =============================================================================
# STAGE: dev
# Source is bind-mounted at /app via docker-compose.override.yml.
# .venv (prod venv) stays inside /app — protected by an anonymous volume
# declared in the override (docker run --volume /app/.venv pattern).
# =============================================================================
FROM base AS dev

# Copy prod venv into .venv (dev runs tests against prod deps + dev extras)
COPY --from=builder /app/.venv /app/.venv
# Copy dev venv (has pytest, ruff, mypy on top)
COPY --from=builder /venv-dev /venv-dev

# Use the dev venv as default so pytest/ruff/mypy are available
ENV PATH="/venv-dev/bin:$PATH" \
    VIRTUAL_ENV="/venv-dev" \
    DAGSTER_HOME=/app/.dagster \
    COMEXT_DATA_DIR=/data \
    COMEXT_RAW_DIR=/data/raw \
    COMEXT_PROCESSED_DIR=/data/processed \
    COMEXT_DB_PATH=/data/comext.duckdb

RUN mkdir -p /app/.dagster /data/raw /data/processed

WORKDIR /app

EXPOSE 3000

CMD ["dagster", "dev", "-m", "comext_pipeline", "--host", "0.0.0.0", "--port", "3000"]

# =============================================================================
# STAGE: prod
# Source baked in, no dev deps, non-root user, minimal surface area.
# =============================================================================
FROM base AS prod

RUN groupadd --gid 1001 comext && \
    useradd --uid 1001 --gid comext --shell /bin/bash --create-home comext

# Copy only the prod venv
COPY --from=builder /app/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    VIRTUAL_ENV="/app/.venv" \
    DAGSTER_HOME=/app/.dagster \
    COMEXT_DATA_DIR=/data \
    COMEXT_RAW_DIR=/data/raw \
    COMEXT_PROCESSED_DIR=/data/processed \
    COMEXT_DB_PATH=/data/comext.duckdb

# Copy source and install project itself (deps already in venv)
COPY --chown=comext:comext . /app/
WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable

RUN mkdir -p /app/.dagster /data/raw /data/processed && \
    chown -R comext:comext /app /data

USER comext

EXPOSE 3000

CMD ["dagster-webserver", "-m", "comext_pipeline", "--host", "0.0.0.0", "--port", "3000"]