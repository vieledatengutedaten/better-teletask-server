# syntax=docker/dockerfile:1.7
# Build from the workspace root (context = whisper/):
#   docker build -f workers/scrape/Dockerfile -t btt-worker-scrape .
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

# 1) Dependency layer: only manifests + lock, so it stays cached until a
#    pyproject/lock changes. All member manifests are needed for the workspace
#    to resolve against the single lockfile.
COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/
COPY apps/scheduler/pyproject.toml apps/scheduler/
COPY workers/scrape/pyproject.toml workers/scrape/
COPY workers/transcribe/pyproject.toml workers/transcribe/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package btt-worker-scrape

# 2) Source layer: only this member + its workspace dep (core).
COPY packages/core packages/core
COPY workers/scrape workers/scrape
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package btt-worker-scrape

# --- runtime ---
FROM python:3.12-slim-bookworm
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
ENTRYPOINT ["python", "-m", "scrape_worker"]
