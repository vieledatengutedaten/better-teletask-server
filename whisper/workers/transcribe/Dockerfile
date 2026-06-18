# syntax=docker/dockerfile:1.7
# Build from the workspace root (context = whisper/):
#   docker build -f workers/transcribe/Dockerfile -t btt-worker-transcribe .
# GPU at runtime needs the NVIDIA driver on the host + nvidia-container-toolkit
# (docker run --gpus all). The CUDA userspace libs ship inside the torch wheels
# (nvidia-*-cu12), so no CUDA base image is required.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=0

COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/
COPY apps/scheduler/pyproject.toml apps/scheduler/
COPY workers/scrape/pyproject.toml workers/scrape/
COPY workers/transcribe/pyproject.toml workers/transcribe/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-workspace --package btt-worker-transcribe

COPY packages/core packages/core
COPY workers/transcribe workers/transcribe
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --package btt-worker-transcribe

# --- runtime ---
FROM python:3.12-slim-bookworm
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" \
    HF_HOME=/models/huggingface \
    TORCH_HOME=/models/torch
ENTRYPOINT ["python", "-m", "transcribe_worker"]
