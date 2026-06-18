# syntax=docker/dockerfile:1.7
# Build from the workspace root (context = whisper/):
#   slim VM (SLURM dispatch only):
#     docker build -f apps/scheduler/Dockerfile -t btt-scheduler .
#   fat VM (runs scrape/transcribe jobs in-process via app.worker.local):
#     docker build -f apps/scheduler/Dockerfile --build-arg INSTALL_LOCAL=true -t btt-scheduler:local .
ARG INSTALL_LOCAL=false

FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build
ARG INSTALL_LOCAL
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

COPY pyproject.toml uv.lock ./
COPY packages/core/pyproject.toml packages/core/
COPY apps/scheduler/pyproject.toml apps/scheduler/
COPY workers/scrape/pyproject.toml workers/scrape/
COPY workers/transcribe/pyproject.toml workers/transcribe/
RUN --mount=type=cache,target=/root/.cache/uv \
    EXTRA=$([ "$INSTALL_LOCAL" = "true" ] && echo "--extra local"); \
    uv sync --frozen --no-dev --no-install-workspace $EXTRA --package btt-scheduler

COPY packages/core packages/core
COPY apps/scheduler apps/scheduler
COPY workers workers
RUN --mount=type=cache,target=/root/.cache/uv \
    EXTRA=$([ "$INSTALL_LOCAL" = "true" ] && echo "--extra local"); \
    uv sync --frozen --no-dev --no-editable $EXTRA --package btt-scheduler

# --- runtime ---
FROM python:3.12-slim-bookworm
ARG INSTALL_LOCAL
WORKDIR /app
# ffmpeg only needed by the fat VM (in-process transcription).
RUN if [ "$INSTALL_LOCAL" = "true" ]; then \
        apt-get update \
        && apt-get install -y --no-install-recommends ffmpeg \
        && rm -rf /var/lib/apt/lists/*; \
    fi
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
EXPOSE 8000
CMD ["python", "-m", "app.main"]
