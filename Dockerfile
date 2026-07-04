# syntax=docker/dockerfile:1

# --- build: resolve the locked environment with uv -------------------------
FROM python:3.13-slim AS build
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Dependencies first so the layer caches while only source changes.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev

# --- runtime ----------------------------------------------------------------
FROM python:3.13-slim
WORKDIR /app

RUN useradd --create-home --uid 1000 skybridge \
    && mkdir /data && chown skybridge:skybridge /data

COPY --from=build --chown=skybridge:skybridge /app /app
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    SKYBRIDGE_DB=/data/skybridge.db

USER skybridge
VOLUME /data
EXPOSE 8000

CMD ["python", "-m", "skybridge", "serve", "--host", "0.0.0.0", "--port", "8000"]
