# Build stage
FROM ghcr.io/astral-sh/uv:bookworm-slim AS builder

RUN apt-get update && apt-get install -y g++ git

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_PYTHON_INSTALL_DIR=/python
ENV UV_PYTHON_PREFERENCE=only-managed

RUN uv python install 3.13

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

COPY app/ app/

# Final stage
FROM debian:bookworm-slim

RUN groupadd -r app && useradd -r -g app app

COPY --from=builder /python /python
COPY --from=builder /app /app

RUN mkdir /data && chown app:app /data

ENV PATH="/app/.venv/bin:$PATH"
ENV DATABASE_URL="sqlite:////data/database.db"

WORKDIR /app
USER app

EXPOSE 8000
VOLUME ["/data"]

CMD uvicorn app.main:app --host 0.0.0.0 --port 8000
