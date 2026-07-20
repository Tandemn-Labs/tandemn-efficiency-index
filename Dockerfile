FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS build

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src ./src

RUN uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.source="https://github.com/Tandemn-Labs/tandemn-efficiency-index"
LABEL org.opencontainers.image.title="Tandemn Efficiency Index"
LABEL org.opencontainers.image.description="Kubernetes GPU inference workload observability"

WORKDIR /app

COPY --from=build --chown=65532:65532 /app/.venv /app/.venv

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

USER 65532:65532

EXPOSE 8000

CMD ["/app/.venv/bin/tei-server"]
