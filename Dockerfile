FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY src ./src

RUN uv sync --frozen --no-dev

USER 65532:65532

EXPOSE 8000

CMD ["/app/.venv/bin/tei"]
