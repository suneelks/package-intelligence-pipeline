FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl ca-certificates tini \
    && rm -rf /var/lib/apt/lists/*

# pip-install avoids ghcr.io rate limits / auth denials.
RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml ./
COPY src ./src

RUN uv sync --no-dev --frozen 2>/dev/null || uv sync --no-dev

RUN mkdir -p ${DAGSTER_HOME}
COPY deploy/dagster.yaml ${DAGSTER_HOME}/dagster.yaml
COPY workspace.yaml ${DAGSTER_HOME}/workspace.yaml

ENV PATH="/app/.venv/bin:${PATH}"

ENTRYPOINT ["/usr/bin/tini", "--"]
