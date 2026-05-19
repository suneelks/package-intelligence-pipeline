FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1

# ─── uv (Python package manager) ────────────────────────────────────
# pip-install avoids ghcr.io rate limits / auth denials we hit with
# `COPY --from=ghcr.io/astral-sh/uv:...`. The base image ships pipx.
RUN pipx install uv || pip install --no-cache-dir --break-system-packages uv

# ─── System tools commonly useful while developing the pipeline ─────
# The MS devcontainer base ships an apt source for yarn whose GPG key
# has rotated; we don't use yarn, so drop it before `apt-get update`
# to avoid GPG failures turning an otherwise clean update into an error.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
 && apt-get update && apt-get install -y --no-install-recommends \
        postgresql-client \
        jq \
        less \
        make \
    && rm -rf /var/lib/apt/lists/*

# The devcontainers/python base image already creates a `vscode` user.
USER vscode

# Shell niceties: uv + dagster completion, project venv on PATH.
RUN echo 'export PATH="/workspaces/package-intelligence-pipeline/.venv/bin:$PATH"' >> ~/.bashrc \
 && echo 'eval "$(uv generate-shell-completion bash)" 2>/dev/null || true' >> ~/.bashrc \
 && echo 'export DAGSTER_HOME=/workspaces/package-intelligence-pipeline/.dagster_home' >> ~/.bashrc
