#!/usr/bin/env bash
# Runs once after the devcontainer first boots.
# - Installs Python deps via uv.
# - Waits for Postgres, applies migrations.
# - Prints next-step commands.
set -euo pipefail

cd /workspaces/package-intelligence-pipeline

echo "─── uv sync (installing Python deps) ─────────────────────────"
uv sync

echo "─── waiting for Postgres ─────────────────────────────────────"
for _ in $(seq 1 30); do
  if PGPASSWORD=postgres psql -h db -U postgres -d nuget_pipeline -c 'SELECT 1' >/dev/null 2>&1; then
    echo "postgres ready"
    break
  fi
  sleep 1
done

echo "─── applying migrations ──────────────────────────────────────"
uv run python -m nuget_pipeline.db.migrate

mkdir -p .dagster_home

cat <<'EOF'

─── ready ────────────────────────────────────────────────────
Next steps (run from the devcontainer terminal):

  # Run the test suite (validates Dagster will work post-deployment):
  uv run pytest -v

  # Smoke-test the NuGet sync directly (NUGET_MAX_PAGES=2 by default):
  uv run python -m nuget_pipeline.sync.nuget

  # Or launch Dagster and trigger raw_nuget_packages from the UI:
  DAGSTER_HOME=$(pwd)/.dagster_home uv run dagster dev -h 0.0.0.0 -p 3000

  # Then open http://localhost:3000 in your browser.

  # Inspect what got written:
  psql "$DATABASE_URL" -c 'SELECT source, watermark, last_sync_at, rows_synced FROM raw.sync_state;'
  psql "$DATABASE_URL" -c 'SELECT COUNT(*) FROM raw.nuget_versions;'

──────────────────────────────────────────────────────────────
EOF
