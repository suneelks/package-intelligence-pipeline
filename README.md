# package-intelligence-pipeline

A NuGet package classification pipeline, built with [Dagster](https://dagster.io)
and Postgres. Ingests the NuGet Catalog, classifies each package as
`open_source` / `proprietary` / `unknown` against the SPDX license list,
and exposes the results for downstream consumers.

This repository is the PoC behind a multi-part blog series on data
orchestrator requirements at [suneelcodes.com](https://suneelcodes.com).

## Status: source-available, not open source

This repository is published publicly so readers of the blog series can
inspect the implementation alongside the articles. **It is not licensed
for reuse.** See the [LICENSE](LICENSE) file for full terms.

In short:

- You may **read** the code on GitHub or clone it locally for reading.
- You may **not** copy, modify, redistribute, or use any part of it in
  another project, personal or commercial, without prior written
  permission.

If you'd like to use any portion of this work, reach out first.

## The blog series

Articles, in order:

1. [Requirements for a Data Orchestrator (Part 1)](https://suneelcodes.com/blog/requirements-for-a-data-orchestrator-part-1/)
   — six requirements I'd hold any data orchestrator to.
2. **Dagster, Airflow, or Prefect (Part 2)** — comparing the three
   against those requirements, with a verdict. Link added on publication.
3. **Architecture of the NuGet Classification Pipeline in Dagster
   (Part 3)** — the asset graph, partitions, sensors, and quality
   checks as actually built in this repo. Link added on publication.

Subsequent parts go deeper into the ingestion and enrichment phases.

## Architecture

Four conceptual phases, three Dagster assets in the current code:

```
                ┌──────────────────────┐         ┌──────────────────────┐
   Ingest  ──▶  │  raw_nuget_packages  │ ──┐
                └──────────────────────┘   │
                                           ▼
                                ┌──────────────────────────────────┐
                  Enrich  ──▶   │  enriched_nuget_package_oss_     │
                                │  status                          │ ──▶ Postgres
                                └──────────────────────────────────┘
                                           ▲
                ┌──────────────────────┐   │
  Reference──▶  │  raw_spdx_licenses   │ ──┘
                └──────────────────────┘
```

- **Ingest:** walks `https://api.nuget.org/v3/catalog0/index.json`,
  upserts packages and versions into `raw.nuget_packages` and
  `raw.nuget_versions`. Watermark = the catalog's `commitTimeStamp`.
- **Reference data:** weekly sync of the SPDX license list into
  `raw.spdx_licenses`.
- **Enrich / classify:** deterministic license-expression + URL
  heuristic against the SPDX list, output into
  `enriched.nuget_package_oss_status`. Driven by an eager
  `AutomationCondition` so it materialises as soon as either upstream
  asset updates.
- **Index:** Postgres only, today. No OpenSearch.

### What the pipeline actually delivers

Three Dagster assets in three groups: `nuget`, `reference`, `enrichment`.
Each ships with:

- `dg.TableSchema` metadata under `dagster/column_schema` so the UI
  shows the column shape on every materialisation.
- An asset check per asset (`raw_nuget_packages_nonempty`,
  `raw_spdx_licenses_osi_floor`, `oss_status_unknown_ratio`). The two
  ERROR checks are blocking, so a failure on the raw layer stops the
  enrichment from running on bad data.
- A freshness check (12 h for NuGet, 10 days for SPDX, 24 h for
  enriched).
- Per-batch `AssetObservation` events with structured progress
  metadata, surfaced on every materialisation in the Dagster UI.

## Local development

The repository ships with a devcontainer. From VS Code with the Dev
Containers extension installed:

1. Open the repository in VS Code.
2. **Command Palette → Dev Containers: Reopen in Container.**
3. The post-create script installs Python dependencies (`uv sync`),
   waits for Postgres, applies migrations, and prints next-step
   commands.

Then, from the devcontainer terminal:

```sh
# Run the test suite.
uv run pytest -v

# Smoke-test the NuGet sync (NUGET_MAX_PAGES=2 by default in the dev env).
uv run python -m nuget_pipeline.sync.nuget

# Launch Dagster and materialise from the UI.
DAGSTER_HOME=$(pwd)/.dagster_home uv run dagster dev -h 0.0.0.0 -p 3000
# Open http://localhost:3000
```

### Without the devcontainer

`uv` + Python 3.12 + a Postgres 16 instance is enough. Copy
[deploy/.env.example](deploy/.env.example) to `.env`, point
`DATABASE_URL` at your Postgres, and run:

```sh
uv sync
uv run python -m nuget_pipeline.db.migrate
uv run pytest -v
```

## Production deployment

The [deploy/](deploy/) directory contains a `docker-compose.yml`
that runs Postgres + the Dagster webserver + the Dagster daemon. By
default the Dagster UI binds to loopback (`127.0.0.1`); set
`DAGSTER_UI_BIND_IP` in `.env` to bind it to a specific private
interface instead of the public internet.

The Dagster instance has telemetry explicitly disabled in
[deploy/dagster.yaml](deploy/dagster.yaml).

## Tech stack

- **Python 3.12**, [uv](https://docs.astral.sh/uv/) for dependency
  management.
- **[Dagster](https://dagster.io) 1.9+** for orchestration.
- **Postgres 16** via `psycopg` and `dagster-postgres`.
- **httpx** + **tenacity** for HTTP; **pydantic** for schemas;
  **structlog** for logging; **packageurl-python** for PURLs.
- **pytest** + **respx** for tests (real Postgres, mocked HTTP).

## License

All rights reserved. See [LICENSE](LICENSE) for the full text.
