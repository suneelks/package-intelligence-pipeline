-- NuGet pipeline raw layer + sync infrastructure.
-- Idempotent by construction; migration runner records applied versions.

CREATE SCHEMA IF NOT EXISTS raw;

-- ─── Sync infrastructure ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw.sync_state (
    source TEXT PRIMARY KEY,
    watermark TEXT NOT NULL,
    last_sync_at TIMESTAMPTZ,
    last_sync_status TEXT,
    rows_synced BIGINT NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS raw.sync_runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source TEXT NOT NULL,
    dagster_run_id TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    rows_inserted BIGINT NOT NULL DEFAULT 0,
    rows_updated BIGINT NOT NULL DEFAULT 0,
    rows_deleted BIGINT NOT NULL DEFAULT 0,
    watermark_before TEXT,
    watermark_after TEXT,
    error_message TEXT,
    error_details JSONB,
    duration_ms BIGINT GENERATED ALWAYS AS (
        CASE
            WHEN completed_at IS NULL THEN NULL
            ELSE (EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000)::BIGINT
        END
    ) STORED
);

CREATE INDEX IF NOT EXISTS sync_runs_source_started_at_idx
    ON raw.sync_runs (source, started_at DESC);

CREATE INDEX IF NOT EXISTS sync_runs_status_idx
    ON raw.sync_runs (status)
    WHERE status IN ('running', 'failed');

-- ─── NuGet raw tables ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw.nuget_packages (
    package_id TEXT PRIMARY KEY,
    latest_version TEXT,
    project_url TEXT,
    license TEXT,
    raw_metadata JSONB,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_updated_at TIMESTAMPTZ,
    deleted_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS nuget_packages_source_updated_at_idx
    ON raw.nuget_packages (source_updated_at DESC);

CREATE TABLE IF NOT EXISTS raw.nuget_versions (
    package_id TEXT NOT NULL,
    version TEXT NOT NULL,
    purl TEXT NOT NULL,
    major INTEGER,
    minor INTEGER,
    patch INTEGER,
    prerelease TEXT,
    published_at TIMESTAMPTZ,
    listed BOOLEAN NOT NULL DEFAULT TRUE,
    deprecated BOOLEAN NOT NULL DEFAULT FALSE,
    deprecation_reasons TEXT[],
    alternative_package TEXT,
    deprecation_message TEXT,
    raw_metadata JSONB,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (package_id, version)
);

CREATE INDEX IF NOT EXISTS nuget_versions_purl_idx
    ON raw.nuget_versions (purl);

CREATE INDEX IF NOT EXISTS nuget_versions_published_at_idx
    ON raw.nuget_versions (published_at DESC);

CREATE INDEX IF NOT EXISTS nuget_versions_deprecated_idx
    ON raw.nuget_versions (package_id)
    WHERE deprecated = TRUE;
