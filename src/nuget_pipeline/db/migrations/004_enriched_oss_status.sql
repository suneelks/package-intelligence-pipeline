-- Enrichment layer: OSS classification.
-- Two new tables:
--   * raw.spdx_licenses        — reference data, synced from SPDX upstream
--   * enriched.nuget_package_oss_status — derived per-package classification
-- The classifier reads from raw.nuget_packages + raw.spdx_licenses and writes
-- one row per package_id. See sync/spdx.py and enrich/oss_status.py.

CREATE SCHEMA IF NOT EXISTS enriched;

-- ─── SPDX reference data ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw.spdx_licenses (
    license_id TEXT PRIMARY KEY,
    name TEXT,
    is_osi_approved BOOLEAN NOT NULL DEFAULT FALSE,
    is_fsf_libre BOOLEAN NOT NULL DEFAULT FALSE,
    is_deprecated_id BOOLEAN NOT NULL DEFAULT FALSE,
    reference_url TEXT,
    see_also_urls TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    raw_metadata JSONB,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_updated_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS spdx_licenses_osi_idx
    ON raw.spdx_licenses (is_osi_approved);

-- GIN over see_also_urls so URL→license lookup is index-supported. The
-- classifier loads the table into memory anyway, but this keeps the door
-- open for ad-hoc SQL ("which packages link to opensource.org/licenses/MIT?").
CREATE INDEX IF NOT EXISTS spdx_licenses_see_also_gin_idx
    ON raw.spdx_licenses USING gin (see_also_urls);

-- ─── Enrichment output ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS enriched.nuget_package_oss_status (
    package_id TEXT PRIMARY KEY,
    license_expression TEXT,
    license_url TEXT,
    spdx_id TEXT,
    spdx_normalized TEXT,
    is_osi_approved BOOLEAN,
    classification TEXT NOT NULL
        CHECK (classification IN ('open_source', 'proprietary', 'unknown')),
    reasoning TEXT,
    classified_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    source_synced_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS oss_status_classification_idx
    ON enriched.nuget_package_oss_status (classification);

CREATE INDEX IF NOT EXISTS oss_status_spdx_idx
    ON enriched.nuget_package_oss_status (spdx_id)
    WHERE spdx_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS oss_status_source_synced_at_idx
    ON enriched.nuget_package_oss_status (source_synced_at);
