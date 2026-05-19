-- Track deletions on individual version rows. `raw.nuget_packages.deleted_at`
-- already exists (001) but the version-grain column was missing — most NuGet
-- PackageDelete events apply to a specific version, not the whole package.

ALTER TABLE raw.nuget_versions
    ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS nuget_versions_deleted_at_idx
    ON raw.nuget_versions (deleted_at)
    WHERE deleted_at IS NOT NULL;
