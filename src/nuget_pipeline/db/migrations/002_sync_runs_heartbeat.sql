-- Heartbeat columns on raw.sync_runs so a running row reflects in-flight
-- progress instead of staying at zero until completion.

ALTER TABLE raw.sync_runs
    ADD COLUMN IF NOT EXISTS pages_processed INT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;

-- Convenience: stuck-run detection by heartbeat staleness.
CREATE INDEX IF NOT EXISTS sync_runs_running_heartbeat_idx
    ON raw.sync_runs (last_heartbeat_at)
    WHERE status = 'running';
