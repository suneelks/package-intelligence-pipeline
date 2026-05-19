"""Sensors for operational health of the NuGet pipeline.

- staleness_sensor: re-triggers the job if the last successful sync is older
  than STALENESS_HOURS.
- zombie_run_sensor: marks `raw.sync_runs` rows as failed if they have been
  in 'running' state for longer than ZOMBIE_AFTER_HOURS (orphaned workers).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import dagster as dg
import psycopg

from nuget_pipeline.config import settings
from nuget_pipeline.dagster_defs.assets import nuget_job

STALENESS_HOURS = 12
ZOMBIE_AFTER_HOURS = 3


async def _last_success_time() -> datetime | None:
    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn, conn.cursor() as cur:
        await cur.execute(
            """
            SELECT completed_at FROM raw.sync_runs
            WHERE source = 'nuget' AND status = 'completed'
            ORDER BY completed_at DESC LIMIT 1
            """
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def _fail_zombies(cutoff: datetime) -> int:
    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn, conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE raw.sync_runs
            SET status = 'failed',
                completed_at = now(),
                error_message = 'zombie run: no completion within cutoff'
            WHERE source = 'nuget'
              AND status = 'running'
              AND started_at < %s
            """,
            (cutoff,),
        )
        affected = cur.rowcount
        await conn.commit()
    return affected or 0


@dg.sensor(
    name="nuget_staleness",
    job=nuget_job,
    minimum_interval_seconds=600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def nuget_staleness_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult | dg.SkipReason:
    last = asyncio.run(_last_success_time())
    now = datetime.now(timezone.utc)
    if last is None:
        reason = "no successful runs yet"
    else:
        age = now - last
        if age < timedelta(hours=STALENESS_HOURS):
            return dg.SkipReason(f"last success {age} ago, under {STALENESS_HOURS}h threshold")
        reason = f"stale by {age}"

    context.log.info(f"nuget stale: {reason} — triggering run")
    return dg.SensorResult(run_requests=[dg.RunRequest(run_key=f"staleness-{now.isoformat()}")])


@dg.sensor(
    name="nuget_zombie_cleanup",
    minimum_interval_seconds=900,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
def nuget_zombie_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult | dg.SkipReason:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=ZOMBIE_AFTER_HOURS)
    affected = asyncio.run(_fail_zombies(cutoff))
    if affected == 0:
        return dg.SkipReason("no zombies")
    context.log.warning(f"marked {affected} zombie sync_runs as failed")
    return dg.SensorResult(run_requests=[])
