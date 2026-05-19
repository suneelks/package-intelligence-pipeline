"""Shared sync-run machinery: watermark read/write, run audit, shutdown.

Each sync (there is one today: NuGet) calls `run_sync` with a source name,
its current-watermark-aware `execute` coroutine, and optional dagster context.
The framework opens a sync_run row, loads the watermark, invokes the worker,
and records the outcome — advancing the watermark in a single atomic write
per batch via `advance_watermark` (called by the worker).
"""

from __future__ import annotations

import asyncio
import json
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID

from psycopg import AsyncConnection

from nuget_pipeline.db.connection import connection, transaction
from nuget_pipeline.utils.http import HTTPMetrics, http_metrics_var
from nuget_pipeline.utils.logging import get_logger

log = get_logger(__name__)

INITIAL_WATERMARK = "0"

SyncStatus = Literal["completed", "partial", "failed"]


@dataclass
class SyncStats:
    inserted: int = 0
    updated: int = 0
    deleted: int = 0


ObserveCallback = Callable[[dict[str, Any]], None]


@dataclass
class SyncContext:
    source: str
    watermark: str
    run_id: UUID
    shutdown_event: asyncio.Event
    stats: SyncStats = field(default_factory=SyncStats)
    pages_processed: int = 0
    http_metrics: HTTPMetrics = field(default_factory=HTTPMetrics)
    dagster_log: Any | None = None
    observe: ObserveCallback | None = None

    def is_shutting_down(self) -> bool:
        return self.shutdown_event.is_set()

    def log_progress(self, message: str, **fields: Any) -> None:
        log.info(message, source=self.source, run_id=str(self.run_id), **fields)
        if self.dagster_log is not None:
            self.dagster_log.info(message, extra=fields)

    def emit_observation(self, **metadata: Any) -> None:
        """Emit a structured progress event. The Dagster asset wires this
        into `context.log_event(AssetObservation(...))` so the UI shows a
        timeline of progress; in non-Dagster contexts it is a no-op."""
        if self.observe is not None:
            self.observe(metadata)


@dataclass
class SyncResult:
    watermark: str
    status: SyncStatus
    metadata: dict[str, Any] = field(default_factory=dict)


Worker = Callable[[SyncContext], Awaitable[SyncResult]]


async def _load_watermark(conn: AsyncConnection, source: str) -> str:
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT watermark FROM raw.sync_state WHERE source = %s",
            (source,),
        )
        row = await cur.fetchone()
    return row["watermark"] if row else INITIAL_WATERMARK


async def _start_run(
    conn: AsyncConnection,
    source: str,
    dagster_run_id: str | None,
    watermark_before: str,
) -> UUID:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO raw.sync_runs (source, dagster_run_id, watermark_before, status)
            VALUES (%s, %s, %s, 'running')
            RETURNING id
            """,
            (source, dagster_run_id, watermark_before),
        )
        row = await cur.fetchone()
    await conn.commit()
    return row["id"]


async def _finalise_run(
    conn: AsyncConnection,
    run_id: UUID,
    *,
    status: SyncStatus,
    stats: SyncStats,
    pages_processed: int,
    watermark_after: str,
    error_message: str | None = None,
    error_details: dict[str, Any] | None = None,
) -> None:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE raw.sync_runs
            SET completed_at = now(),
                status = %s,
                rows_inserted = %s,
                rows_updated = %s,
                rows_deleted = %s,
                pages_processed = %s,
                watermark_after = %s,
                error_message = %s,
                error_details = %s,
                last_heartbeat_at = now()
            WHERE id = %s
            """,
            (
                status,
                stats.inserted,
                stats.updated,
                stats.deleted,
                pages_processed,
                watermark_after,
                error_message,
                json.dumps(error_details) if error_details else None,
                str(run_id),
            ),
        )
    await conn.commit()


async def heartbeat_run(
    conn: AsyncConnection,
    run_id: UUID,
    *,
    stats: SyncStats,
    pages_processed: int,
    watermark_after: str,
) -> None:
    """In-flight progress update on a running sync_runs row.

    Call this from the worker's per-batch transaction so the audit row
    reflects current progress and a crash leaves a usable forensic record.
    The `status = 'running'` guard prevents accidentally clobbering a row
    that has already been finalised.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            UPDATE raw.sync_runs
            SET rows_inserted = %s,
                rows_updated = %s,
                rows_deleted = %s,
                pages_processed = %s,
                watermark_after = %s,
                last_heartbeat_at = now()
            WHERE id = %s AND status = 'running'
            """,
            (
                stats.inserted,
                stats.updated,
                stats.deleted,
                pages_processed,
                watermark_after,
                str(run_id),
            ),
        )


async def advance_watermark(
    conn: AsyncConnection,
    source: str,
    watermark: str,
    *,
    rows_synced: int,
    status: str = "running",
) -> None:
    """Upsert `raw.sync_state` for `source`. Call this per batch from the worker,
    using the same connection as the data upsert so both commit atomically.
    """
    async with conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO raw.sync_state (source, watermark, last_sync_at, last_sync_status, rows_synced)
            VALUES (%s, %s, now(), %s, %s)
            ON CONFLICT (source) DO UPDATE SET
                watermark = EXCLUDED.watermark,
                last_sync_at = EXCLUDED.last_sync_at,
                last_sync_status = EXCLUDED.last_sync_status,
                rows_synced = EXCLUDED.rows_synced
            """,
            (source, watermark, status, rows_synced),
        )


def _install_shutdown_handlers(event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, event.set)


async def run_sync(
    *,
    source: str,
    worker: Worker,
    dagster_run_id: str | None = None,
    dagster_log: Any | None = None,
    observe: ObserveCallback | None = None,
) -> SyncResult:
    shutdown_event = asyncio.Event()
    _install_shutdown_handlers(shutdown_event)

    async with connection() as conn:
        watermark_before = await _load_watermark(conn, source)
        run_id = await _start_run(conn, source, dagster_run_id, watermark_before)

    ctx = SyncContext(
        source=source,
        watermark=watermark_before,
        run_id=run_id,
        shutdown_event=shutdown_event,
        dagster_log=dagster_log,
        observe=observe,
    )
    # Bind HTTP counters for this run so utils.http.get_json increments
    # them without callers needing to thread a metrics object through.
    metrics_token = http_metrics_var.set(ctx.http_metrics)
    ctx.log_progress(
        "sync.start",
        watermark_before=watermark_before,
        dagster_run_id=dagster_run_id,
    )

    try:
        try:
            worker_task = asyncio.create_task(worker(ctx))
            shutdown_task = asyncio.create_task(shutdown_event.wait())

            done, pending = await asyncio.wait(
                {worker_task, shutdown_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if worker_task in done:
                shutdown_task.cancel()
                with suppress(asyncio.CancelledError):
                    await shutdown_task
                result = worker_task.result()
            else:
                # Shutdown fired first — let worker observe ctx.is_shutting_down()
                # and exit its loop cleanly. We wait for it to wrap up.
                ctx.log_progress("sync.shutdown_signalled")
                result = await worker_task

        except Exception as exc:
            async with connection() as conn:
                await _finalise_run(
                    conn,
                    run_id,
                    status="failed",
                    stats=ctx.stats,
                    pages_processed=ctx.pages_processed,
                    watermark_after=ctx.watermark,
                    error_message=str(exc),
                    error_details={"type": type(exc).__name__},
                )
                await advance_watermark(
                    conn,
                    source,
                    ctx.watermark,
                    rows_synced=ctx.stats.inserted + ctx.stats.updated,
                    status="failed",
                )
                await conn.commit()
            ctx.log_progress("sync.failed", error=str(exc))
            raise

        async with connection() as conn:
            await _finalise_run(
                conn,
                run_id,
                status=result.status,
                stats=ctx.stats,
                pages_processed=ctx.pages_processed,
                watermark_after=result.watermark,
            )
            await advance_watermark(
                conn,
                source,
                result.watermark,
                rows_synced=ctx.stats.inserted + ctx.stats.updated,
                status=result.status,
            )
            await conn.commit()

        ctx.log_progress(
            "sync.done",
            status=result.status,
            watermark_after=result.watermark,
            inserted=ctx.stats.inserted,
            updated=ctx.stats.updated,
            http_requests=ctx.http_metrics.requests,
            http_429=ctx.http_metrics.http_429,
            http_5xx=ctx.http_metrics.http_5xx,
        )
        return result
    finally:
        http_metrics_var.reset(metrics_token)


__all__ = [
    "INITIAL_WATERMARK",
    "ObserveCallback",
    "SyncContext",
    "SyncResult",
    "SyncStats",
    "SyncStatus",
    "advance_watermark",
    "heartbeat_run",
    "run_sync",
    "transaction",
]
