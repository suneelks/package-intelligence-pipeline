"""Tests for the generic sync framework (`run_sync`, watermark, audit, shutdown).

Uses a fake worker function so we don't touch HTTP. Exercises against the
real Postgres to catch schema/query drift.
"""

from __future__ import annotations

import asyncio

import psycopg
import pytest

from nuget_pipeline.config import settings
from nuget_pipeline.db.connection import transaction
from nuget_pipeline.sync.framework import (
    SyncContext,
    SyncResult,
    advance_watermark,
    run_sync,
)


async def _fetch_state(source: str) -> dict | None:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT source, watermark, last_sync_status, rows_synced "
                "FROM raw.sync_state WHERE source = %s",
                (source,),
            )
            row = await cur.fetchone()
    finally:
        await conn.close()
    if row is None:
        return None
    return {"source": row[0], "watermark": row[1], "status": row[2], "rows_synced": row[3]}


async def _fetch_runs(source: str) -> list[tuple]:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT status, rows_inserted, rows_updated, watermark_before, "
                "watermark_after, error_message FROM raw.sync_runs "
                "WHERE source = %s ORDER BY started_at",
                (source,),
            )
            return await cur.fetchall()
    finally:
        await conn.close()


async def test_run_sync_completes_and_advances_watermark() -> None:
    async def worker(ctx: SyncContext) -> SyncResult:
        async with transaction() as conn:
            await advance_watermark(conn, ctx.source, "2025-01-01T00:00:00Z", rows_synced=3)
        ctx.stats.inserted = 3
        return SyncResult(watermark="2025-01-01T00:00:00Z", status="completed")

    result = await run_sync(source="test-source", worker=worker)

    assert result.status == "completed"
    assert result.watermark == "2025-01-01T00:00:00Z"

    state = await _fetch_state("test-source")
    assert state == {
        "source": "test-source",
        "watermark": "2025-01-01T00:00:00Z",
        "status": "completed",
        "rows_synced": 3,
    }

    runs = await _fetch_runs("test-source")
    assert len(runs) == 1
    status, inserted, updated, wm_before, wm_after, err = runs[0]
    assert status == "completed"
    assert inserted == 3
    assert wm_before == "0"
    assert wm_after == "2025-01-01T00:00:00Z"
    assert err is None


async def test_run_sync_resumes_from_prior_watermark() -> None:
    async def first(ctx: SyncContext) -> SyncResult:
        assert ctx.watermark == "0"
        return SyncResult(watermark="A", status="completed")

    async def second(ctx: SyncContext) -> SyncResult:
        assert ctx.watermark == "A"  # picked up from prior run's persisted watermark
        return SyncResult(watermark="B", status="completed")

    await run_sync(source="resume-test", worker=first)
    await run_sync(source="resume-test", worker=second)

    state = await _fetch_state("resume-test")
    assert state["watermark"] == "B"


async def test_run_sync_records_failure() -> None:
    async def worker(ctx: SyncContext) -> SyncResult:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        await run_sync(source="fail-test", worker=worker)

    runs = await _fetch_runs("fail-test")
    assert len(runs) == 1
    status, _, _, _, _, err = runs[0]
    assert status == "failed"
    assert err == "boom"

    state = await _fetch_state("fail-test")
    assert state["status"] == "failed"


async def test_run_sync_honours_shutdown_event() -> None:
    """If the worker observes `ctx.is_shutting_down()` and returns 'partial',
    run_sync records it cleanly rather than crashing."""

    async def worker(ctx: SyncContext) -> SyncResult:
        for _ in range(100):
            if ctx.is_shutting_down():
                return SyncResult(watermark="mid", status="partial")
            await asyncio.sleep(0.01)
        return SyncResult(watermark="end", status="completed")

    async def fire_shutdown_soon(shutdown: asyncio.Event) -> None:
        await asyncio.sleep(0.05)
        shutdown.set()

    # Patch shutdown event: easiest is to run worker, then externally
    # wake it via the ctx's shutdown_event. Since run_sync installs signal
    # handlers, we instead wire shutdown from within the worker on tick 5.
    async def worker_self_shutdown(ctx: SyncContext) -> SyncResult:
        for i in range(100):
            if i == 5:
                ctx.shutdown_event.set()
            if ctx.is_shutting_down():
                return SyncResult(watermark=f"tick-{i}", status="partial")
            await asyncio.sleep(0.001)
        return SyncResult(watermark="end", status="completed")

    result = await run_sync(source="shutdown-test", worker=worker_self_shutdown)
    assert result.status == "partial"
    assert result.watermark.startswith("tick-")

    state = await _fetch_state("shutdown-test")
    assert state["status"] == "partial"
