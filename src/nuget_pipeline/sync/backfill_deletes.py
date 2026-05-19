"""One-shot backfill: walk the NuGet catalog, mark `deleted_at` on rows
whose catalog leaf is `nuget:PackageDelete`.

Re-uses the catalog walker in `sync.nuget` but acts on a *separate* sync_state
row (`nuget_deletes_backfill`) so it can resume independently of the main
incremental sync and never advances the main `nuget` watermark.

Semantics (option a, deliberately simple):
    * Walks every page from INITIAL_WATERMARK forward.
    * For each leaf ref whose `@type == nuget:PackageDelete`, sets
      `deleted_at = ref.commit_time_stamp` on the matching version row.
    * Skips fetching the leaf body — the ref already carries package_id,
      version, and timestamp.
    * UPDATE-only: rows that don't exist in `raw.nuget_versions` are not
      created. (A delete for a version we never ingested is a no-op.)
    * Edge case ignored on purpose: if a version was deleted at T1 and
      re-published at T2 > T1, this backfill marks it deleted. Acceptable
      because re-publishing after a NuGet delete is exceptionally rare —
      deletes are reserved for malware/takedowns. Forward incremental sync
      with proper @type handling will correct any false positive.
"""

from __future__ import annotations

import json
import time
from typing import Any

from nuget_pipeline.config import settings
from nuget_pipeline.db.connection import transaction
from nuget_pipeline.sync.framework import (
    INITIAL_WATERMARK,
    ObserveCallback,
    SyncContext,
    SyncResult,
    advance_watermark,
    heartbeat_run,
    run_sync,
)
from nuget_pipeline.sync.nuget import (
    PACKAGE_DELETE_TYPE,
    CatalogLeafRef,
    _fetch_index,
    _fetch_page,
)
from nuget_pipeline.utils.http import http_client
from nuget_pipeline.utils.logging import get_logger

log = get_logger(__name__)

SOURCE = "nuget_deletes_backfill"


_MARK_DELETED_SQL = """
UPDATE raw.nuget_versions
SET deleted_at = %s
WHERE package_id = %s
  AND version = %s
  AND (deleted_at IS NULL OR deleted_at < %s::timestamptz)
"""


async def _apply_deletes(
    refs: list[CatalogLeafRef],
    ctx: SyncContext,
    latest_watermark: list[str],
) -> None:
    deletes = [r for r in refs if r.type == PACKAGE_DELETE_TYPE]

    # Always advance the watermark across the full batch — even if no
    # deletes were present — so we don't re-walk pages we've already seen.
    for ref in refs:
        if ref.commit_time_stamp > latest_watermark[0]:
            latest_watermark[0] = ref.commit_time_stamp

    async with transaction() as conn, conn.cursor() as cur:
        if deletes:
            params = [
                (r.commit_time_stamp, r.package_id, r.version, r.commit_time_stamp)
                for r in deletes
            ]
            await cur.executemany(_MARK_DELETED_SQL, params)
            # cur.rowcount after executemany reflects the last statement only,
            # so we treat each delete ref as one "candidate" mark — not all
            # candidates necessarily hit a row (the version may not have been
            # ingested), but the count is what we attempted.
            ctx.stats.deleted += len(deletes)

        await advance_watermark(
            conn,
            SOURCE,
            latest_watermark[0],
            rows_synced=ctx.stats.deleted,
            status="running",
        )
        await heartbeat_run(
            conn,
            ctx.run_id,
            stats=ctx.stats,
            pages_processed=ctx.pages_processed,
            watermark_after=latest_watermark[0],
        )


async def _worker(ctx: SyncContext) -> SyncResult:
    latest_watermark = [ctx.watermark]
    max_pages = settings.nuget_max_pages
    started_monotonic = time.monotonic()

    ctx.log_progress(
        "nuget.backfill_deletes.start",
        watermark=ctx.watermark,
        max_pages=max_pages,
    )

    async with http_client() as client:
        index = await _fetch_index(client)

        filtered = [
            p
            for p in index.items
            if ctx.watermark == INITIAL_WATERMARK or p.commit_time_stamp > ctx.watermark
        ]
        filtered.sort(key=lambda p: p.commit_time_stamp)
        pages_total = len(filtered)

        ctx.log_progress(
            "nuget.backfill_deletes.index_fetched",
            total_pages=len(index.items),
            filtered_pages=pages_total,
            catalog_commit_time_stamp=index.commit_time_stamp,
        )

        if not filtered:
            ctx.log_progress("nuget.backfill_deletes.caught_up", watermark=ctx.watermark)

        for page_ref in filtered:
            if ctx.is_shutting_down() or (
                max_pages is not None and ctx.pages_processed >= max_pages
            ):
                break

            page = await _fetch_page(client, page_ref.url)
            ctx.log_progress(
                "nuget.backfill_deletes.page_fetched",
                url=page_ref.url,
                leaf_count=len(page.items),
                page_commit_time_stamp=page_ref.commit_time_stamp,
                pages_processed=ctx.pages_processed,
            )

            batch_size = settings.nuget_process_batch_size
            for i in range(0, len(page.items), batch_size):
                if ctx.is_shutting_down():
                    break
                batch = page.items[i : i + batch_size]
                await _apply_deletes(batch, ctx, latest_watermark)
                ctx.log_progress(
                    "nuget.backfill_deletes.batch_processed",
                    batch_size=len(batch),
                    deletes_so_far=ctx.stats.deleted,
                    watermark=latest_watermark[0],
                )

            ctx.pages_processed += 1
            elapsed = max(time.monotonic() - started_monotonic, 0.001)
            ctx.emit_observation(
                pages_processed=ctx.pages_processed,
                pages_total=pages_total,
                progress_pct=round(ctx.pages_processed / pages_total * 100, 2)
                if pages_total
                else 100.0,
                current_watermark=latest_watermark[0],
                deletes_marked=ctx.stats.deleted,
                pages_per_sec=round(ctx.pages_processed / elapsed, 2),
                http_requests=ctx.http_metrics.requests,
                http_429=ctx.http_metrics.http_429,
                http_5xx=ctx.http_metrics.http_5xx,
            )

    remaining_pages = pages_total - ctx.pages_processed
    stopped_early = ctx.is_shutting_down() or remaining_pages > 0
    status = "partial" if stopped_early else "completed"

    elapsed_total = max(time.monotonic() - started_monotonic, 0.001)
    return SyncResult(
        watermark=latest_watermark[0],
        status=status,
        metadata={
            "pages_processed": ctx.pages_processed,
            "pages_remaining": remaining_pages,
            "deletes_marked": ctx.stats.deleted,
            "watermark": latest_watermark[0],
            "pages_per_sec": round(ctx.pages_processed / elapsed_total, 2),
            "http_requests": ctx.http_metrics.requests,
            "http_429": ctx.http_metrics.http_429,
            "http_5xx": ctx.http_metrics.http_5xx,
        },
    )


async def backfill_nuget_deletes(
    dagster_run_id: str | None = None,
    dagster_log: Any | None = None,
    observe: ObserveCallback | None = None,
) -> SyncResult:
    return await run_sync(
        source=SOURCE,
        worker=_worker,
        dagster_run_id=dagster_run_id,
        dagster_log=dagster_log,
        observe=observe,
    )


# `python -m nuget_pipeline.sync.backfill_deletes`
if __name__ == "__main__":
    import asyncio

    from nuget_pipeline.db.connection import close_pool
    from nuget_pipeline.utils.logging import configure_logging

    async def _main() -> None:
        configure_logging()
        try:
            result = await backfill_nuget_deletes()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "watermark": result.watermark,
                        "deletes_marked": result.metadata.get("deletes_marked"),
                    }
                )
            )
        finally:
            await close_pool()

    asyncio.run(_main())
