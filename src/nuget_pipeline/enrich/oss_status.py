"""OSS-status enrichment worker.

Streams from raw.nuget_packages, applies the deterministic
license-expression + URL classifier, upserts into
enriched.nuget_package_oss_status.

Source identifier: `oss_status_classifier`. Watermark = ISO-8601 string of
the highest `raw.nuget_packages.synced_at` we've classified. On restart we
re-classify rows on the boundary (idempotent) so a crash mid-run leaves
no gap.

Dependency: raw.spdx_licenses must be populated. The worker loads it once
at start and raises if it's empty (deterministic failure beats silently
classifying everything as unknown).
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

from psycopg import AsyncConnection

from nuget_pipeline.config import settings
from nuget_pipeline.db.connection import connection, transaction
from nuget_pipeline.enrich.classifier import (
    Classification,
    SpdxLicense,
    build_url_index,
    classify,
)
from nuget_pipeline.sync.framework import (
    INITIAL_WATERMARK,
    ObserveCallback,
    SyncContext,
    SyncResult,
    advance_watermark,
    heartbeat_run,
    run_sync,
)
from nuget_pipeline.utils.logging import get_logger

log = get_logger(__name__)

SOURCE = "oss_status_classifier"


_UPSERT_HEAD = """
INSERT INTO enriched.nuget_package_oss_status
    (package_id, license_expression, license_url, spdx_id, spdx_normalized,
     is_osi_approved, classification, reasoning, classified_at,
     source_synced_at)
VALUES
"""

_UPSERT_TAIL = """
ON CONFLICT (package_id) DO UPDATE SET
    license_expression = EXCLUDED.license_expression,
    license_url = EXCLUDED.license_url,
    spdx_id = EXCLUDED.spdx_id,
    spdx_normalized = EXCLUDED.spdx_normalized,
    is_osi_approved = EXCLUDED.is_osi_approved,
    classification = EXCLUDED.classification,
    reasoning = EXCLUDED.reasoning,
    classified_at = EXCLUDED.classified_at,
    source_synced_at = EXCLUDED.source_synced_at
"""

_PLACEHOLDER = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


async def _load_spdx(
    conn: AsyncConnection,
) -> tuple[dict[str, SpdxLicense], dict[str, str]]:
    """Build the SPDX dict + URL index from raw.spdx_licenses."""
    licenses: list[SpdxLicense] = []
    see_also: dict[str, list[str]] = {}
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT license_id, is_osi_approved, is_deprecated_id, see_also_urls "
            "FROM raw.spdx_licenses"
        )
        rows = await cur.fetchall()
    for row in rows:
        lic = SpdxLicense(
            license_id=row["license_id"],
            is_osi_approved=row["is_osi_approved"],
            is_deprecated_id=row["is_deprecated_id"],
        )
        licenses.append(lic)
        see_also[lic.license_id] = list(row["see_also_urls"] or [])

    spdx_dict = {lic.license_id: lic for lic in licenses}
    url_index = build_url_index(licenses, see_also)
    return spdx_dict, url_index


def _decode_watermark(watermark: str) -> tuple[datetime | None, str]:
    """Decode the compound watermark: `<isoformat-synced-at>|<package_id>`.

    Compound is required: many `raw.nuget_packages` rows can share a
    `synced_at` (they're written in batches by the upstream NuGet sync).
    A `synced_at`-only watermark would either skip rows on resume or
    reprocess the boundary row across runs."""
    if watermark == INITIAL_WATERMARK:
        return None, ""
    if "|" not in watermark:
        # Backward-compat: an older deployment may have a synced_at-only
        # watermark. Treat it as the start of synced_at = T (empty pkg
        # cursor); a few boundary rows may be reprocessed once, harmless.
        return datetime.fromisoformat(watermark.replace("Z", "+00:00")), ""
    ts_str, pkg = watermark.split("|", 1)
    return datetime.fromisoformat(ts_str), pkg


def _encode_watermark(ts: datetime, package_id: str) -> str:
    return f"{ts.astimezone().isoformat()}|{package_id}"


async def _fetch_batch(
    conn: AsyncConnection,
    *,
    after_synced_at: datetime | None,
    after_pkg: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    """Keyset-paginated fetch over raw.nuget_packages, ordered by
    (synced_at, package_id) ascending.

    Using a compound cursor is slightly more code than a plain `synced_at >`
    cursor, but it stops boundary rows from being skipped when many packages
    share a synced_at (which happens during bulk-insert batches in the
    upstream NuGet sync)."""
    async with conn.cursor() as cur:
        if after_synced_at is None:
            await cur.execute(
                """
                SELECT package_id, license, raw_metadata, synced_at
                FROM raw.nuget_packages
                ORDER BY synced_at, package_id
                LIMIT %s
                """,
                (limit,),
            )
        else:
            await cur.execute(
                """
                SELECT package_id, license, raw_metadata, synced_at
                FROM raw.nuget_packages
                WHERE (synced_at, package_id) > (%s, %s)
                ORDER BY synced_at, package_id
                LIMIT %s
                """,
                (after_synced_at, after_pkg or "", limit),
            )
        return await cur.fetchall()


async def _upsert_batch(
    rows: list[tuple[str, dict[str, Any], Classification, datetime]],
    ctx: SyncContext,
    latest_watermark: list[str],
    tally: dict[str, int],
) -> None:
    if not rows:
        return

    now = datetime.now().astimezone()
    params: list[tuple[Any, ...]] = []
    for package_id, raw_pkg, cls, source_synced_at in rows:
        params.append(
            (
                package_id,
                raw_pkg["license"],
                _extract_license_url(raw_pkg["raw_metadata"]),
                cls.spdx_id,
                cls.spdx_normalized,
                cls.is_osi_approved,
                cls.classification,
                cls.reasoning,
                now,
                source_synced_at,
            )
        )

    async with transaction() as conn, conn.cursor() as cur:
        values_sql = ",\n".join([_PLACEHOLDER] * len(params))
        await cur.execute(
            _UPSERT_HEAD + values_sql + _UPSERT_TAIL,
            [v for row in params for v in row],
        )

        for _, _, cls, _ in rows:
            tally[cls.classification] = tally.get(cls.classification, 0) + 1
        ctx.stats.inserted += len(rows)

        await advance_watermark(
            conn,
            SOURCE,
            latest_watermark[0],
            rows_synced=ctx.stats.inserted,
            status="running",
        )
        await heartbeat_run(
            conn,
            ctx.run_id,
            stats=ctx.stats,
            pages_processed=ctx.pages_processed,
            watermark_after=latest_watermark[0],
        )


def _extract_license_url(raw_metadata: dict[str, Any] | None) -> str | None:
    if not raw_metadata:
        return None
    url = raw_metadata.get("licenseUrl")
    return url if isinstance(url, str) and url.strip() else None


async def _count_candidates(
    conn: AsyncConnection, after_synced_at: datetime | None, after_pkg: str
) -> int:
    """Count rows the classifier will process in this run. One COUNT(*) at
    start so per-batch observations can compute progress_pct. Cheap (~one
    index scan); table is bounded by the NuGet catalog size."""
    async with conn.cursor() as cur:
        if after_synced_at is None:
            await cur.execute("SELECT count(*) AS n FROM raw.nuget_packages")
        else:
            await cur.execute(
                "SELECT count(*) AS n FROM raw.nuget_packages "
                "WHERE (synced_at, package_id) > (%s, %s)",
                (after_synced_at, after_pkg),
            )
        row = await cur.fetchone()
    return int(row["n"]) if row else 0


async def _worker(ctx: SyncContext) -> SyncResult:
    started_monotonic = time.monotonic()
    latest_watermark = [ctx.watermark]
    batch_size = settings.oss_classifier_batch_size

    ctx.log_progress("oss_status.start", watermark=ctx.watermark, batch_size=batch_size)

    async with connection() as conn:
        spdx_dict, url_index = await _load_spdx(conn)

    if not spdx_dict:
        raise RuntimeError(
            "raw.spdx_licenses is empty — run sync_spdx before classifying"
        )

    ctx.log_progress(
        "oss_status.spdx_loaded",
        license_count=len(spdx_dict),
        url_index_size=len(url_index),
        osi_approved_count=sum(1 for s in spdx_dict.values() if s.is_osi_approved),
    )

    after_synced_at, after_pkg = _decode_watermark(ctx.watermark)

    async with connection() as count_conn:
        candidates_total = await _count_candidates(count_conn, after_synced_at, after_pkg)

    ctx.log_progress(
        "oss_status.candidates_counted",
        candidates_total=candidates_total,
        watermark=ctx.watermark,
    )

    if candidates_total == 0:
        ctx.log_progress("oss_status.caught_up", watermark=ctx.watermark)
        # Emit at least one observation so the UI run isn't empty.
        ctx.emit_observation(
            candidates_total=0,
            rows_classified=0,
            progress_pct=100.0,
            current_watermark=latest_watermark[0],
        )
        return SyncResult(
            watermark=latest_watermark[0],
            status="completed",
            metadata={
                "pages_processed": 0,
                "candidates_total": 0,
                "rows_classified": 0,
                "open_source": 0,
                "proprietary": 0,
                "unknown": 0,
                "signal_expression": 0,
                "signal_url": 0,
                "signal_none": 0,
                "spdx_license_count": len(spdx_dict),
                "watermark": latest_watermark[0],
                "elapsed_s": round(time.monotonic() - started_monotonic, 2),
            },
        )

    pages = 0
    tally: dict[str, int] = {"open_source": 0, "proprietary": 0, "unknown": 0}
    signal_tally: dict[str, int] = {"expression": 0, "url": 0, "none": 0}

    async with connection() as data_conn:
        while not ctx.is_shutting_down():
            rows = await _fetch_batch(
                data_conn,
                after_synced_at=after_synced_at,
                after_pkg=after_pkg,
                limit=batch_size,
            )
            if not rows:
                break

            classified: list[tuple[str, dict[str, Any], Classification, datetime]] = []
            for row in rows:
                license_expr = row["license"]
                license_url = _extract_license_url(row["raw_metadata"])
                cls = classify(license_expr, license_url, spdx_dict, url_index)
                classified.append((row["package_id"], row, cls, row["synced_at"]))

                if license_expr and license_expr.strip():
                    signal_tally["expression"] += 1
                elif license_url and license_url.strip():
                    signal_tally["url"] += 1
                else:
                    signal_tally["none"] += 1

            last = rows[-1]
            after_synced_at = last["synced_at"]
            after_pkg = last["package_id"]
            latest_watermark[0] = _encode_watermark(last["synced_at"], last["package_id"])

            await _upsert_batch(classified, ctx, latest_watermark, tally)

            pages += 1
            ctx.pages_processed = pages

            elapsed = max(time.monotonic() - started_monotonic, 0.001)
            progress_pct = (
                round(min(ctx.stats.inserted / candidates_total * 100, 100.0), 2)
                if candidates_total
                else 100.0
            )

            ctx.log_progress(
                "oss_status.batch_processed",
                page=pages,
                batch_size=len(rows),
                rows_classified=ctx.stats.inserted,
                progress_pct=progress_pct,
                watermark=latest_watermark[0],
            )

            ctx.emit_observation(
                pages_processed=pages,
                candidates_total=candidates_total,
                progress_pct=progress_pct,
                rows_classified=ctx.stats.inserted,
                open_source=tally["open_source"],
                proprietary=tally["proprietary"],
                unknown=tally["unknown"],
                signal_expression=signal_tally["expression"],
                signal_url=signal_tally["url"],
                signal_none=signal_tally["none"],
                rows_per_sec=round(ctx.stats.inserted / elapsed, 2),
                current_watermark=latest_watermark[0],
            )

    elapsed_total = max(time.monotonic() - started_monotonic, 0.001)
    status = "partial" if ctx.is_shutting_down() else "completed"

    ctx.log_progress(
        "oss_status.done",
        status=status,
        rows_classified=ctx.stats.inserted,
        open_source=tally["open_source"],
        proprietary=tally["proprietary"],
        unknown=tally["unknown"],
        signal_expression=signal_tally["expression"],
        signal_url=signal_tally["url"],
        signal_none=signal_tally["none"],
        elapsed_s=round(elapsed_total, 2),
    )

    return SyncResult(
        watermark=latest_watermark[0],
        status=status,
        metadata={
            "pages_processed": pages,
            "candidates_total": candidates_total,
            "rows_classified": ctx.stats.inserted,
            "open_source": tally["open_source"],
            "proprietary": tally["proprietary"],
            "unknown": tally["unknown"],
            "signal_expression": signal_tally["expression"],
            "signal_url": signal_tally["url"],
            "signal_none": signal_tally["none"],
            "rows_per_sec": round(ctx.stats.inserted / elapsed_total, 2),
            "spdx_license_count": len(spdx_dict),
            "watermark": latest_watermark[0],
            "elapsed_s": round(elapsed_total, 2),
        },
    )


async def classify_oss_status(
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


# `python -m nuget_pipeline.enrich.oss_status`
if __name__ == "__main__":
    import asyncio

    from nuget_pipeline.db.connection import close_pool
    from nuget_pipeline.utils.logging import configure_logging

    async def _main() -> None:
        configure_logging()
        try:
            result = await classify_oss_status()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "watermark": result.watermark,
                        "rows_classified": result.metadata.get("rows_classified"),
                        "open_source": result.metadata.get("open_source"),
                        "proprietary": result.metadata.get("proprietary"),
                        "unknown": result.metadata.get("unknown"),
                    }
                )
            )
        finally:
            await close_pool()

    asyncio.run(_main())
