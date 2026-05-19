"""SPDX license-list sync.

Fetches the canonical SPDX licenses.json and upserts every license into
`raw.spdx_licenses`. Watermark is the `licenseListVersion` string — the
list is forward-only versioned, so equality with the watermark means
"caught up" and we no-op.

The list is small (~600 entries) and changes ~quarterly, so we do a full
rewrite per release rather than a diff. Costs ~one round-trip and ~100ms.
"""

from __future__ import annotations

import json
import time
from typing import Any

from psycopg.types.json import Jsonb
from pydantic import BaseModel, Field

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
from nuget_pipeline.utils.http import get_json, http_client
from nuget_pipeline.utils.logging import get_logger

log = get_logger(__name__)

SOURCE = "spdx"


class SpdxLicense(BaseModel):
    license_id: str = Field(alias="licenseId")
    name: str | None = None
    is_osi_approved: bool = Field(default=False, alias="isOsiApproved")
    is_fsf_libre: bool = Field(default=False, alias="isFsfLibre")
    is_deprecated_id: bool = Field(default=False, alias="isDeprecatedLicenseId")
    reference: str | None = None
    see_also: list[str] = Field(default_factory=list, alias="seeAlso")


class SpdxIndex(BaseModel):
    license_list_version: str = Field(alias="licenseListVersion")
    release_date: str | None = Field(default=None, alias="releaseDate")
    licenses: list[SpdxLicense]


_INSERT_HEAD = """
INSERT INTO raw.spdx_licenses
    (license_id, name, is_osi_approved, is_fsf_libre, is_deprecated_id,
     reference_url, see_also_urls, raw_metadata, synced_at, source_updated_at)
VALUES
"""

_INSERT_TAIL = """
ON CONFLICT (license_id) DO UPDATE SET
    name = EXCLUDED.name,
    is_osi_approved = EXCLUDED.is_osi_approved,
    is_fsf_libre = EXCLUDED.is_fsf_libre,
    is_deprecated_id = EXCLUDED.is_deprecated_id,
    reference_url = EXCLUDED.reference_url,
    see_also_urls = EXCLUDED.see_also_urls,
    raw_metadata = EXCLUDED.raw_metadata,
    synced_at = EXCLUDED.synced_at,
    source_updated_at = EXCLUDED.source_updated_at
"""

_PLACEHOLDER = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"


async def _worker(ctx: SyncContext) -> SyncResult:
    started_monotonic = time.monotonic()

    ctx.log_progress("spdx.sync.start", watermark=ctx.watermark)

    async with http_client() as client:
        data = await get_json(client, settings.spdx_licenses_url)

    index = SpdxIndex.model_validate(data)
    ctx.log_progress(
        "spdx.fetched",
        version=index.license_list_version,
        release_date=index.release_date,
        license_count=len(index.licenses),
    )

    # Equality check: SPDX versions are forward-only ("3.24.0" → "3.25.0"). If
    # the upstream version equals our watermark we're already in sync. We do
    # not lexicographic-compare — "3.10.0" < "3.9.0" is a footgun.
    if ctx.watermark != INITIAL_WATERMARK and index.license_list_version == ctx.watermark:
        ctx.log_progress("spdx.caught_up", version=ctx.watermark)
        ctx.emit_observation(
            version=index.license_list_version,
            release_date=index.release_date,
            license_count=len(index.licenses),
            rewritten=False,
            elapsed_s=round(time.monotonic() - started_monotonic, 2),
        )
        return SyncResult(
            watermark=ctx.watermark,
            status="completed",
            metadata={
                "version": index.license_list_version,
                "license_count": len(index.licenses),
                "rewritten": False,
            },
        )

    from datetime import datetime

    now = datetime.now().astimezone()
    source_updated_at = (
        datetime.fromisoformat(index.release_date.replace("Z", "+00:00"))
        if index.release_date
        else None
    )

    osi_count = sum(1 for lic in index.licenses if lic.is_osi_approved)
    fsf_count = sum(1 for lic in index.licenses if lic.is_fsf_libre)
    deprecated_count = sum(1 for lic in index.licenses if lic.is_deprecated_id)
    see_also_total = sum(len(lic.see_also) for lic in index.licenses)

    rows: list[tuple[Any, ...]] = []
    for lic in index.licenses:
        rows.append(
            (
                lic.license_id,
                lic.name,
                lic.is_osi_approved,
                lic.is_fsf_libre,
                lic.is_deprecated_id,
                lic.reference,
                lic.see_also,
                Jsonb(lic.model_dump(by_alias=True, mode="json")),
                now,
                source_updated_at,
            )
        )

    async with transaction() as conn, conn.cursor() as cur:
        values_sql = ",\n".join([_PLACEHOLDER] * len(rows))
        await cur.execute(
            _INSERT_HEAD + values_sql + _INSERT_TAIL,
            [v for row in rows for v in row],
        )
        ctx.stats.inserted += len(rows)
        ctx.pages_processed = 1

        await advance_watermark(
            conn,
            SOURCE,
            index.license_list_version,
            rows_synced=ctx.stats.inserted,
            status="running",
        )
        await heartbeat_run(
            conn,
            ctx.run_id,
            stats=ctx.stats,
            pages_processed=ctx.pages_processed,
            watermark_after=index.license_list_version,
        )

    elapsed = max(time.monotonic() - started_monotonic, 0.001)
    ctx.log_progress(
        "spdx.upserted",
        version=index.license_list_version,
        license_count=len(index.licenses),
        osi_count=osi_count,
        fsf_count=fsf_count,
        deprecated_count=deprecated_count,
        see_also_total=see_also_total,
        elapsed_s=round(elapsed, 2),
    )

    ctx.emit_observation(
        version=index.license_list_version,
        release_date=index.release_date,
        license_count=len(index.licenses),
        osi_count=osi_count,
        fsf_count=fsf_count,
        deprecated_count=deprecated_count,
        see_also_total=see_also_total,
        rewritten=True,
        elapsed_s=round(elapsed, 2),
        http_requests=ctx.http_metrics.requests,
    )

    return SyncResult(
        watermark=index.license_list_version,
        status="completed",
        metadata={
            "version": index.license_list_version,
            "release_date": index.release_date,
            "license_count": len(index.licenses),
            "osi_count": osi_count,
            "fsf_count": fsf_count,
            "deprecated_count": deprecated_count,
            "see_also_total": see_also_total,
            "rewritten": True,
            "elapsed_s": round(elapsed, 2),
            "http_requests": ctx.http_metrics.requests,
        },
    )


async def sync_spdx(
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


# `python -m nuget_pipeline.sync.spdx`
if __name__ == "__main__":
    import asyncio

    from nuget_pipeline.db.connection import close_pool
    from nuget_pipeline.utils.logging import configure_logging

    async def _main() -> None:
        configure_logging()
        try:
            result = await sync_spdx()
            print(
                json.dumps(
                    {
                        "status": result.status,
                        "watermark": result.watermark,
                        "license_count": result.metadata.get("license_count"),
                        "rewritten": result.metadata.get("rewritten"),
                    }
                )
            )
        finally:
            await close_pool()

    asyncio.run(_main())
