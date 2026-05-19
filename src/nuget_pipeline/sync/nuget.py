"""NuGet Catalog sync.

Walks https://api.nuget.org/v3/catalog0/index.json -> pages -> leaves,
upserting packages and versions into the raw layer. Watermark is the ISO 8601
`commitTimeStamp` of the highest-seen catalog entry; advanced per batch.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime
from typing import Any

import httpx
from packageurl import PackageURL
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
from nuget_pipeline.utils.concurrency import gather_bounded
from nuget_pipeline.utils.http import get_json, http_client
from nuget_pipeline.utils.logging import get_logger

log = get_logger(__name__)

SOURCE = "nuget"

# ─── Catalog response models ────────────────────────────────────────────────


class CatalogPageRef(BaseModel):
    url: str = Field(alias="@id")
    commit_time_stamp: str = Field(alias="commitTimeStamp")
    count: int | None = None


class CatalogIndex(BaseModel):
    commit_time_stamp: str = Field(alias="commitTimeStamp")
    items: list[CatalogPageRef]


class CatalogLeafRef(BaseModel):
    url: str = Field(alias="@id")
    commit_time_stamp: str = Field(alias="commitTimeStamp")
    package_id: str = Field(alias="nuget:id")
    version: str = Field(alias="nuget:version")
    type: str = Field(alias="@type")


PACKAGE_DELETE_TYPE = "nuget:PackageDelete"
PACKAGE_DETAILS_TYPE = "nuget:PackageDetails"


class CatalogPage(BaseModel):
    items: list[CatalogLeafRef]


class CatalogLeafDeprecation(BaseModel):
    reasons: list[str] | None = None
    alternate_package: dict[str, Any] | None = Field(default=None, alias="alternatePackage")
    message: str | None = None


class CatalogLeaf(BaseModel):
    id: str
    version: str
    published: str
    listed: bool | None = None
    project_url: str | None = Field(default=None, alias="projectUrl")
    license_expression: str | None = Field(default=None, alias="licenseExpression")
    deprecation: CatalogLeafDeprecation | None = None


# ─── Semver parsing (permissive — NuGet allows non-strict SemVer) ───────────

_SEMVER_RE = re.compile(
    r"^(?P<major>\d+)(?:\.(?P<minor>\d+))?(?:\.(?P<patch>\d+))?"
    r"(?:-(?P<prerelease>[0-9A-Za-z.-]+))?(?:\+[0-9A-Za-z.-]+)?$"
)

INT32_MAX = 2_147_483_647


def _parse_semver(version: str) -> tuple[int | None, int | None, int | None, str | None]:
    m = _SEMVER_RE.match(version.strip())
    if not m:
        return None, None, None, None

    def _safe_int(raw: str | None) -> int | None:
        if raw is None:
            return None
        try:
            n = int(raw)
        except ValueError:
            return None
        return n if 0 <= n <= INT32_MAX else None

    return (
        _safe_int(m.group("major")),
        _safe_int(m.group("minor")),
        _safe_int(m.group("patch")),
        m.group("prerelease"),
    )


# ─── Catalog API helpers ────────────────────────────────────────────────────


async def _fetch_index(client: httpx.AsyncClient) -> CatalogIndex:
    data = await get_json(client, settings.nuget_catalog_index_url)
    return CatalogIndex.model_validate(data)


async def _fetch_page(client: httpx.AsyncClient, url: str) -> CatalogPage:
    data = await get_json(client, url)
    return CatalogPage.model_validate(data)


async def _fetch_leaf(
    client: httpx.AsyncClient, ref: CatalogLeafRef
) -> tuple[CatalogLeafRef, CatalogLeaf, dict] | None:
    try:
        raw = await get_json(client, ref.url)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            return None
        raise
    leaf = CatalogLeaf.model_validate(raw)
    return ref, leaf, raw


# ─── Batch processing ───────────────────────────────────────────────────────


_PACKAGE_INSERT_HEAD = """
INSERT INTO raw.nuget_packages
    (package_id, latest_version, project_url, license,
     raw_metadata, synced_at, source_updated_at)
VALUES
"""

_PACKAGE_INSERT_TAIL = """
ON CONFLICT (package_id) DO UPDATE SET
    latest_version = EXCLUDED.latest_version,
    project_url = EXCLUDED.project_url,
    license = EXCLUDED.license,
    raw_metadata = EXCLUDED.raw_metadata,
    synced_at = EXCLUDED.synced_at,
    source_updated_at = EXCLUDED.source_updated_at
"""

_VERSION_INSERT_HEAD = """
INSERT INTO raw.nuget_versions
    (package_id, version, purl, major, minor, patch, prerelease,
     published_at, listed, deprecated, deprecation_reasons,
     alternative_package, deprecation_message, raw_metadata, synced_at,
     deleted_at)
VALUES
"""

# A PackageDetails event means the version is live as of this catalog entry —
# any prior deleted_at (set by an earlier delete or by the backfill) must be
# cleared so the row reflects current state.
_VERSION_INSERT_TAIL = """
ON CONFLICT (package_id, version) DO UPDATE SET
    purl = EXCLUDED.purl,
    major = EXCLUDED.major,
    minor = EXCLUDED.minor,
    patch = EXCLUDED.patch,
    prerelease = EXCLUDED.prerelease,
    published_at = EXCLUDED.published_at,
    listed = EXCLUDED.listed,
    deprecated = EXCLUDED.deprecated,
    deprecation_reasons = EXCLUDED.deprecation_reasons,
    alternative_package = EXCLUDED.alternative_package,
    deprecation_message = EXCLUDED.deprecation_message,
    raw_metadata = EXCLUDED.raw_metadata,
    synced_at = EXCLUDED.synced_at,
    deleted_at = NULL
"""

_VERSION_DELETE_SQL = """
UPDATE raw.nuget_versions
SET deleted_at = %s
WHERE package_id = %s
  AND version = %s
  AND (deleted_at IS NULL OR deleted_at < %s::timestamptz)
"""

_PACKAGE_PLACEHOLDER = "(%s, %s, %s, %s, %s, %s, %s)"
_VERSION_PLACEHOLDER = "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)"


async def _upsert_batch(
    client: httpx.AsyncClient,
    refs: list[CatalogLeafRef],
    ctx: SyncContext,
    latest_watermark: list[str],
) -> None:
    # Track max commitTimeStamp across the *whole* batch — including delete
    # refs, which never go through the leaf fetch path below.
    for r in refs:
        if r.commit_time_stamp > latest_watermark[0]:
            latest_watermark[0] = r.commit_time_stamp

    # Partition by event type. PackageDelete refs carry everything we need
    # (package_id, version, commit_time_stamp) so we skip the leaf body
    # fetch entirely.
    detail_refs = [r for r in refs if r.type != PACKAGE_DELETE_TYPE]
    delete_refs = [r for r in refs if r.type == PACKAGE_DELETE_TYPE]

    fetched = await gather_bounded(
        detail_refs, lambda r: _fetch_leaf(client, r), concurrency=settings.nuget_concurrency
    )

    leaves: list[tuple[CatalogLeafRef, CatalogLeaf, dict]] = []
    for result in fetched:
        if isinstance(result, BaseException):
            log.warning("nuget.leaf_fetch_failed", error=str(result))
            continue
        if result is None:
            continue
        leaves.append(result)

    if not leaves and not delete_refs:
        return

    # Merge details + deletes into a single chronological stream so dedupe
    # respects the *last* event per (package_id, version) within this batch.
    # Handles delete-then-republish or republish-then-delete in one batch.
    # Multi-row INSERT VALUES with ON CONFLICT misbehaves on intra-statement
    # duplicates, so dedupe must happen client-side.
    events: list[tuple[str, CatalogLeafRef, CatalogLeaf | None, dict | None]] = [
        (ref.commit_time_stamp, ref, leaf, raw) for ref, leaf, raw in leaves
    ]
    events.extend((ref.commit_time_stamp, ref, None, None) for ref in delete_refs)
    events.sort(key=lambda e: e[0])

    now = datetime.now().astimezone()
    package_rows: dict[str, tuple] = {}
    version_rows: dict[tuple[str, str], tuple] = {}
    delete_rows: dict[tuple[str, str], str] = {}

    for _, ref, leaf, raw in events:
        key = (ref.package_id, ref.version)

        if ref.type == PACKAGE_DELETE_TYPE:
            delete_rows[key] = ref.commit_time_stamp
            # Do *not* drop version_rows[key]: if a PackageDetails appeared
            # earlier in this same batch we still want that data persisted
            # (so the row reflects "was published at T1, deleted at T2").
            # The UPDATE that sets deleted_at runs after the upsert, so
            # the final state correctly shows deleted_at = T_delete even
            # though the upsert wrote deleted_at = NULL.
            # Package row is *not* affected: a delete is version-scoped.
            continue

        # PackageDetails path
        assert leaf is not None and raw is not None
        source_updated_at = datetime.fromisoformat(leaf.published.replace("Z", "+00:00"))
        deprecation = leaf.deprecation
        alternative_package = (
            deprecation.alternate_package.get("id")
            if deprecation and deprecation.alternate_package
            else None
        )

        package_rows[leaf.id] = (
            leaf.id,
            leaf.version,
            leaf.project_url,
            leaf.license_expression,
            Jsonb(raw),
            now,
            source_updated_at,
        )

        major, minor, patch, prerelease = _parse_semver(leaf.version)
        purl = PackageURL(type="nuget", name=leaf.id, version=leaf.version).to_string()

        version_rows[key] = (
            leaf.id,
            leaf.version,
            purl,
            major,
            minor,
            patch,
            prerelease,
            source_updated_at,
            leaf.listed if leaf.listed is not None else True,
            deprecation is not None,
            deprecation.reasons if deprecation else None,
            alternative_package,
            deprecation.message if deprecation else None,
            Jsonb(raw),
            now,
        )
        # A re-publish (details after delete) supersedes the delete; the
        # version upsert SQL also resets deleted_at = NULL on conflict.
        delete_rows.pop(key, None)

    package_params = list(package_rows.values())
    version_params = list(version_rows.values())

    async with transaction() as conn, conn.cursor() as cur:
        # Bulk INSERT...ON CONFLICT. Values clause is built by repeating a
        # fixed placeholder tuple — no user-controlled SQL is interpolated.
        if package_params:
            package_values_sql = ",\n".join([_PACKAGE_PLACEHOLDER] * len(package_params))
            await cur.execute(
                _PACKAGE_INSERT_HEAD + package_values_sql + _PACKAGE_INSERT_TAIL,
                [p for row in package_params for p in row],
            )
            ctx.stats.updated += len(package_params)

        if version_params:
            version_values_sql = ",\n".join([_VERSION_PLACEHOLDER] * len(version_params))
            await cur.execute(
                _VERSION_INSERT_HEAD + version_values_sql + _VERSION_INSERT_TAIL,
                [p for row in version_params for p in row],
            )
            ctx.stats.inserted += len(version_params)

        if delete_rows:
            delete_params = [
                (ts, pid, ver, ts) for (pid, ver), ts in delete_rows.items()
            ]
            await cur.executemany(_VERSION_DELETE_SQL, delete_params)
            ctx.stats.deleted += len(delete_params)

        await advance_watermark(
            conn,
            SOURCE,
            latest_watermark[0],
            rows_synced=ctx.stats.inserted + ctx.stats.updated + ctx.stats.deleted,
            status="running",
        )
        # Heartbeat the audit row in the same transaction so durable progress
        # survives a crash mid-run.
        await heartbeat_run(
            conn,
            ctx.run_id,
            stats=ctx.stats,
            pages_processed=ctx.pages_processed,
            watermark_after=latest_watermark[0],
        )


# ─── Worker ─────────────────────────────────────────────────────────────────


async def _worker(ctx: SyncContext) -> SyncResult:
    latest_watermark = [ctx.watermark]
    max_pages = settings.nuget_max_pages
    started_monotonic = time.monotonic()

    ctx.log_progress(
        "nuget.sync.start",
        watermark=ctx.watermark,
        concurrency=settings.nuget_concurrency,
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
            "nuget.index_fetched",
            total_pages=len(index.items),
            filtered_pages=pages_total,
            catalog_commit_time_stamp=index.commit_time_stamp,
        )

        if not filtered:
            ctx.log_progress("nuget.caught_up", watermark=ctx.watermark)

        for page_ref in filtered:
            if ctx.is_shutting_down() or (
                max_pages is not None and ctx.pages_processed >= max_pages
            ):
                break

            page = await _fetch_page(client, page_ref.url)
            ctx.log_progress(
                "nuget.page_fetched",
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
                await _upsert_batch(client, batch, ctx, latest_watermark)
                ctx.log_progress(
                    "nuget.batch_processed",
                    batch_size=len(batch),
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
                versions_synced=ctx.stats.inserted,
                packages_synced=ctx.stats.updated,
                versions_deleted=ctx.stats.deleted,
                rows_per_sec=round(ctx.stats.inserted / elapsed, 2),
                http_requests=ctx.http_metrics.requests,
                http_429=ctx.http_metrics.http_429,
                http_5xx=ctx.http_metrics.http_5xx,
                http_4xx_other=ctx.http_metrics.http_4xx - ctx.http_metrics.http_429,
                transport_errors=ctx.http_metrics.transport_errors,
                retries_exhausted=ctx.http_metrics.retries_exhausted,
                concurrency=settings.nuget_concurrency,
            )

    # "partial" only if we stopped with pages still to process.
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
            "packages_synced": ctx.stats.updated,
            "versions_synced": ctx.stats.inserted,
            "versions_deleted": ctx.stats.deleted,
            "watermark": latest_watermark[0],
            "rows_per_sec": round(ctx.stats.inserted / elapsed_total, 2),
            "http_requests": ctx.http_metrics.requests,
            "http_429": ctx.http_metrics.http_429,
            "http_5xx": ctx.http_metrics.http_5xx,
            "http_4xx_other": ctx.http_metrics.http_4xx - ctx.http_metrics.http_429,
            "transport_errors": ctx.http_metrics.transport_errors,
            "retries_exhausted": ctx.http_metrics.retries_exhausted,
            "concurrency": settings.nuget_concurrency,
        },
    )


async def sync_nuget(
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


# Allow running standalone: `python -m nuget_pipeline.sync.nuget`
if __name__ == "__main__":
    import asyncio

    from nuget_pipeline.db.connection import close_pool
    from nuget_pipeline.utils.logging import configure_logging

    async def _main() -> None:
        configure_logging()
        try:
            result = await sync_nuget()
            print(json.dumps({"status": result.status, "watermark": result.watermark}))
        finally:
            await close_pool()

    asyncio.run(_main())
