import asyncio
from typing import Any

import dagster as dg
from pydantic import Field

from nuget_pipeline.config import settings
from nuget_pipeline.db.connection import close_pool
from nuget_pipeline.enrich.oss_status import classify_oss_status
from nuget_pipeline.sync.nuget import sync_nuget
from nuget_pipeline.sync.spdx import sync_spdx
from nuget_pipeline.utils.logging import configure_logging


def _render_metadata(metadata: dict[str, Any]) -> dict[str, dg.MetadataValue]:
    rendered: dict[str, dg.MetadataValue] = {}
    for k, v in metadata.items():
        if isinstance(v, bool):
            rendered[k] = dg.MetadataValue.bool(v)
        elif isinstance(v, int):
            rendered[k] = dg.MetadataValue.int(v)
        elif isinstance(v, float):
            rendered[k] = dg.MetadataValue.float(v)
        else:
            rendered[k] = dg.MetadataValue.text(str(v))
    return rendered


# Asset schemas. Emitted under the well-known `dagster/column_schema` metadata
# key so the UI renders the column list under each materialisation. Mirrors
# the DDL in src/nuget_pipeline/db/migrations/.
_RAW_NUGET_PACKAGES_SCHEMA = dg.TableSchema(
    columns=[
        dg.TableColumn(name="package_id", type="text", description="Primary key."),
        dg.TableColumn(name="latest_version", type="text"),
        dg.TableColumn(name="project_url", type="text"),
        dg.TableColumn(name="license", type="text", description="SPDX licenseExpression as declared by the publisher."),
        dg.TableColumn(name="raw_metadata", type="jsonb", description="Full catalog leaf body."),
        dg.TableColumn(name="synced_at", type="timestamptz"),
        dg.TableColumn(name="source_updated_at", type="timestamptz", description="leaf.published from NuGet."),
        dg.TableColumn(name="deleted_at", type="timestamptz", description="Reserved for package-level deletes; today only version-level deletes are emitted by NuGet."),
    ]
)

_RAW_SPDX_LICENSES_SCHEMA = dg.TableSchema(
    columns=[
        dg.TableColumn(name="license_id", type="text", description="Primary key. SPDX licenseId, e.g. 'MIT'."),
        dg.TableColumn(name="name", type="text"),
        dg.TableColumn(name="is_osi_approved", type="boolean"),
        dg.TableColumn(name="is_fsf_libre", type="boolean"),
        dg.TableColumn(name="is_deprecated_id", type="boolean"),
        dg.TableColumn(name="reference_url", type="text"),
        dg.TableColumn(name="see_also_urls", type="text[]", description="Source for the classifier's URL fallback index."),
        dg.TableColumn(name="raw_metadata", type="jsonb"),
        dg.TableColumn(name="synced_at", type="timestamptz"),
        dg.TableColumn(name="source_updated_at", type="timestamptz"),
    ]
)

_ENRICHED_OSS_STATUS_SCHEMA = dg.TableSchema(
    columns=[
        dg.TableColumn(name="package_id", type="text", description="Primary key. Foreign key to raw.nuget_packages."),
        dg.TableColumn(name="license_expression", type="text"),
        dg.TableColumn(name="license_url", type="text"),
        dg.TableColumn(name="spdx_id", type="text", description="Resolved SPDX id when the verdict was driven by a recognised license."),
        dg.TableColumn(name="spdx_normalized", type="text"),
        dg.TableColumn(name="is_osi_approved", type="boolean"),
        dg.TableColumn(name="classification", type="text", description="One of: open_source, proprietary, unknown. CHECK-constrained in DDL."),
        dg.TableColumn(name="reasoning", type="text", description="Human-readable explanation of the verdict; useful for the 'unknown' bucket."),
        dg.TableColumn(name="classified_at", type="timestamptz"),
        dg.TableColumn(name="source_synced_at", type="timestamptz", description="Mirrors raw.nuget_packages.synced_at for the row that produced this classification."),
    ]
)


class NugetSyncConfig(dg.Config):
    concurrency: int = Field(default=20, description="Parallel leaf fetches")
    batch_size: int = Field(default=100, description="Leaves per DB transaction")
    max_pages: int | None = Field(
        default=500,
        description="Cap catalog pages per run (None = unbounded). Safety valve for backfill.",
    )


@dg.asset(
    name="raw_nuget_packages",
    group_name="nuget",
    description="Incremental sync of the NuGet Catalog API into raw.nuget_packages / raw.nuget_versions.",
    compute_kind="python",
)
def raw_nuget_packages(context: dg.AssetExecutionContext, config: NugetSyncConfig) -> dg.MaterializeResult:
    configure_logging()

    settings.nuget_concurrency = config.concurrency
    settings.nuget_process_batch_size = config.batch_size
    settings.nuget_max_pages = config.max_pages

    def _emit_observation(metadata: dict[str, Any]) -> None:
        context.log_event(
            dg.AssetObservation(asset_key=context.asset_key, metadata=_render_metadata(metadata))
        )

    async def _run_and_cleanup():
        try:
            return await sync_nuget(
                dagster_run_id=context.run_id,
                dagster_log=context.log,
                observe=_emit_observation,
            )
        finally:
            # Close the pool inside the same event loop that opened it;
            # otherwise the global pool is left bound to a dead loop and
            # later cleanup calls (tests, retries) raise CancelledError.
            await close_pool()

    result = asyncio.run(_run_and_cleanup())

    if result.status == "failed":
        raise dg.Failure(description=f"NuGet sync reported failed status: {result.metadata}")

    meta = _render_metadata(result.metadata)
    meta["status"] = dg.MetadataValue.text(result.status)
    meta["watermark"] = dg.MetadataValue.text(result.watermark)
    meta["dagster/column_schema"] = _RAW_NUGET_PACKAGES_SCHEMA

    return dg.MaterializeResult(metadata=meta)


@dg.asset(
    name="raw_spdx_licenses",
    group_name="reference",
    description="Sync of the SPDX license list into raw.spdx_licenses. Reference data for the OSS classifier.",
    compute_kind="python",
)
def raw_spdx_licenses(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    configure_logging()

    def _emit_observation(metadata: dict[str, Any]) -> None:
        context.log_event(
            dg.AssetObservation(asset_key=context.asset_key, metadata=_render_metadata(metadata))
        )

    async def _run_and_cleanup():
        try:
            return await sync_spdx(
                dagster_run_id=context.run_id,
                dagster_log=context.log,
                observe=_emit_observation,
            )
        finally:
            await close_pool()

    result = asyncio.run(_run_and_cleanup())

    if result.status == "failed":
        raise dg.Failure(description=f"SPDX sync reported failed status: {result.metadata}")

    meta = _render_metadata(result.metadata)
    meta["status"] = dg.MetadataValue.text(result.status)
    meta["watermark"] = dg.MetadataValue.text(result.watermark)
    meta["dagster/column_schema"] = _RAW_SPDX_LICENSES_SCHEMA
    return dg.MaterializeResult(metadata=meta)


@dg.asset(
    name="enriched_nuget_package_oss_status",
    group_name="enrichment",
    description=(
        "Per-package OSS classification (open_source / proprietary / unknown) "
        "derived from raw.nuget_packages license fields and raw.spdx_licenses."
    ),
    deps=[raw_nuget_packages, raw_spdx_licenses],
    compute_kind="python",
    automation_condition=dg.AutomationCondition.eager(),
)
def enriched_nuget_package_oss_status(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    configure_logging()

    def _emit_observation(metadata: dict[str, Any]) -> None:
        context.log_event(
            dg.AssetObservation(asset_key=context.asset_key, metadata=_render_metadata(metadata))
        )

    async def _run_and_cleanup():
        try:
            return await classify_oss_status(
                dagster_run_id=context.run_id,
                dagster_log=context.log,
                observe=_emit_observation,
            )
        finally:
            await close_pool()

    result = asyncio.run(_run_and_cleanup())

    if result.status == "failed":
        raise dg.Failure(
            description=f"OSS classifier reported failed status: {result.metadata}"
        )

    meta = _render_metadata(result.metadata)
    meta["status"] = dg.MetadataValue.text(result.status)
    meta["watermark"] = dg.MetadataValue.text(result.watermark)
    meta["dagster/column_schema"] = _ENRICHED_OSS_STATUS_SCHEMA
    return dg.MaterializeResult(metadata=meta)


nuget_job = dg.define_asset_job(
    name="nuget_sync_job",
    selection=dg.AssetSelection.assets(raw_nuget_packages),
)

spdx_job = dg.define_asset_job(
    name="spdx_sync_job",
    selection=dg.AssetSelection.assets(raw_spdx_licenses),
)

oss_status_job = dg.define_asset_job(
    name="oss_status_job",
    selection=dg.AssetSelection.assets(enriched_nuget_package_oss_status),
)
