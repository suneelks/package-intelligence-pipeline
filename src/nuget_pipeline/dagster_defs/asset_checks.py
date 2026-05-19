"""Asset checks: quality gates that surface in the Dagster UI.

Three checks, one per asset:

- `raw_nuget_packages_nonempty` (ERROR): the raw NuGet table must have
  rows. A zero count means the upstream sync produced nothing and any
  downstream classification is meaningless.
- `raw_spdx_licenses_osi_floor` (ERROR): SPDX must report at least
  `SPDX_OSI_FLOOR` OSI-approved licenses. observability.md notes ~150 as
  the steady-state baseline; anything below 100 means the SPDX feed
  broke or shifted schema, and the classifier's URL fallback index will
  be wrong.
- `oss_status_unknown_ratio` (WARN): share of rows classified as
  `unknown` should stay <= `UNKNOWN_WARN_PCT`. v1 of the classifier
  intentionally leaves a chunk in `unknown` (closed by v2's
  license-file probing), so this is a regression alarm rather than a
  hard floor.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import TypeVar

import dagster as dg

from nuget_pipeline.dagster_defs.assets import (
    enriched_nuget_package_oss_status,
    raw_nuget_packages,
    raw_spdx_licenses,
)
from nuget_pipeline.db.connection import close_pool, connection

SPDX_OSI_FLOOR = 100
UNKNOWN_WARN_PCT = 60.0

# Freshness thresholds. Each is the maximum tolerated gap between
# successive materialisations before the check warns. Calibrated to the
# upstream cadences:
#   - NuGet catalog: 6h schedule + slack -> 12h
#   - SPDX:          weekly schedule + slack -> 10 days
#   - Enrichment:    auto-materialises after NuGet -> 24h
NUGET_MAX_AGE = timedelta(hours=12)
SPDX_MAX_AGE = timedelta(days=10)
ENRICHED_MAX_AGE = timedelta(hours=24)

T = TypeVar("T")


def _run(check: Callable[[], Awaitable[T]]) -> T:
    """Execute an async check body in a fresh event loop and close the
    connection pool inside the same loop. Mirrors the pattern used by
    the asset wrappers in `assets.py` — the global pool is bound to the
    loop that opened it, so closing must happen there."""

    async def _wrapper() -> T:
        try:
            return await check()
        finally:
            await close_pool()

    return asyncio.run(_wrapper())


@dg.asset_check(asset=raw_nuget_packages, name="raw_nuget_packages_nonempty", blocking=True)
def raw_nuget_packages_nonempty() -> dg.AssetCheckResult:
    async def _check() -> int:
        async with connection() as conn, conn.cursor() as cur:
            await cur.execute("SELECT count(*) AS n FROM raw.nuget_packages")
            row = await cur.fetchone()
        return int(row["n"]) if row else 0

    count = _run(_check)
    return dg.AssetCheckResult(
        passed=count > 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"row_count": count},
        description="raw.nuget_packages must have at least one row.",
    )


@dg.asset_check(asset=raw_spdx_licenses, name="raw_spdx_licenses_osi_floor", blocking=True)
def raw_spdx_licenses_osi_floor() -> dg.AssetCheckResult:
    async def _check() -> int:
        async with connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT count(*) AS n FROM raw.spdx_licenses WHERE is_osi_approved"
            )
            row = await cur.fetchone()
        return int(row["n"]) if row else 0

    osi_count = _run(_check)
    return dg.AssetCheckResult(
        passed=osi_count >= SPDX_OSI_FLOOR,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"osi_approved_count": osi_count, "floor": SPDX_OSI_FLOOR},
        description=(
            f"raw.spdx_licenses must report >= {SPDX_OSI_FLOOR} OSI-approved "
            "licenses; fewer means the SPDX feed broke or shifted schema."
        ),
    )


@dg.asset_check(
    asset=enriched_nuget_package_oss_status,
    name="oss_status_unknown_ratio",
)
def oss_status_unknown_ratio() -> dg.AssetCheckResult:
    async def _check() -> tuple[int, int]:
        async with connection() as conn, conn.cursor() as cur:
            await cur.execute(
                """
                SELECT
                    count(*) FILTER (WHERE classification = 'unknown') AS unknown,
                    count(*) AS total
                FROM enriched.nuget_package_oss_status
                """
            )
            row = await cur.fetchone()
        if not row:
            return 0, 0
        return int(row["unknown"]), int(row["total"])

    unknown, total = _run(_check)
    pct = (100.0 * unknown / total) if total else 0.0
    return dg.AssetCheckResult(
        passed=pct <= UNKNOWN_WARN_PCT,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "unknown_count": unknown,
            "total_count": total,
            "unknown_pct": round(pct, 2),
            "warn_threshold_pct": UNKNOWN_WARN_PCT,
        },
        description=(
            f"share of 'unknown' classifications should stay <= "
            f"{UNKNOWN_WARN_PCT}%; above that suggests a regression in the "
            "classifier or upstream SPDX."
        ),
    )


# ─── Freshness checks ───────────────────────────────────────────────────────
#
# `build_last_update_freshness_checks` emits one AssetCheck per asset that
# WARNs when the gap since the last successful materialisation exceeds
# `lower_bound_delta`. Evaluating them is the job of `freshness_check_sensor`
# below; the sensor is registered alongside the existing operational sensors
# and ships stopped by default.

nuget_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[raw_nuget_packages],
    lower_bound_delta=NUGET_MAX_AGE,
)

spdx_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[raw_spdx_licenses],
    lower_bound_delta=SPDX_MAX_AGE,
)

enriched_freshness_checks = dg.build_last_update_freshness_checks(
    assets=[enriched_nuget_package_oss_status],
    lower_bound_delta=ENRICHED_MAX_AGE,
)

all_freshness_checks = [
    *nuget_freshness_checks,
    *spdx_freshness_checks,
    *enriched_freshness_checks,
]

freshness_check_sensor = dg.build_sensor_for_freshness_checks(
    freshness_checks=all_freshness_checks,
    minimum_interval_seconds=600,
    default_status=dg.DefaultSensorStatus.STOPPED,
)
