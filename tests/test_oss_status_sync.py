"""OSS-status worker integration test against real Postgres.

Seeds raw.spdx_licenses + raw.nuget_packages directly, runs the worker,
asserts enriched.nuget_package_oss_status matches expectations.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

import psycopg

from nuget_pipeline.config import settings
from nuget_pipeline.enrich.oss_status import classify_oss_status


async def _exec(sql: str, *params) -> None:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
        await conn.commit()
    finally:
        await conn.close()


async def _fetch_all(sql: str, *params) -> list[tuple]:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()
    finally:
        await conn.close()


async def _seed_spdx() -> None:
    rows = [
        ("MIT", "MIT License", True, ["https://opensource.org/licenses/MIT"]),
        (
            "Apache-2.0",
            "Apache License 2.0",
            True,
            ["https://www.apache.org/licenses/LICENSE-2.0"],
        ),
        ("CC-BY-NC-4.0", "Creative Commons NC 4.0", False, []),
    ]
    for license_id, name, osi, see_also in rows:
        await _exec(
            "INSERT INTO raw.spdx_licenses (license_id, name, is_osi_approved, see_also_urls) "
            "VALUES (%s, %s, %s, %s)",
            license_id,
            name,
            osi,
            see_also,
        )


async def _seed_package(
    package_id: str,
    *,
    license_expression: str | None,
    license_url: str | None = None,
    synced_at: datetime,
) -> None:
    raw_metadata = {
        "id": package_id,
        "version": "1.0.0",
        "licenseExpression": license_expression,
        "licenseUrl": license_url,
    }
    await _exec(
        "INSERT INTO raw.nuget_packages (package_id, latest_version, license, raw_metadata, synced_at) "
        "VALUES (%s, %s, %s, %s::jsonb, %s)",
        package_id,
        "1.0.0",
        license_expression,
        json.dumps(raw_metadata),
        synced_at,
    )


async def test_classifier_runs_end_to_end() -> None:
    await _seed_spdx()

    base = datetime(2025, 4, 1, tzinfo=timezone.utc)
    await _seed_package("OssPkg", license_expression="MIT", synced_at=base)
    await _seed_package(
        "ProprietaryPkg",
        license_expression="LicenseRef-Internal",
        synced_at=base,
    )
    await _seed_package(
        "UrlOnlyPkg",
        license_expression=None,
        license_url="https://opensource.org/licenses/MIT",
        synced_at=base,
    )
    await _seed_package(
        "NoLicensePkg",
        license_expression=None,
        synced_at=base,
    )
    await _seed_package(
        "MicrosoftPkg",
        license_expression=None,
        license_url="https://go.microsoft.com/fwlink/?LinkId=329770",
        synced_at=base,
    )

    result = await classify_oss_status()

    assert result.status == "completed"
    assert result.metadata["rows_classified"] == 5
    assert result.metadata["open_source"] == 2  # OssPkg, UrlOnlyPkg
    assert result.metadata["proprietary"] == 2  # ProprietaryPkg, MicrosoftPkg
    assert result.metadata["unknown"] == 1  # NoLicensePkg

    rows = await _fetch_all(
        "SELECT package_id, classification, spdx_id, is_osi_approved "
        "FROM enriched.nuget_package_oss_status "
        "ORDER BY package_id"
    )
    assert rows == [
        ("MicrosoftPkg", "proprietary", None, False),
        ("NoLicensePkg", "unknown", None, None),
        ("OssPkg", "open_source", "MIT", True),
        ("ProprietaryPkg", "proprietary", "LicenseRef-Internal", False),
        ("UrlOnlyPkg", "open_source", "MIT", True),
    ]


async def test_classifier_is_idempotent() -> None:
    await _seed_spdx()
    base = datetime(2025, 4, 1, tzinfo=timezone.utc)
    await _seed_package("Pkg1", license_expression="MIT", synced_at=base)

    await classify_oss_status()
    await classify_oss_status()

    rows = await _fetch_all(
        "SELECT classification FROM enriched.nuget_package_oss_status"
    )
    assert rows == [("open_source",)]


async def test_classifier_picks_up_new_packages_via_watermark() -> None:
    """First run classifies the seeded package. A new raw row with a later
    `synced_at` is picked up on the next run via the watermark."""
    await _seed_spdx()
    t1 = datetime(2025, 4, 1, tzinfo=timezone.utc)
    t2 = datetime(2025, 4, 2, tzinfo=timezone.utc)

    await _seed_package("First", license_expression="MIT", synced_at=t1)
    first = await classify_oss_status()
    assert first.metadata["rows_classified"] == 1

    await _seed_package("Second", license_expression="Apache-2.0", synced_at=t2)
    second = await classify_oss_status()
    assert second.metadata["rows_classified"] == 1
    assert second.metadata["open_source"] == 1

    rows = await _fetch_all(
        "SELECT package_id, classification FROM enriched.nuget_package_oss_status "
        "ORDER BY package_id"
    )
    assert rows == [
        ("First", "open_source"),
        ("Second", "open_source"),
    ]


async def test_classifier_fails_loudly_when_spdx_empty() -> None:
    """Running the classifier without seeding SPDX must fail rather than
    silently classifying everything as unknown."""
    base = datetime(2025, 4, 1, tzinfo=timezone.utc)
    await _seed_package("Pkg1", license_expression="MIT", synced_at=base)

    import pytest

    with pytest.raises(RuntimeError, match="raw.spdx_licenses is empty"):
        await classify_oss_status()
