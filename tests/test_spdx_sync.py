"""SPDX sync against a respx-mocked SPDX endpoint + real Postgres."""

from __future__ import annotations

import psycopg
import respx

from nuget_pipeline.config import settings
from nuget_pipeline.sync.spdx import sync_spdx


SPDX_URL = settings.spdx_licenses_url


async def _fetch_all(sql: str, *params) -> list[tuple]:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()
    finally:
        await conn.close()


def _payload(version: str = "3.24.0") -> dict:
    return {
        "licenseListVersion": version,
        "releaseDate": "2024-12-01",
        "licenses": [
            {
                "licenseId": "MIT",
                "name": "MIT License",
                "isOsiApproved": True,
                "isFsfLibre": True,
                "isDeprecatedLicenseId": False,
                "reference": "https://spdx.org/licenses/MIT.html",
                "seeAlso": ["https://opensource.org/licenses/MIT"],
            },
            {
                "licenseId": "Apache-2.0",
                "name": "Apache License 2.0",
                "isOsiApproved": True,
                "isFsfLibre": True,
                "isDeprecatedLicenseId": False,
                "reference": "https://spdx.org/licenses/Apache-2.0.html",
                "seeAlso": [
                    "https://www.apache.org/licenses/LICENSE-2.0",
                    "https://opensource.org/licenses/Apache-2.0",
                ],
            },
            {
                "licenseId": "CC-BY-NC-4.0",
                "name": "Creative Commons Attribution Non Commercial 4.0",
                "isOsiApproved": False,
                # Some real SPDX entries omit isFsfLibre — pydantic default kicks in.
                "isDeprecatedLicenseId": False,
                "reference": "https://spdx.org/licenses/CC-BY-NC-4.0.html",
                "seeAlso": [],
            },
        ],
    }


async def test_spdx_sync_inserts_all_rows() -> None:
    with respx.mock(assert_all_called=False) as router:
        router.get(SPDX_URL).respond(json=_payload(version="3.24.0"))

        result = await sync_spdx()

    assert result.status == "completed"
    assert result.watermark == "3.24.0"
    assert result.metadata["license_count"] == 3
    assert result.metadata["rewritten"] is True

    rows = await _fetch_all(
        "SELECT license_id, is_osi_approved, see_also_urls "
        "FROM raw.spdx_licenses ORDER BY license_id"
    )
    assert rows == [
        ("Apache-2.0", True, [
            "https://www.apache.org/licenses/LICENSE-2.0",
            "https://opensource.org/licenses/Apache-2.0",
        ]),
        ("CC-BY-NC-4.0", False, []),
        ("MIT", True, ["https://opensource.org/licenses/MIT"]),
    ]


async def test_spdx_sync_noops_on_unchanged_version() -> None:
    """Second run with the same `licenseListVersion` must not rewrite."""
    with respx.mock(assert_all_called=False) as router:
        router.get(SPDX_URL).respond(json=_payload(version="3.24.0"))
        await sync_spdx()

    # Capture synced_at to detect rewrites.
    before = await _fetch_all(
        "SELECT license_id, synced_at FROM raw.spdx_licenses ORDER BY license_id"
    )

    with respx.mock(assert_all_called=False) as router:
        router.get(SPDX_URL).respond(json=_payload(version="3.24.0"))
        result = await sync_spdx()

    assert result.metadata["rewritten"] is False
    assert result.metadata["license_count"] == 3

    after = await _fetch_all(
        "SELECT license_id, synced_at FROM raw.spdx_licenses ORDER BY license_id"
    )
    # synced_at must be identical — proves the upsert was skipped.
    assert before == after


async def test_spdx_sync_rewrites_on_version_bump() -> None:
    with respx.mock(assert_all_called=False) as router:
        router.get(SPDX_URL).respond(json=_payload(version="3.24.0"))
        await sync_spdx()

    with respx.mock(assert_all_called=False) as router:
        router.get(SPDX_URL).respond(json=_payload(version="3.25.0"))
        result = await sync_spdx()

    assert result.watermark == "3.25.0"
    assert result.metadata["rewritten"] is True
