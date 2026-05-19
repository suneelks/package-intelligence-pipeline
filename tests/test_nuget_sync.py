"""End-to-end NuGet sync against a respx-mocked Catalog API + real Postgres.

Covers the index → page → leaf walk, upsert behaviour, watermark advance,
and deprecation metadata. This is the biggest deployment-confidence test:
if this passes, the VM-side Dagster run is exercising the same code paths
against the same schema.
"""

from __future__ import annotations

import httpx
import psycopg
import pytest
import respx

from nuget_pipeline.config import settings
from nuget_pipeline.sync.nuget import sync_nuget


INDEX_URL = "https://api.nuget.org/v3/catalog0/index.json"


async def _fetch_all(sql: str, *params) -> list[tuple]:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(sql, params)
            return await cur.fetchall()
    finally:
        await conn.close()


@pytest.fixture
def mock_nuget_one_package():
    page_url = "https://api.nuget.org/v3/catalog0/page0.json"
    leaf_url = "https://api.nuget.org/v3/catalog0/data/leaf0.json"
    commit_ts = "2025-04-01T00:00:00Z"

    with respx.mock(assert_all_called=False) as router:
        router.get(INDEX_URL).respond(
            json={
                "commitTimeStamp": commit_ts,
                "items": [{"@id": page_url, "commitTimeStamp": commit_ts, "count": 1}],
            }
        )
        router.get(page_url).respond(
            json={
                "items": [
                    {
                        "@id": leaf_url,
                        "@type": "nuget:PackageDetails",
                        "commitTimeStamp": commit_ts,
                        "nuget:id": "Newtonsoft.Json",
                        "nuget:version": "13.0.1",
                    }
                ]
            }
        )
        router.get(leaf_url).respond(
            json={
                "id": "Newtonsoft.Json",
                "version": "13.0.1",
                "published": commit_ts,
                "listed": True,
                "projectUrl": "https://www.newtonsoft.com/json",
                "licenseExpression": "MIT",
            }
        )
        yield router


async def test_sync_inserts_package_and_version(mock_nuget_one_package) -> None:
    settings.nuget_max_pages = 10

    result = await sync_nuget()

    assert result.status == "completed"
    assert result.watermark == "2025-04-01T00:00:00Z"

    pkgs = await _fetch_all(
        "SELECT package_id, latest_version, license, project_url FROM raw.nuget_packages"
    )
    assert pkgs == [("Newtonsoft.Json", "13.0.1", "MIT", "https://www.newtonsoft.com/json")]

    versions = await _fetch_all(
        "SELECT package_id, version, purl, major, minor, patch, listed, deprecated "
        "FROM raw.nuget_versions"
    )
    assert versions == [
        ("Newtonsoft.Json", "13.0.1", "pkg:nuget/Newtonsoft.Json@13.0.1", 13, 0, 1, True, False)
    ]


async def test_sync_is_idempotent_on_replay(mock_nuget_one_package) -> None:
    """Two back-to-back syncs over the same catalog data must leave exactly
    one package row and one version row — no duplicate-key errors, no
    duplicated rows."""
    settings.nuget_max_pages = 10

    await sync_nuget()
    await sync_nuget()

    pkg_count = await _fetch_all("SELECT COUNT(*) FROM raw.nuget_packages")
    ver_count = await _fetch_all("SELECT COUNT(*) FROM raw.nuget_versions")
    assert pkg_count == [(1,)]
    assert ver_count == [(1,)]


async def test_sync_captures_deprecation_metadata() -> None:
    """A leaf with deprecation fields lands in the version row with the
    deprecation columns populated."""
    settings.nuget_max_pages = 10
    page_url = "https://api.nuget.org/v3/catalog0/page0.json"
    leaf_url = "https://api.nuget.org/v3/catalog0/data/leaf0.json"
    commit_ts = "2025-04-02T00:00:00Z"

    with respx.mock(assert_all_called=False) as router:
        router.get(INDEX_URL).respond(
            json={
                "commitTimeStamp": commit_ts,
                "items": [{"@id": page_url, "commitTimeStamp": commit_ts, "count": 1}],
            }
        )
        router.get(page_url).respond(
            json={
                "items": [
                    {
                        "@id": leaf_url,
                        "@type": "nuget:PackageDetails",
                        "commitTimeStamp": commit_ts,
                        "nuget:id": "OldLib",
                        "nuget:version": "1.0.0",
                    }
                ]
            }
        )
        router.get(leaf_url).respond(
            json={
                "id": "OldLib",
                "version": "1.0.0",
                "published": commit_ts,
                "listed": True,
                "licenseExpression": "MIT",
                "deprecation": {
                    "reasons": ["Legacy", "CriticalBugs"],
                    "alternatePackage": {"id": "NewLib"},
                    "message": "Use NewLib instead.",
                },
            }
        )

        await sync_nuget()

    rows = await _fetch_all(
        "SELECT deprecated, deprecation_reasons, alternative_package, deprecation_message "
        "FROM raw.nuget_versions WHERE package_id = 'OldLib' AND version = '1.0.0'"
    )
    assert rows == [(True, ["Legacy", "CriticalBugs"], "NewLib", "Use NewLib instead.")]


async def test_sync_dedupes_within_batch_keeping_latest_commit() -> None:
    """If the same (package_id, version) appears twice in one page, the
    bulk-insert path must dedupe by primary key and keep the leaf with the
    highest commitTimeStamp — otherwise multi-row INSERT ... ON CONFLICT
    raises 'cannot affect row a second time'. Same package_id with
    different versions (mutating `latest_version`) tests the package-level
    dedupe."""
    settings.nuget_max_pages = 10
    page_url = "https://api.nuget.org/v3/catalog0/page0.json"
    leaf_a_url = "https://api.nuget.org/v3/catalog0/data/leaf-a.json"
    leaf_b_url = "https://api.nuget.org/v3/catalog0/data/leaf-b.json"
    leaf_c_url = "https://api.nuget.org/v3/catalog0/data/leaf-c.json"

    earlier = "2025-04-01T00:00:00Z"
    later = "2025-04-01T05:00:00Z"
    latest = "2025-04-01T10:00:00Z"

    with respx.mock(assert_all_called=False) as router:
        router.get(INDEX_URL).respond(
            json={
                "commitTimeStamp": latest,
                "items": [{"@id": page_url, "commitTimeStamp": latest, "count": 3}],
            }
        )
        router.get(page_url).respond(
            json={
                "items": [
                    # Same (id, version) twice: later one's metadata must win.
                    {
                        "@id": leaf_a_url,
                        "@type": "nuget:PackageDetails",
                        "commitTimeStamp": earlier,
                        "nuget:id": "Dup",
                        "nuget:version": "1.0.0",
                    },
                    {
                        "@id": leaf_b_url,
                        "@type": "nuget:PackageDetails",
                        "commitTimeStamp": later,
                        "nuget:id": "Dup",
                        "nuget:version": "1.0.0",
                    },
                    # Same package_id, different version — package row's
                    # `latest_version` should reflect the latest commit.
                    {
                        "@id": leaf_c_url,
                        "@type": "nuget:PackageDetails",
                        "commitTimeStamp": latest,
                        "nuget:id": "Dup",
                        "nuget:version": "1.0.1",
                    },
                ]
            }
        )
        router.get(leaf_a_url).respond(
            json={
                "id": "Dup",
                "version": "1.0.0",
                "published": earlier,
                "listed": True,
                "licenseExpression": "Apache-2.0",
            }
        )
        router.get(leaf_b_url).respond(
            json={
                "id": "Dup",
                "version": "1.0.0",
                "published": later,
                "listed": True,
                "licenseExpression": "MIT",  # Different from leaf_a — must win.
            }
        )
        router.get(leaf_c_url).respond(
            json={
                "id": "Dup",
                "version": "1.0.1",
                "published": latest,
                "listed": True,
                "licenseExpression": "MIT",
            }
        )

        result = await sync_nuget()

    assert result.status == "completed"

    # One package row (latest_version = "1.0.1" because that leaf's commit
    # time is the highest — package_rows dict's last assignment wins).
    pkgs = await _fetch_all(
        "SELECT package_id, latest_version, license FROM raw.nuget_packages"
    )
    assert pkgs == [("Dup", "1.0.1", "MIT")]

    # Two version rows: 1.0.0 (deduped) and 1.0.1.
    ver_rows = await _fetch_all(
        "SELECT version FROM raw.nuget_versions WHERE package_id = 'Dup' ORDER BY version"
    )
    assert ver_rows == [("1.0.0",), ("1.0.1",)]

    # The 1.0.0 row should reflect leaf_b's metadata (license MIT in raw_metadata),
    # because leaf_b had the higher commit_time_stamp.
    raw_meta = await _fetch_all(
        "SELECT raw_metadata->>'licenseExpression' FROM raw.nuget_versions "
        "WHERE package_id = 'Dup' AND version = '1.0.0'"
    )
    assert raw_meta == [("MIT",)]


async def test_sync_caught_up_is_noop_when_watermark_is_current() -> None:
    """If every page's commitTimeStamp is <= current watermark, the sync
    completes without touching any leaves."""
    settings.nuget_max_pages = 10

    # Seed the watermark beyond the catalog.
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO raw.sync_state (source, watermark, last_sync_status, rows_synced) "
                "VALUES ('nuget', '2099-01-01T00:00:00Z', 'completed', 0)"
            )
        await conn.commit()
    finally:
        await conn.close()

    with respx.mock(assert_all_called=False) as router:
        router.get(INDEX_URL).respond(
            json={
                "commitTimeStamp": "2025-04-01T00:00:00Z",
                "items": [
                    {
                        "@id": "https://api.nuget.org/v3/catalog0/old.json",
                        "commitTimeStamp": "2025-04-01T00:00:00Z",
                        "count": 0,
                    }
                ],
            }
        )
        result = await sync_nuget()

    assert result.status == "completed"
    # No rows written.
    pkgs = await _fetch_all("SELECT COUNT(*) FROM raw.nuget_packages")
    assert pkgs == [(0,)]


async def test_sync_marks_deleted_versions() -> None:
    """A `nuget:PackageDelete` leaf ref must set `deleted_at` on the
    matching version row without fetching a leaf body."""
    settings.nuget_max_pages = 10
    page_url = "https://api.nuget.org/v3/catalog0/page-delete.json"
    details_leaf_url = "https://api.nuget.org/v3/catalog0/data/leaf-details.json"
    upsert_ts = "2025-04-01T00:00:00Z"
    delete_ts = "2025-04-02T00:00:00Z"

    with respx.mock(assert_all_called=False) as router:
        router.get(INDEX_URL).respond(
            json={
                "commitTimeStamp": delete_ts,
                "items": [{"@id": page_url, "commitTimeStamp": delete_ts, "count": 2}],
            }
        )
        # Same page contains the original details event (to seed the row)
        # and a later delete event for the same (id, version).
        router.get(page_url).respond(
            json={
                "items": [
                    {
                        "@id": details_leaf_url,
                        "@type": "nuget:PackageDetails",
                        "commitTimeStamp": upsert_ts,
                        "nuget:id": "Mortal",
                        "nuget:version": "1.0.0",
                    },
                    {
                        "@id": "https://api.nuget.org/v3/catalog0/data/leaf-del.json",
                        "@type": "nuget:PackageDelete",
                        "commitTimeStamp": delete_ts,
                        "nuget:id": "Mortal",
                        "nuget:version": "1.0.0",
                    },
                ]
            }
        )
        router.get(details_leaf_url).respond(
            json={
                "id": "Mortal",
                "version": "1.0.0",
                "published": upsert_ts,
                "listed": True,
                "licenseExpression": "MIT",
            }
        )
        # Crucial: no route for the delete leaf body. If the worker tries
        # to fetch it, respx raises and the test fails.

        result = await sync_nuget()

    assert result.status == "completed"
    assert result.metadata["versions_deleted"] == 1

    rows = await _fetch_all(
        "SELECT deleted_at FROM raw.nuget_versions "
        "WHERE package_id = 'Mortal' AND version = '1.0.0'"
    )
    assert len(rows) == 1
    assert rows[0][0] is not None


async def test_sync_clears_deleted_at_on_republish() -> None:
    """A PackageDelete followed by a PackageDetails for the same
    (package_id, version) — even within one batch — must leave the version
    row live (deleted_at = NULL). Catalog order is the source of truth."""
    settings.nuget_max_pages = 10
    page_url = "https://api.nuget.org/v3/catalog0/page0.json"
    leaf_url = "https://api.nuget.org/v3/catalog0/data/leaf-republish.json"

    delete_ts = "2025-04-01T00:00:00Z"
    republish_ts = "2025-04-01T05:00:00Z"

    with respx.mock(assert_all_called=False) as router:
        router.get(INDEX_URL).respond(
            json={
                "commitTimeStamp": republish_ts,
                "items": [{"@id": page_url, "commitTimeStamp": republish_ts, "count": 2}],
            }
        )
        router.get(page_url).respond(
            json={
                "items": [
                    {
                        "@id": "https://api.nuget.org/v3/catalog0/data/leaf-del.json",
                        "@type": "nuget:PackageDelete",
                        "commitTimeStamp": delete_ts,
                        "nuget:id": "Phoenix",
                        "nuget:version": "1.0.0",
                    },
                    {
                        "@id": leaf_url,
                        "@type": "nuget:PackageDetails",
                        "commitTimeStamp": republish_ts,
                        "nuget:id": "Phoenix",
                        "nuget:version": "1.0.0",
                    },
                ]
            }
        )
        router.get(leaf_url).respond(
            json={
                "id": "Phoenix",
                "version": "1.0.0",
                "published": republish_ts,
                "listed": True,
                "licenseExpression": "MIT",
            }
        )

        result = await sync_nuget()

    assert result.status == "completed"

    rows = await _fetch_all(
        "SELECT deleted_at FROM raw.nuget_versions "
        "WHERE package_id = 'Phoenix' AND version = '1.0.0'"
    )
    assert rows == [(None,)]
