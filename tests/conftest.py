"""Shared test fixtures.

Every test runs against the real Postgres from the devcontainer (so we
catch migration drift). Tables are truncated between tests.
HTTP is mocked with respx so tests never hit the live NuGet catalog.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterator
from typing import Any

import psycopg
import pytest
import respx

from nuget_pipeline.config import settings
from nuget_pipeline.db.connection import close_pool
from nuget_pipeline.db.migrate import run as run_migrations

TRUNCATE_SQL = """
TRUNCATE raw.nuget_versions, raw.nuget_packages,
         raw.spdx_licenses,
         enriched.nuget_package_oss_status,
         raw.sync_runs, raw.sync_state
RESTART IDENTITY CASCADE
"""


@pytest.fixture(scope="session", autouse=True)
def _ensure_schema() -> None:
    """Apply migrations once per test session (idempotent)."""
    asyncio.run(run_migrations())


@pytest.fixture(autouse=True)
async def _clean_tables() -> None:
    conn = await psycopg.AsyncConnection.connect(settings.database_url)
    try:
        async with conn.cursor() as cur:
            await cur.execute(TRUNCATE_SQL)
        await conn.commit()
    finally:
        await conn.close()
    yield
    # Pool may have been opened by code under test. Close it so the next
    # test gets a fresh pool bound to its own event loop.
    await close_pool()


@pytest.fixture
def nuget_mock() -> Iterator[respx.MockRouter]:
    """Return a respx router scoped to just nuget endpoints, pre-wired
    with one page containing one leaf for a single package."""
    with respx.mock(base_url="https://api.nuget.org", assert_all_called=False) as router:
        yield router


def make_catalog(page_url: str, leaf_url: str, commit_ts: str = "2025-04-01T00:00:00Z") -> dict[str, Any]:
    return {
        "commitTimeStamp": commit_ts,
        "items": [
            {"@id": page_url, "commitTimeStamp": commit_ts, "count": 1},
        ],
    }


def make_page(
    leaf_url: str,
    package_id: str,
    version: str,
    commit_ts: str,
    *,
    leaf_type: str = "nuget:PackageDetails",
) -> dict[str, Any]:
    return {
        "items": [
            {
                "@id": leaf_url,
                "@type": leaf_type,
                "commitTimeStamp": commit_ts,
                "nuget:id": package_id,
                "nuget:version": version,
            }
        ],
    }


def make_leaf(
    package_id: str,
    version: str,
    *,
    published: str = "2025-04-01T00:00:00Z",
    project_url: str | None = None,
    license_expression: str | None = "MIT",
    listed: bool = True,
    deprecation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": package_id,
        "version": version,
        "published": published,
        "listed": listed,
        "projectUrl": project_url,
        "licenseExpression": license_expression,
    }
    if deprecation is not None:
        body["deprecation"] = deprecation
    return body
