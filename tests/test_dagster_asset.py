"""Materialize `raw_nuget_packages` through Dagster's in-process executor.

This is the highest-fidelity pre-deployment check: the asset function is
invoked exactly as Dagster will invoke it on the VM (config parsing,
metadata emission, Failure on sync failure, etc.).

NOTE: `dagster.materialize` is synchronous; it spins up its own event loop
internally for the asset's `asyncio.run(sync_nuget(...))` call. The test
functions below are therefore plain `def`, not `async def` — pytest-asyncio
would otherwise wrap them and clash with the nested event loop.
"""

from __future__ import annotations

import psycopg
import respx
from dagster import materialize

from nuget_pipeline.config import settings
from nuget_pipeline.dagster_defs.assets import raw_nuget_packages


INDEX_URL = "https://api.nuget.org/v3/catalog0/index.json"


def _fetch_sync(sql: str) -> list[tuple]:
    with psycopg.connect(settings.database_url) as conn, conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall()


def test_materialize_raw_nuget_packages_against_mocked_catalog() -> None:
    page_url = "https://api.nuget.org/v3/catalog0/page0.json"
    leaf_url = "https://api.nuget.org/v3/catalog0/data/leaf0.json"
    commit_ts = "2025-04-05T00:00:00Z"

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
                        "nuget:id": "Serilog",
                        "nuget:version": "3.1.0",
                    }
                ]
            }
        )
        router.get(leaf_url).respond(
            json={
                "id": "Serilog",
                "version": "3.1.0",
                "published": commit_ts,
                "listed": True,
                "licenseExpression": "Apache-2.0",
            }
        )

        result = materialize(
            [raw_nuget_packages],
            run_config={
                "ops": {
                    "raw_nuget_packages": {
                        "config": {"concurrency": 5, "batch_size": 10, "max_pages": 1}
                    }
                }
            },
        )

    assert result.success

    # The materialisation should have produced metadata we can inspect.
    mat_events = result.asset_materializations_for_node("raw_nuget_packages")
    assert len(mat_events) == 1
    meta = mat_events[0].metadata
    assert meta["status"].text == "completed"
    assert meta["watermark"].text == commit_ts

    # And the DB must actually have the row.
    assert _fetch_sync("SELECT package_id FROM raw.nuget_packages") == [("Serilog",)]


def test_materialize_surfaces_failure_as_dagster_failure() -> None:
    """If the catalog index 500s, Dagster should see a failed materialization."""
    settings.nuget_max_pages = 1

    with respx.mock(assert_all_called=False) as router:
        router.get(INDEX_URL).respond(status_code=500, text="boom")

        result = materialize(
            [raw_nuget_packages],
            run_config={
                "ops": {
                    "raw_nuget_packages": {"config": {"max_pages": 1}}
                }
            },
            raise_on_error=False,
        )

    assert not result.success
