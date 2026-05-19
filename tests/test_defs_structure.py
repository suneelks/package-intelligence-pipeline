"""Structural smoke tests for the Dagster Definitions object.

These catch import errors, missing wiring, or misnamed schedules/sensors
before the code hits a real Dagster instance. Fast: no DB, no HTTP.

We inspect asset / job / schedule / sensor objects directly rather than
going through Definitions accessor methods, which have churned across
Dagster minor versions.
"""

from nuget_pipeline.dagster_defs.assets import nuget_job, raw_nuget_packages
from nuget_pipeline.dagster_defs.definitions import defs
from nuget_pipeline.dagster_defs.schedules import nuget_incremental_schedule
from nuget_pipeline.dagster_defs.sensors import (
    nuget_staleness_sensor,
    nuget_zombie_sensor,
)


def test_defs_loads() -> None:
    assert defs is not None


def test_asset_registered() -> None:
    keys = [k.to_user_string() for k in raw_nuget_packages.keys]
    assert "raw_nuget_packages" in keys
    assert raw_nuget_packages in list(defs.assets or [])


def test_job_registered() -> None:
    assert nuget_job.name == "nuget_sync_job"
    assert nuget_job in list(defs.jobs or [])


def test_schedule_cron_valid() -> None:
    assert nuget_incremental_schedule.cron_schedule == "0 */6 * * *"
    assert nuget_incremental_schedule.name == "nuget_incremental"
    assert nuget_incremental_schedule in list(defs.schedules or [])


def test_sensors_registered() -> None:
    assert nuget_staleness_sensor.name == "nuget_staleness"
    assert nuget_zombie_sensor.name == "nuget_zombie_cleanup"
    sensors = list(defs.sensors or [])
    assert nuget_staleness_sensor in sensors
    assert nuget_zombie_sensor in sensors


def test_asset_config_shape() -> None:
    """NugetSyncConfig must expose the three knobs the user can override."""
    from nuget_pipeline.dagster_defs.assets import NugetSyncConfig

    fields = NugetSyncConfig.model_fields
    assert {"concurrency", "batch_size", "max_pages"} <= set(fields)
