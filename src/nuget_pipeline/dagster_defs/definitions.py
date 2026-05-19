import dagster as dg

from nuget_pipeline.dagster_defs.asset_checks import (
    all_freshness_checks,
    freshness_check_sensor,
    oss_status_unknown_ratio,
    raw_nuget_packages_nonempty,
    raw_spdx_licenses_osi_floor,
)
from nuget_pipeline.dagster_defs.assets import (
    enriched_nuget_package_oss_status,
    nuget_job,
    oss_status_job,
    raw_nuget_packages,
    raw_spdx_licenses,
    spdx_job,
)
from nuget_pipeline.dagster_defs.schedules import (
    nuget_incremental_schedule,
    spdx_weekly_schedule,
)
from nuget_pipeline.dagster_defs.sensors import nuget_staleness_sensor, nuget_zombie_sensor

defs = dg.Definitions(
    assets=[raw_nuget_packages, raw_spdx_licenses, enriched_nuget_package_oss_status],
    asset_checks=[
        raw_nuget_packages_nonempty,
        raw_spdx_licenses_osi_floor,
        oss_status_unknown_ratio,
        *all_freshness_checks,
    ],
    jobs=[nuget_job, spdx_job, oss_status_job],
    schedules=[nuget_incremental_schedule, spdx_weekly_schedule],
    sensors=[nuget_staleness_sensor, nuget_zombie_sensor, freshness_check_sensor],
)
