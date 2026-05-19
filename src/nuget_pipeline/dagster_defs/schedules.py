import dagster as dg

from nuget_pipeline.dagster_defs.assets import nuget_job, spdx_job

nuget_incremental_schedule = dg.ScheduleDefinition(
    name="nuget_incremental",
    cron_schedule="0 */6 * * *",  # Every 6 hours
    job=nuget_job,
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

# SPDX changes ~quarterly; weekly is plenty and the worker no-ops on
# unchanged version.
spdx_weekly_schedule = dg.ScheduleDefinition(
    name="spdx_weekly",
    cron_schedule="0 6 * * 1",  # Mondays 06:00 UTC
    job=spdx_job,
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)

# The OSS classifier no longer needs a schedule: `enriched_nuget_package_oss_status`
# carries an eager AutomationCondition, so Dagster materialises it as soon as
# either upstream raw asset is updated.
