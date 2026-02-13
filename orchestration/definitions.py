"""
Dagster orchestration for the Environment Agency dbt project.

Run locally with:
    dagster dev -f orchestration/definitions.py

Then open http://localhost:3000
"""

import subprocess
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    DefaultScheduleStatus,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

# Path to the dbt project (parent of this orchestration folder)
DBT_PROJECT_DIR = Path(__file__).parent.parent
DBT_PROFILES_DIR = Path.home() / ".dbt"

# Create a DbtProject to handle manifest generation
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
)

# Prepare the manifest (generates if missing or stale)
dbt_project.prepare_if_dev()


def _load_raw_data(context):
    """Run dbt run-operation load_raw_data via subprocess."""
    context.log.info("Loading raw data from Environment Agency API...")

    result = subprocess.run(
        ["dbt", "run-operation", "load_raw_data", "--profiles-dir", str(DBT_PROFILES_DIR)],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )

    context.log.info(result.stdout)
    if result.returncode != 0:
        context.log.error(result.stderr)
        raise Exception(f"dbt run-operation failed with exit code {result.returncode}")

    context.log.info("Raw data loaded successfully")


@dbt_assets(manifest=dbt_project.manifest_path)
def env_agency_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    All dbt models as Dagster assets.

    First loads raw data from the API, then runs dbt build.
    """
    # Step 1: Load raw data (runs sequentially before dbt build)
    _load_raw_data(context)

    # Step 2: Build all dbt models
    context.log.info("Running dbt build...")
    yield from dbt.cli(["build"], context=context).stream()


# Define a job that materializes all assets
refresh_all_job = define_asset_job(
    name="refresh_all",
    selection="*",  # all assets
)

# Schedule: run every 15 minutes
fifteen_minute_schedule = ScheduleDefinition(
    name="fifteen_minute_refresh",
    cron_schedule="*/15 * * * *",
    default_status=DefaultScheduleStatus.RUNNING,
    job=refresh_all_job,
)

# Dagster Definitions: assets, resources, schedules
defs = Definitions(
    assets=[env_agency_dbt_assets],
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
    },
    jobs=[refresh_all_job],
    schedules=[fifteen_minute_schedule],
)
