"""
Dagster orchestration for the Environment Agency dbt project.

Run locally with:
    dagster dev -f orchestration/definitions.py

Then open http://localhost:3000
"""

import subprocess
import time
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DefaultScheduleStatus,
    Definitions,
    MaterializeResult,
    ScheduleDefinition,
    asset,
    define_asset_job,
    in_process_executor,
)
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

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


class SourceAssetTranslator(DagsterDbtTranslator):
    """Map dbt source asset keys to match our raw_* asset names.

    By default dagster-dbt gives sources a key like ["env_agency", "raw_readings"].
    Our raw ingestion assets are just ["raw_readings"], so we override source keys
    to drop the source_name prefix.
    """

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)


def make_raw_asset(name: str, operation: str, required: bool = True):
    @asset(name=name)
    def _asset(context: AssetExecutionContext) -> MaterializeResult:
        start = time.time()

        result = subprocess.run(
            [
                "dbt",
                "run-operation",
                operation,
                "--profiles-dir",
                str(DBT_PROFILES_DIR),
            ],
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
        )

        elapsed = time.time() - start
        context.log.info(result.stdout)

        if result.returncode != 0:
            if required:
                raise Exception(f"{operation} failed: {result.stderr}")
            else:
                context.log.warning(
                    f"{operation} failed after {elapsed:.1f}s, continuing with stale data"
                )

        return MaterializeResult(metadata={"fetch_duration_seconds": elapsed})

    return _asset


raw_readings = make_raw_asset("raw_readings", "load_raw_readings", required=True)
raw_stations = make_raw_asset("raw_stations", "load_raw_stations", required=False)
raw_measures = make_raw_asset("raw_measures", "load_raw_measures", required=False)


@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=SourceAssetTranslator(),
)
def env_agency_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# Define a job that materializes all assets.
# DuckDB is single-writer, so we use in_process_executor to run steps sequentially.
refresh_all_job = define_asset_job(
    name="refresh_all",
    selection="*",  # all assets
    executor_def=in_process_executor,
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
    assets=[raw_readings, raw_stations, raw_measures, env_agency_dbt_assets],
    resources={
        "dbt": DbtCliResource(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROFILES_DIR,
        ),
    },
    jobs=[refresh_all_job],
    schedules=[fifteen_minute_schedule],
)
