from dagster import (
    load_assets_from_package_module,
    define_asset_job,
    ScheduleDefinition,
)
from my_dagster_project import assets
from my_dagster_project.dagster_quickstart import quickstart
from github import Github
import os

quickstart(
    assets=load_assets_from_package_module(assets),
    schedules=[
        ScheduleDefinition(
            job=define_asset_job(name="daily_refresh", selection="*"),
            cron_schedule="@daily",
        )
    ],
    resources={"github_api": Github(os.getenv("GITHUB_ACCESS_TOKEN"))},
)
