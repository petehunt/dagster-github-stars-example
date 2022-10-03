from dagster import StringSource, resource
from github import Github


@resource(config_schema={"access_token": StringSource})
def github_api(init_context):
    return Github(init_context.resource_config["access_token"])
