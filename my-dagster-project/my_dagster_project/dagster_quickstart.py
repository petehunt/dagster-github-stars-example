from typing import List, Union, Any, Mapping
from dagster import (
    AssetsDefinition,
    SourceAsset,
    ScheduleDefinition,
    SensorDefinition,
    ResourceDefinition,
    repository,
    with_resources,
)
import inspect

# This is a bandaid library to paper over core API usability issues until we sort
# them out. The idea is that this will be a library separate from Dagster but
# referenced in our tutorials. It will be used for simple examples where 80%
# solutions suffice. If you need more flexibility beyond this, you can
# learn more about the internals of Dagster.

# This is inspired by cherrypy.quickstart():
# https://cherrypydocrework.readthedocs.io/basics.html#hosting-one-or-more-applications

# There are three goals of this package:
# 1. Hide the word "repository" from code snippets. It's still present in the UI
#    but will be less front-and-center when someone is learning Dagster
# 2. Hide the complexities of the resources system. You can just provide plain
#    old values to the `resources` kwarg. This lets you write your business logic
#    with resources in mind without having to learn how the whole resource and
#    config system works to wire them up. This is better than hardcoding
#    the resources directly in your business logic, since you won't have to
#    rewrite all your business logic to support resources later, just the code
#    that injects resources here.
# 3. Make secrets easy and obvious. Users of this library will be discouraged
#    from using Dagster config. Instead, we will instruct users to instantiate
#    resources directly and read secrets using os.getenv(). When
#    https://github.com/dagster-io/dagster/pull/10491 lands users will be able
#    to add secrets to a `.env.local` file during development, and Cloud users
#    will get a nice UI to add secrets for prod (just like Vercel)


_quickstart_called = False


def quickstart(
    assets: List[Union[AssetsDefinition, SourceAsset]] = [],
    schedules: List[ScheduleDefinition] = [],
    sensors: List[SensorDefinition] = [],
    resources: Mapping[str, Any] = {},
    resource_config: Mapping[str, Any] = {},
):
    global _quickstart_called
    if _quickstart_called:
        raise RuntimeError("you may only call quickstart() once in your app")
    _quickstart_called = True

    @repository
    def quickstart_repo():
        resource_defs = {}
        for resource_key, resource_def in resources.items():
            if not isinstance(resource_def, ResourceDefinition):
                resource_def = ResourceDefinition.hardcoded_resource(resource_def)
            if resource_key in resource_config:
                resource_def = resource_def.configured(
                    bundle.resource_config[resource_key]
                )
            resource_defs[resource_key] = resource_def

        if len(resource_defs) > 0:
            asset_defs = with_resources(assets, resource_defs)

        return [
            *asset_defs,
            *sensors,
            *schedules,
        ]

    frame = inspect.stack()[1].frame
    if not frame.f_code.co_filename.endswith("__init__.py"):
        raise RuntimeError("quickstart() must be called from __init__.py")
        
    f_locals = frame.f_locals

    if "quickstart_repo" in f_locals:
        raise RuntimeError(
            "you cannot define a symbol called quickstart_repo in your module"
        )
    f_locals["quickstart_repo"] = quickstart_repo

    return quickstart_repo
