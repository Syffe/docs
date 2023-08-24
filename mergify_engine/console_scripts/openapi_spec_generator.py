import json
import os
import typing

import click
import fastapi
from fastapi.openapi.utils import get_openapi
import starlette

from mergify_engine.console_scripts.devtools_cli import devtools_cli
from mergify_engine.web import root as all_root
from mergify_engine.web.api import root as public_root


def patch_router_to_include_everything(
    route: starlette.routing.BaseRoute,
    parent_route: starlette.routing.BaseRoute | None,
) -> list[starlette.routing.BaseRoute]:
    if isinstance(route, fastapi.routing.APIRoute):
        route.include_in_schema = True
        if parent_route is not None:
            typed_route = typing.cast(starlette.routing.Route, route)
            typed_parent_route = typing.cast(starlette.routing.Route, parent_route)
            typed_route.path = f"{typed_parent_route.path}{typed_route.path}"
            (
                typed_route.path_regex,
                typed_route.path_format,
                typed_route.param_convertors,
            ) = starlette.routing.compile_path(typed_route.path)
            if not route.tags:
                raise RuntimeError(f"A route is not tagged: {route}")
        return [route]

    if isinstance(route, starlette.routing.Mount):
        subapp = typing.cast(fastapi.FastAPI, route.app)
        for subroute in subapp.routes:
            patch_router_to_include_everything(subroute, route)
        return subapp.routes

    return []


@devtools_cli.command()
@click.option("--visibility", type=click.Choice(["public", "internal"]), required=True)
@click.argument("output", required=True)
def generate_openapi_spec(
    visibility: typing.Literal["public", "all"], output: str
) -> None:
    if visibility == "public":
        app = public_root.create_app(cors_enabled=True)
        openapi_schema = app.openapi()

    else:
        app = all_root.create_app()
        routes = []
        for route in app.routes:
            routes.extend(patch_router_to_include_everything(route, None))
        openapi_schema = get_openapi(
            title="Internal API", routes=routes, version="0.0.0"
        )

    path = os.path.dirname(output)
    if path:
        os.makedirs(path, exist_ok=True)
    with open(output, "w") as f:
        json.dump(fp=f, obj=openapi_schema)
    click.echo(f"{output} created")
