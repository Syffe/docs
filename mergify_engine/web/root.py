import argparse
import json
import os
import typing

import daiquiri
import fastapi
from fastapi.middleware import httpsredirect
from fastapi.openapi.utils import get_openapi
import starlette.routing
from uvicorn.middleware import proxy_headers

from mergify_engine import settings
from mergify_engine import signals
from mergify_engine.clients import github
from mergify_engine.middlewares import security
from mergify_engine.web import github_webhook
from mergify_engine.web import healthcheck
from mergify_engine.web import legacy_badges
from mergify_engine.web import redis
from mergify_engine.web import subscriptions
from mergify_engine.web import utils
from mergify_engine.web.api import root as api_root
from mergify_engine.web.front import react
from mergify_engine.web.front import root as front_root


LOG = daiquiri.getLogger(__name__)


def saas_root_endpoint() -> fastapi.Response:
    return fastapi.responses.JSONResponse({})


def create_app(https_only: bool = True, debug: bool = False) -> fastapi.FastAPI:
    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None, debug=debug)
    if https_only:
        app.add_middleware(httpsredirect.HTTPSRedirectMiddleware)
    app.add_middleware(
        proxy_headers.ProxyHeadersMiddleware,
        trusted_hosts="*",
    )
    app.add_middleware(security.SecurityMiddleware)

    app.mount("/badges", legacy_badges.create_app(debug=debug))
    app.mount("/v1", api_root.create_app(cors_enabled=True, debug=debug))
    app.mount("/front", front_root.create_app(debug=debug))
    app.mount("/subscriptions", subscriptions.create_app(debug=debug))
    app.include_router(healthcheck.router)

    if settings.DASHBOARD_UI_STATIC_FILES_DIRECTORY is None:
        app.get("/")(saas_root_endpoint)
        app.mount("/", github_webhook.create_app(debug=debug))
    else:
        app.include_router(github_webhook.router)
        app.include_router(react.router)

    utils.setup_exception_handlers(app)

    @app.on_event("startup")
    async def startup() -> None:
        await redis.startup()
        # NOTE(sileht): Warm GitHubAppInfo cache
        redis_links = redis.get_redis_links()
        signals.register()
        await github.GitHubAppInfo.warm_cache(redis_links.cache)

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await redis.shutdown()
        signals.unregister()

    return app


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


def generate_openapi_spec() -> None:
    parser = argparse.ArgumentParser(description="Generate OpenAPI spec file")
    parser.add_argument("output", help="output dir")
    args = parser.parse_args()

    path = os.path.dirname(args.output)
    if path:
        os.makedirs(path, exist_ok=True)

    routes = []
    app = create_app()
    for route in app.routes:
        routes.extend(patch_router_to_include_everything(route, None))

    openapi_schema = get_openapi(
        title="Internal API",
        routes=routes,
        version="0.0.0",
    )
    with open(args.output, "w") as f:
        json.dump(fp=f, obj=openapi_schema)
