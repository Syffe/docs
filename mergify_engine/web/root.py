import daiquiri
import fastapi
from fastapi.middleware import httpsredirect
from uvicorn.middleware import proxy_headers

from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.web import github_webhook
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

    app.mount("/badges", legacy_badges.create_app(debug=debug))
    app.mount("/v1", api_root.create_app(cors_enabled=True, debug=debug))
    app.mount("/front", front_root.create_app(debug=debug))
    app.mount("/subscriptions", subscriptions.create_app(debug=debug))

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
        await github.GitHubAppInfo.warm_cache(redis_links.cache)

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await redis.shutdown()

    return app
