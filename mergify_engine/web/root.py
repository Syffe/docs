import contextlib
import typing

import fastapi
from fastapi.middleware import httpsredirect
from fastapi.middleware import trustedhost
from uvicorn.middleware import proxy_headers

from mergify_engine import settings
from mergify_engine import signals
from mergify_engine.clients import github
from mergify_engine.middlewares import content_length
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


def saas_root_endpoint() -> fastapi.Response:
    return fastapi.responses.JSONResponse({})


@contextlib.asynccontextmanager
async def lifespan(app: fastapi.FastAPI) -> typing.AsyncGenerator[None, None]:
    await redis.startup()
    # NOTE(sileht): Warm GitHubAppInfo cache
    redis_links = redis.get_redis_links()
    signals.register()
    await github.GitHubAppInfo.warm_cache(redis_links.cache)

    yield

    await redis.shutdown()
    signals.unregister()


def create_app(debug: bool = False) -> fastapi.FastAPI:
    app = fastapi.FastAPI(
        openapi_url=None,
        redoc_url=None,
        docs_url=None,
        debug=debug,
        lifespan=lifespan,
    )

    app.add_middleware(content_length.ContentLengthMiddleware)

    if settings.HTTP_TO_HTTPS_REDIRECT:
        app.add_middleware(httpsredirect.HTTPSRedirectMiddleware)

    app.add_middleware(
        trustedhost.TrustedHostMiddleware,
        allowed_hosts=settings.HTTP_TRUSTED_HOSTS,
        www_redirect=False,
    )
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
        app.get("/")(saas_root_endpoint)  # type: ignore[unreachable]
        app.mount("/", github_webhook.create_app(debug=debug))
    else:
        app.include_router(github_webhook.router)
        app.include_router(react.router)

    utils.setup_exception_handlers(app)

    return app
