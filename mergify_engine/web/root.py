import contextlib
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import fastapi
from fastapi.middleware import httpsredirect
from fastapi.middleware import trustedhost
import ratelimit
import ratelimit.backends.simple
import ratelimit.core
import ratelimit.types
import starlette.types
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


LOG = daiquiri.getLogger(__name__)


def on_ratelimit_hook(retry_after: int) -> ratelimit.types.ASGIApp:
    statsd.increment("engine.asgi.ratelimit")
    return ratelimit.core._on_blocked(retry_after)


async def get_ratelimit_identifier(
    scope: starlette.types.Scope,
) -> tuple[str, str]:
    if scope["client"]:
        return scope["client"][0], "default"

    return "127.0.0.1", "default"


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


def create_app(
    https_only: bool = True, debug: bool = False, rate_limiter: bool = False
) -> fastapi.FastAPI:
    app = fastapi.FastAPI(
        openapi_url=None,
        redoc_url=None,
        docs_url=None,
        debug=debug,
        lifespan=lifespan,
    )

    if rate_limiter:
        app.add_middleware(
            ratelimit.RateLimitMiddleware,
            authenticate=get_ratelimit_identifier,
            backend=ratelimit.backends.simple.MemoryBackend(),  # type: ignore[no-untyped-call]
            config={
                r"^/": [ratelimit.Rule(second=30, block_time=60, group="default")],
                r"^/front/proxy/sentry": [
                    ratelimit.Rule(second=5, block_time=60, group="default")
                ],
            },
            on_blocked=on_ratelimit_hook,
        )

    app.add_middleware(content_length.ContentLengthMiddleware)

    if https_only:
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
