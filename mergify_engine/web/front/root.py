import urllib.parse

import fastapi
import imia
import starsessions

from mergify_engine import config
from mergify_engine.web import utils
from mergify_engine.web.front import admin
from mergify_engine.web.front import auth
from mergify_engine.web.front import configuration
from mergify_engine.web.front import proxy
from mergify_engine.web.front import security
from mergify_engine.web.front import sessions
from mergify_engine.web.front.middlewares import logging


def create_app() -> fastapi.FastAPI:
    parsed_base_url = urllib.parse.urlparse(config.DASHBOARD_UI_FRONT_BASE_URL)

    cookie_https_only = parsed_base_url.scheme == "https"
    if parsed_base_url.hostname == "localhost":
        cookie_domain = None
    else:
        cookie_domain = parsed_base_url.hostname

    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None)

    user_provider = sessions.ImiaUserProvider()
    app.add_middleware(
        imia.ImpersonationMiddleware,
        user_provider=user_provider,
        guard_fn=security.is_mergify_admin,
    )
    app.add_middleware(
        imia.AuthenticationMiddleware,
        authenticators=[imia.SessionAuthenticator(user_provider=user_provider)],
    )
    app.add_middleware(starsessions.SessionAutoloadMiddleware)
    app.add_middleware(
        starsessions.SessionMiddleware,
        rolling=True,
        cookie_https_only=cookie_https_only,
        cookie_domain=cookie_domain,
        lifetime=3600 * config.DASHBOARD_UI_SESSION_EXPIRATION_HOURS,
        store=sessions.RedisStore(),
    )
    app.add_middleware(logging.LoggingMiddleware)
    utils.setup_exception_handlers(app)

    app.include_router(admin.router)
    app.include_router(configuration.router)
    app.include_router(auth.router, prefix="/auth")
    app.include_router(proxy.router, prefix="/proxy")
    return app
