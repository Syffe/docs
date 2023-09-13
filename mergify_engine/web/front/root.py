import fastapi
import imia
import starsessions

from mergify_engine import settings
from mergify_engine.middlewares import logging
from mergify_engine.middlewares import sudo
from mergify_engine.web import utils
from mergify_engine.web.front import admin
from mergify_engine.web.front import applications
from mergify_engine.web.front import auth
from mergify_engine.web.front import configuration
from mergify_engine.web.front import proxy
from mergify_engine.web.front import security
from mergify_engine.web.front import sessions


def create_app(debug: bool = False) -> fastapi.FastAPI:
    cookie_https_only = settings.DASHBOARD_UI_FRONT_URL.scheme == "https"
    if settings.DASHBOARD_UI_FRONT_URL.host == "localhost":
        cookie_domain = None
    else:
        cookie_domain = settings.DASHBOARD_UI_FRONT_URL.host

    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None, debug=debug)

    user_provider = sessions.ImiaUserProvider()
    app.add_middleware(sudo.SudoMiddleware)
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
        cookie_name="mergify-session",
        cookie_https_only=cookie_https_only,
        cookie_domain=cookie_domain,
        lifetime=3600 * settings.DASHBOARD_UI_SESSION_EXPIRATION_HOURS,
        store=sessions.RedisStore(),
    )
    app.add_middleware(logging.LoggingMiddleware)
    utils.setup_exception_handlers(app)

    app.include_router(admin.router)
    app.include_router(applications.router)
    app.include_router(configuration.router)
    app.include_router(auth.router, prefix="/auth")
    app.include_router(proxy.router, prefix="/proxy")
    return app
