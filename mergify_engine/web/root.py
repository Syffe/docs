import daiquiri
import fastapi

from mergify_engine.web import github_webhook
from mergify_engine.web import legacy_badges
from mergify_engine.web import redis
from mergify_engine.web import refresher
from mergify_engine.web import subscriptions
from mergify_engine.web import utils
from mergify_engine.web.api import root as api_root
from mergify_engine.web.front import react
from mergify_engine.web.front import root as front_root


LOG = daiquiri.getLogger(__name__)


def create_app() -> fastapi.FastAPI:
    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None)
    app.include_router(subscriptions.router)

    # /event can't be moved without breaking on-premise installation
    app.include_router(github_webhook.router)

    app.include_router(subscriptions.router, prefix="/subscriptions")
    app.include_router(refresher.router, prefix="/refresh")
    app.include_router(legacy_badges.router, prefix="/badges")

    app.mount("/v1", api_root.create_app(cors_enabled=True))
    app.mount("/front", front_root.create_app())

    app.include_router(react.router)

    utils.setup_exception_handlers(app)

    @app.on_event("startup")
    async def startup() -> None:
        await redis.startup()

    @app.on_event("shutdown")
    async def shutdown() -> None:
        await redis.shutdown()

    return app
