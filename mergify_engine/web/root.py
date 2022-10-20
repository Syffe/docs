import typing

import daiquiri
import fastapi
from starlette import responses

from mergify_engine.web import github_webhook
from mergify_engine.web import legacy_badges
from mergify_engine.web import redis
from mergify_engine.web import refresher
from mergify_engine.web import subscriptions
from mergify_engine.web import utils
from mergify_engine.web.api import root as api_root


LOG = daiquiri.getLogger(__name__)

app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None)

# For backward compatibility until we migrate the dashboard side
app.include_router(subscriptions.router)

# /event can't be moved without breaking on-premise installation
app.include_router(github_webhook.router)

app.include_router(subscriptions.router, prefix="/subscriptions")
app.include_router(refresher.router, prefix="/refresh")
app.include_router(legacy_badges.router, prefix="/badges")

app.mount("/v1", api_root.create_app())

utils.setup_exception_handlers(app)


@app.on_event("startup")
async def startup() -> None:
    await redis.startup()


@app.on_event("shutdown")
async def shutdown() -> None:
    await redis.shutdown()


@app.get("/")
async def index(
    setup_action: typing.Optional[str] = None,
) -> responses.Response:  # pragma: no cover
    if setup_action:
        return responses.Response(
            "Your Mergify installation succeeded.",
            status_code=200,
        )
    return responses.RedirectResponse(url="https://mergify.com")
