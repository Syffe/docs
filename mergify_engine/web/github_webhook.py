import typing

import daiquiri
import fastapi
import httpx
from starlette import requests
from starlette import responses

from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import http
from mergify_engine.web import auth
from mergify_engine.web import redis
from mergify_engine.web import utils


LOG = daiquiri.getLogger(__name__)

# Set the maximum timeout to 5 seconds: GitHub is not going to wait for
# more than 10 seconds for us to accept an event, so if we're unable to
# forward an event in 5 seconds, just drop it.
EVENT_FORWARD_TIMEOUT = 5


router = fastapi.APIRouter(
    dependencies=[fastapi.Depends(auth.github_webhook_signature)],
)


@router.post("/event")
async def event_handler(
    request: requests.Request,
    background_tasks: fastapi.BackgroundTasks,
    redis_links: redis.RedisLinks,
) -> responses.Response:
    event_type = typing.cast(
        github_types.GitHubEventType, request.headers["X-GitHub-Event"]
    )
    event_id = request.headers["X-GitHub-Delivery"]
    data = await request.json()

    # FIXME: complete payload with fake data. A ticket has been created at
    # GitHub support because of an incomplete payload on
    # pull_request_review_thread event sometimes. We could erase this block once
    # the issue is resolved. MRGFY-1324
    if "sender" not in data:
        data["sender"] = {
            "id": -1,
            "login": "unknown",
            "type": "unknown",
        }

    try:
        await github_events.filter_and_dispatch(
            background_tasks, redis_links, event_type, event_id, data
        )
    except github_events.IgnoredEvent as ie:
        status_code = 200
        reason = f"Event ignored: {ie.reason}"
    else:
        status_code = 202
        reason = "Event queued"

    if (
        settings.GITHUB_WEBHOOK_FORWARD_URL
        and settings.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES is not None
        and event_type in settings.GITHUB_WEBHOOK_FORWARD_EVENT_TYPES
    ):
        raw = await request.body()
        try:
            async with http.AsyncClient(timeout=EVENT_FORWARD_TIMEOUT) as client:
                await client.post(
                    settings.GITHUB_WEBHOOK_FORWARD_URL,
                    content=raw.decode(),
                    headers={
                        "X-GitHub-Event": event_type,
                        "X-GitHub-Delivery": event_id,
                        "X-Hub-Signature": request.headers["X-Hub-Signature"],
                        "User-Agent": request.headers["User-Agent"],
                        "Content-Type": request.headers["Content-Type"],
                    },
                )
        except httpx.HTTPError:
            LOG.warning(
                "Fail to forward GitHub event",
                event_type=event_type,
                event_id=event_id,
                sender=data["sender"]["login"],
            )

    return responses.Response(reason, status_code=status_code)


def create_app(debug: bool = False) -> fastapi.FastAPI:
    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None, debug=debug)
    app.include_router(router)
    utils.setup_exception_handlers(app)
    return app
