import typing

import fastapi
import sqlalchemy
from starlette import requests
from starlette import responses

from mergify_engine import count_seats
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine import redis_utils
from mergify_engine.dashboard import application
from mergify_engine.dashboard import subscription
from mergify_engine.models import github_user
from mergify_engine.usage import last_seen
from mergify_engine.web import auth
from mergify_engine.web import redis
from mergify_engine.web import utils


router = fastapi.APIRouter()


@router.get(
    "/organization/{owner_id}/usage",  # noqa: FS003
    dependencies=[fastapi.Depends(auth.dashboard)],
)
async def get_stats(
    owner_id: github_types.GitHubAccountIdType,
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
) -> responses.Response:
    last_seen_at = await last_seen.get(redis_links.cache, owner_id)
    seats = await count_seats.Seats.get(redis_links.active_users, owner_id=owner_id)
    data = seats.jsonify()
    if data["organizations"]:
        if len(data["organizations"]) > 1:
            raise RuntimeError(
                "count_seats.Seats.get() returns more than one organization"
            )
        repos = data["organizations"][0]["repositories"]
    else:
        repos = []

    return responses.JSONResponse(
        {
            "repositories": repos,
            "last_seen_at": None if last_seen_at is None else last_seen_at.isoformat(),
        }
    )


@router.put(
    "/subscription-cache/{owner_id}",  # noqa: FS003
    dependencies=[fastapi.Depends(auth.dashboard)],
)
async def subscription_cache_update(
    owner_id: github_types.GitHubAccountIdType,
    request: requests.Request,
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
) -> responses.Response:
    sub = await request.json()
    if sub is None:
        return responses.Response("Empty content", status_code=400)
    try:
        await subscription.Subscription.update_subscription(
            redis_links.cache, owner_id, sub
        )
    except NotImplementedError:
        return responses.Response("Updating subscription is disabled", status_code=400)

    return responses.Response("Cache updated", status_code=200)


@router.delete(
    "/subscription-cache/{owner_id}",  # noqa: FS003
    dependencies=[fastapi.Depends(auth.dashboard)],
)
async def subscription_cache_delete(
    owner_id: github_types.GitHubAccountIdType,
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
) -> responses.Response:
    try:
        await subscription.Subscription.delete_subscription(redis_links.cache, owner_id)
    except NotImplementedError:
        return responses.Response("Deleting subscription is disabled", status_code=400)
    return responses.Response("Cache cleaned", status_code=200)


@router.delete(
    "/tokens-cache/{owner_id}",  # noqa: FS003
    dependencies=[fastapi.Depends(auth.dashboard)],
)
async def tokens_cache_delete(
    owner_id: github_types.GitHubAccountIdType,
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
) -> responses.Response:
    # FIXME(sileht): drop me once dashboard is cleaned
    return responses.Response("Cache cleaned", status_code=200)


@router.put(
    "/application/{api_access_key}",  # noqa: FS003
    dependencies=[fastapi.Depends(auth.dashboard)],
)
async def application_cache_update(
    api_access_key: str,
    request: requests.Request,
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
) -> responses.Response:
    data = typing.cast(
        application.ApplicationDashboardJSON | None, await request.json()
    )
    if data is None:
        return responses.Response("Empty content", status_code=400)

    try:
        await application.Application.update(redis_links.cache, api_access_key, data)
    except NotImplementedError:
        return responses.Response("Updating application is disabled", status_code=400)

    return responses.Response("Cache updated", status_code=200)


@router.delete(
    "/application/{api_access_key}",  # noqa: FS003
    dependencies=[fastapi.Depends(auth.dashboard)],
)
async def application_cache_delete(
    api_access_key: str,
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
) -> responses.Response:
    try:
        await application.Application.delete(redis_links.cache, api_access_key)
    except NotImplementedError:
        return responses.Response("Deleting subscription is disabled", status_code=400)
    return responses.Response("Cache cleaned", status_code=200)


@router.get(
    "/user-oauth-access-token/{github_account_id}",  # noqa: FS003
    dependencies=[fastapi.Depends(auth.dashboard)],
)
async def get_user_oauth_access_token(
    github_account_id: github_types.GitHubAccountIdType,
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
    session: sqlalchemy.ext.asyncio.AsyncSession = fastapi.Depends(  # noqa: B008
        models.get_session
    ),
) -> responses.Response:
    user = await github_user.GitHubUser.get_by_id(session, github_account_id)
    if not user:
        raise fastapi.HTTPException(404)
    return responses.JSONResponse({"oauth_access_token": user.oauth_access_token})


def create_app(debug: bool = False) -> fastapi.FastAPI:
    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None, debug=debug)
    app.include_router(router)
    utils.setup_exception_handlers(app)
    return app
