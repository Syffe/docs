import datetime
import typing

import fastapi
import msgpack

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine import subscription
from mergify_engine.clients import http_proxy
from mergify_engine.clients import shadow_office
from mergify_engine.web import redis
from mergify_engine.web.front import security


router = fastapi.APIRouter()

SUBSCRIPTION_DETAILS_EXPIRATION = datetime.timedelta(days=1)


class SubscriptionDetailsResponse(typing.TypedDict):
    status_code: int
    body: bytes
    headers: dict[str, str]
    updated: str


def get_subscription_detail_hkey(
    github_account_id: github_types.GitHubAccountIdType,
) -> str:
    return f"front/subscription-details/{github_account_id}"


async def get_subscription_details_cache(
    redis_cache: redis_utils.RedisCache,
    github_account_id: github_types.GitHubAccountIdType,
    user_id: github_types.GitHubAccountIdType,
) -> fastapi.Response | None:
    raw = await redis_cache.hget(
        get_subscription_detail_hkey(github_account_id),
        str(user_id),
    )
    if raw:
        deserialized = typing.cast(SubscriptionDetailsResponse, msgpack.unpackb(raw))
        # workaround for per field expiration in a hash
        updated = datetime.datetime.fromisoformat(deserialized["updated"])
        if date.utcnow() - updated < SUBSCRIPTION_DETAILS_EXPIRATION:
            return fastapi.Response(
                content=deserialized["body"],
                status_code=deserialized["status_code"],
                headers=deserialized["headers"],
            )
    return None


async def set_subscription_details_cache(
    redis_cache: redis_utils.RedisCache,
    github_account_id: github_types.GitHubAccountIdType,
    user_id: github_types.GitHubAccountIdType,
    response: fastapi.Response,
) -> None:
    serialized = msgpack.packb(
        SubscriptionDetailsResponse(
            {
                "status_code": response.status_code,
                "body": response.body,
                "headers": dict(response.headers),
                "updated": date.utcnow().isoformat(timespec="seconds"),
            },
        ),
    )
    pipeline = await redis_cache.pipeline()
    subscription_detail_hkey = get_subscription_detail_hkey(github_account_id)
    await pipeline.hset(subscription_detail_hkey, str(user_id), serialized)
    await pipeline.expire(subscription_detail_hkey, SUBSCRIPTION_DETAILS_EXPIRATION)
    await pipeline.execute()


async def clear_subscription_details_cache(
    redis_cache: redis_utils.RedisCache,
    github_account_id: github_types.GitHubAccountIdType,
    user_github_account_id: github_types.GitHubAccountIdType | None = None,
) -> None:
    if user_github_account_id is None:
        await redis_cache.delete(get_subscription_detail_hkey(github_account_id))
    else:
        await redis_cache.hdel(
            get_subscription_detail_hkey(github_account_id),
            str(user_github_account_id),
        )


async def saas_proxy(
    request: fastapi.Request,
    current_user: security.CurrentUser,
) -> fastapi.responses.Response:
    if not settings.SAAS_MODE:
        raise fastapi.HTTPException(
            510,
            "On-Premise installation must not use SaaS endpoints",
        )

    path = request.url.path.removeprefix("/front/proxy/saas/")

    # nosemgrep: python.django.security.injection.tainted-url-host.tainted-url-host
    async with shadow_office.AsyncShadowOfficeSaasClient() as client:
        return await http_proxy.proxy(
            client,
            request,
            url=f"/engine/saas/{path}",
            extra_headers={"Mergify-On-Behalf-Of": str(current_user.id)},
            follow_redirects=False,
            rewrite_url=(
                f"{settings.SUBSCRIPTION_URL}/engine/saas",
                "/front/proxy/saas",
            ),
        )


@router.get(
    "/saas/github-account/{github_account_id}/subscription-details",
)
async def saas_subscription(
    request: fastapi.Request,
    github_account_id: security.GitHubAccountId,
    current_user: security.CurrentUser,
    redis_links: redis.RedisLinks,
) -> fastapi.responses.Response:
    if settings.SAAS_MODE:
        response = await get_subscription_details_cache(
            redis_links.cache,
            github_account_id,
            current_user.id,
        )

        if response is None:
            response = await saas_proxy(request, current_user)
            await set_subscription_details_cache(
                redis_links.cache,
                github_account_id,
                current_user.id,
                response,
            )
        return response

    sub = await subscription.Subscription.get_subscription(
        redis_links.cache,
        github_account_id,
    )

    return fastapi.responses.JSONResponse(
        {
            "role": "member",
            "billable_seats": [],
            "billable_seats_count": 0,
            "billing_manager": False,
            "subscription": None,
            "plan": {
                "name": "OnPremise Premium",
                "features": sub.to_dict()["features"],
            },
        },
    )


@router.get("/saas/github-account/{github_account_id}/stripe-create")
@router.get("/saas/github-account/{github_account_id}/stripe-customer-portal")
async def saas_generic_with_github_account_id(
    request: fastapi.Request,
    github_account_id: security.GitHubAccountId,  # noqa: ARG001
    current_user: security.CurrentUser,
) -> fastapi.responses.Response:
    # NOTE(sileht): We need to validate the github_account_id before processing
    return await saas_proxy(request, current_user)


@router.post("/saas/plain/bug-report")
@router.get("/saas/intercom")
async def saas_generic_without_github_account_id(
    request: fastapi.Request,
    current_user: security.CurrentUser,
) -> fastapi.responses.Response:
    return await saas_proxy(request, current_user)
