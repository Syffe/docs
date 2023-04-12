import datetime
import typing

import fastapi
import httpx
import msgpack

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import dashboard
from mergify_engine.web import redis
from mergify_engine.web.front import security
from mergify_engine.web.front import utils


router = fastapi.APIRouter()

SUBSCRIPTION_DETAILS_EXPIRATION = datetime.timedelta(days=1)


class SubscriptionDetailsReponse(typing.TypedDict):
    status_code: int
    body: bytes
    headers: dict[str, str]


def get_subscription_details_redis_key(
    github_account_id: github_types.GitHubAccountIdType,
    user_id: github_types.GitHubAccountIdType | typing.Literal["*"],
) -> str:
    return f"front/subscription-details/{github_account_id}/{user_id}"


async def get_subscription_details_cache(
    redis_cache: redis_utils.RedisCache,
    github_account_id: github_types.GitHubAccountIdType,
    user_id: github_types.GitHubAccountIdType,
) -> fastapi.Response | None:
    cache_key = get_subscription_details_redis_key(github_account_id, user_id)
    raw = await redis_cache.get(cache_key)
    if raw:
        deserialized = typing.cast(SubscriptionDetailsReponse, msgpack.unpackb(raw))
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
        SubscriptionDetailsReponse(
            {
                "status_code": response.status_code,
                "body": response.body,
                "headers": dict(response.headers),
            }
        )
    )
    cache_key = get_subscription_details_redis_key(github_account_id, user_id)
    await redis_cache.set(cache_key, serialized, ex=SUBSCRIPTION_DETAILS_EXPIRATION)


async def clear_subscription_details_cache(
    redis_cache: redis_utils.RedisCache,
    github_account_id: github_types.GitHubAccountIdType,
) -> None:
    pipeline = await redis_cache.pipeline()
    async for key in redis_cache.scan_iter(
        get_subscription_details_redis_key(github_account_id, "*"), count=10000
    ):
        await pipeline.delete(key)
    await pipeline.execute()


@router.get(
    "/saas/github-account/{github_account_id}/subscription-details",  # noqa: FS003
)
async def saas_subscription(
    request: fastapi.Request,
    github_account_id: github_types.GitHubAccountIdType,
    current_user: security.CurrentUser,
    redis_links: redis.RedisLinks,
) -> fastapi.responses.Response:
    if settings.SAAS_MODE:
        response = await get_subscription_details_cache(
            redis_links.cache, github_account_id, current_user.id
        )
        if response is None:
            response = await saas_proxy(
                request,
                f"github-account/{github_account_id}/subscription-details",
                current_user,
            )
            await set_subscription_details_cache(
                redis_links.cache, github_account_id, current_user.id, response
            )
        return response

    return fastapi.responses.JSONResponse(
        {
            "role": "member",
            "billable_seats": [],
            "billable_seats_count": 0,
            "billing_manager": False,
            "subscription": None,
            "plan": {
                "name": "OnPremise Premium",
                "discontinued": False,
                "features": [
                    "private_repository",
                    "public_repository",
                    "priority_queues",
                    "custom_checks",
                    "random_request_reviews",
                    "merge_bot_account",
                    "queue_action",
                    "depends_on",
                    "show_sponsor",
                    "dedicated_worker",
                    "advanced_monitoring",
                    "queue_freeze",
                    "eventlogs_short",
                    "eventlogs_long",
                    "merge_queue_stats",
                ],
            },
        }
    )


@router.api_route(
    "/saas/{path:path}",  # noqa: FS003
    methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
)
async def saas_proxy(
    request: fastapi.Request,
    path: str,
    current_user: security.CurrentUser,
) -> fastapi.responses.Response:
    if not settings.SAAS_MODE:
        raise fastapi.HTTPException(
            510, "On-Premise installation must not use SaaS endpoints"
        )

    # nosemgrep: python.django.security.injection.tainted-url-host.tainted-url-host
    async with dashboard.AsyncDashboardSaasClient() as client:
        proxy_request: httpx.Request | None = None
        try:
            resp = await client.request(
                method=request.method,
                url=f"/engine/saas/{path}",
                headers={"Mergify-On-Behalf-Of": str(current_user.id)},
                params=request.url.query,
                content=await request.body(),
                follow_redirects=False,
            )
        except httpx.HTTPStatusError as e:
            resp = e.response
            proxy_request = e.request
        except httpx.RequestError as e:
            resp = None
            proxy_request = e.request

        if resp is None or resp.status_code >= 500:
            resp = httpx.Response(
                status_code=502,
                content="Bad Gateway",
                request=proxy_request,
                headers=dict[str, str](),
            )

        base_url = f"{request.url.scheme}://{request.url.hostname}"
        default_port = {"http": 80, "https": 443}[request.url.scheme]
        if request.url.port != default_port:
            base_url += f":{request.url.port}"

        return fastapi.Response(
            status_code=resp.status_code,
            content=resp.content,
            headers=dict(
                utils.httpx_to_fastapi_headers(
                    resp.headers,
                    rewrite_url=(
                        f"{settings.SUBSCRIPTION_URL}/engine/saas",
                        f"{base_url}/front/proxy/saas",
                    ),
                ),
            ),
        )
