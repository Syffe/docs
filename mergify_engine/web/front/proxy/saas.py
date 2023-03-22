import fastapi
import httpx

from mergify_engine import config
from mergify_engine.clients import dashboard
from mergify_engine.web.front import security
from mergify_engine.web.front import utils


router = fastapi.APIRouter()


@router.get(
    "/saas/github-account/{github_account_id}/subscription-details",  # noqa: FS003
)
async def saas_subscription(
    request: fastapi.Request,
    github_account_id: int,
    current_user: security.CurrentUser,
) -> fastapi.responses.Response:
    if config.SAAS_MODE:
        return await saas_proxy(
            request,
            f"github-account/{github_account_id}/subscription-details",
            current_user,
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
    if not config.SAAS_MODE:
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
                        f"{config.SUBSCRIPTION_BASE_URL}/engine/saas",
                        f"{base_url}/front/proxy/saas",
                    ),
                ),
            ),
        )
