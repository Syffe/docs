import fastapi
import httpx

from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.web.front import security
from mergify_engine.web.front import utils


router = fastapi.APIRouter()


@router.api_route(
    "/github/{path:path}",
    methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
)
async def github_proxy(
    request: fastapi.Request,
    path: str,
    current_user: security.CurrentUser,
) -> fastapi.responses.Response:
    headers = utils.headers_to_forward(request)

    async with github.AsyncGitHubInstallationClient(
        github.GitHubTokenAuth(current_user.oauth_access_token),
    ) as client:
        proxy_request: httpx.Request
        try:
            proxy_response = await client.request(
                method=request.method,
                url=f"{settings.GITHUB_REST_API_URL}/{path}",
                params=request.url.query,
                headers=headers,
                content=await request.body(),
                follow_redirects=True,
            )
            proxy_request = proxy_response.request
        except httpx.InvalidURL:
            raise fastapi.HTTPException(
                status_code=422,
                detail={"messages": "Invalid request"},
            )
        except httpx.HTTPStatusError as e:
            proxy_response = e.response
            proxy_request = e.request
        except httpx.RequestError as e:
            proxy_response = None
            proxy_request = e.request

        if proxy_response is None or proxy_response.status_code >= 500:
            proxy_response = httpx.Response(
                status_code=502,
                content="Bad Gateway",
                request=proxy_request,
                headers=dict[str, str](),
            )

        base_url = f"{request.url.scheme}://{request.url.hostname}"
        default_port = {"http": 80, "https": 443}[request.url.scheme]
        if request.url.port and request.url.port != default_port:
            base_url += f":{request.url.port}"

        await utils.override_or_raise_unexpected_content_type(
            proxy_request,
            proxy_response,
            [
                ("POST", "/markdown", "text/plain"),
                ("POST", "/markdown/raw", "text/plain"),
            ],
        )

        return fastapi.Response(
            status_code=proxy_response.status_code,
            content=proxy_response.content,
            headers=dict(
                utils.httpx_to_fastapi_headers(
                    proxy_response.headers,
                    rewrite_url=(
                        "https://api.github.com",
                        f"{base_url}/front/proxy/github",
                    ),
                ),
            ),
        )
