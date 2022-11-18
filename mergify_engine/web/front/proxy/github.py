import fastapi
import httpx

from mergify_engine import config
from mergify_engine.clients import github
from mergify_engine.models import github_user
from mergify_engine.web.front import security
from mergify_engine.web.front import utils


router = fastapi.APIRouter()


@router.api_route(
    "/github/{path:path}",  # noqa: FS003
    methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
)
async def github_proxy(
    request: fastapi.Request,
    path: str,
    current_user: github_user.GitHubUser = fastapi.Depends(  # noqa: B008
        security.get_current_user
    ),
) -> fastapi.responses.Response:
    headers = {
        k: v for k, v in request.headers.items() if k.lower().startswith("accept")
    }

    async with github.AsyncGithubInstallationClient(
        github.GithubTokenAuth(current_user.oauth_access_token)
    ) as client:
        proxy_request: httpx.Request | None = None
        try:
            resp = await client.request(
                method=request.method,
                url=f"{config.GITHUB_REST_API_URL}/{path}",
                params=request.url.query,
                headers=headers,
                content=await request.body(),
                follow_redirects=True,
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
        if request.url.port and request.url.port != default_port:
            base_url += f":{request.url.port}"

        return fastapi.Response(
            status_code=resp.status_code,
            content=resp.content,
            headers=dict(
                utils.httpx_to_fastapi_headers(
                    resp.headers,
                    rewrite_url=(
                        "https://api.github.com",
                        f"{base_url}/front/proxy/github",
                    ),
                ),
            ),
        )
