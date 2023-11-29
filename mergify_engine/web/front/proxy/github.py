import fastapi

from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import http_proxy
from mergify_engine.web.front import security


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
    async with github.AsyncGitHubInstallationClient(
        github.GitHubTokenAuth(current_user.oauth_access_token),
    ) as client:
        return await http_proxy.proxy(
            client,
            request,
            url=f"{settings.GITHUB_REST_API_URL}/{path}",
            rewrite_url=(
                "https://api.github.com",
                "/front/proxy/github",
            ),
            requests_to_override_content_type=[
                ("POST", "/markdown", "text/plain"),
                ("POST", "/markdown/raw", "text/plain"),
            ],
        )
