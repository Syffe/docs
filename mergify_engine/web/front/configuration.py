import fastapi
import typing_extensions

from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.web import redis


class ConfigJSON(typing_extensions.TypedDict):
    dd_client_token: str | None
    github_application_name: str
    github_server_url: str
    ui_features: list[str]


router = fastapi.APIRouter(tags=["front"])


@router.get("/configuration", response_model=ConfigJSON)
async def configuration(redis_links: redis.RedisLinks) -> ConfigJSON:
    app = await github.GitHubAppInfo.get_app(redis_cache=redis_links.cache)
    return ConfigJSON(
        {
            "dd_client_token": settings.DASHBOARD_UI_DATADOG_CLIENT_TOKEN,
            "github_application_name": app["slug"],
            "github_server_url": settings.GITHUB_URL,
            "ui_features": sorted(settings.DASHBOARD_UI_FEATURES),
        }
    )
