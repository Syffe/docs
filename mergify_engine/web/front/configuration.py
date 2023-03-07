import typing

import fastapi

from mergify_engine import config
from mergify_engine import redis_utils
from mergify_engine.clients import github
from mergify_engine.web import redis


class ConfigJSON(typing.TypedDict):
    dd_client_token: str | None
    github_application_name: str
    github_server_url: str
    ui_features: list[str]


router = fastapi.APIRouter()


@router.get("/configuration", response_model=ConfigJSON)
async def configuration(
    redis_links: redis_utils.RedisLinks = fastapi.Depends(  # noqa: B008
        redis.get_redis_links
    ),
) -> ConfigJSON:
    app = await github.GitHubAppInfo.get_app(redis_cache=redis_links.cache)
    return ConfigJSON(
        {
            "dd_client_token": config.DASHBOARD_UI_DATADOG_CLIENT_TOKEN,
            "github_application_name": app["slug"],
            "github_server_url": config.GITHUB_URL,
            "ui_features": sorted(config.DASHBOARD_UI_FEATURES),
        }
    )
