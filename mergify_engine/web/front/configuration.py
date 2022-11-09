import typing

import fastapi

from mergify_engine import config


class ConfigJSON(typing.TypedDict):
    dd_client_token: str | None
    github_application_name: str
    github_server_url: str
    ui_features: list[str]


router = fastapi.APIRouter()


@router.get("/configuration", response_model=ConfigJSON)
def configuration() -> ConfigJSON:
    return ConfigJSON(
        {
            "dd_client_token": config.DASHBOARD_UI_DATADOG_CLIENT_TOKEN,
            "github_application_name": config.BOT_USER_LOGIN[:-5],
            "github_server_url": config.GITHUB_URL,
            "ui_features": sorted(config.DASHBOARD_UI_FEATURES),
        }
    )
