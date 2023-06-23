import fastapi
from starlette import responses

from mergify_engine import settings
from mergify_engine.web import utils


router = fastapi.APIRouter(tags=["badges"])


def _get_badge_url(
    owner: str, repo: str, ext: str, style: str
) -> responses.RedirectResponse:
    return responses.RedirectResponse(
        url=f"https://img.shields.io/endpoint.{ext}?url={settings.SUBSCRIPTION_URL}/badges/{owner}/{repo}&style={style}",
        status_code=302,
    )


@router.get("/{owner}/{repo}.png")
async def badge_png(
    owner: str, repo: str, style: str = "flat"
) -> responses.RedirectResponse:  # pragma: no cover
    return _get_badge_url(owner, repo, "png", style)


@router.get("/{owner}/{repo}.svg")
async def badge_svg(
    owner: str, repo: str, style: str = "flat"
) -> responses.RedirectResponse:  # pragma: no cover
    return _get_badge_url(owner, repo, "svg", style)


@router.get("/{owner}/{repo}")
async def badge(owner: str, repo: str) -> responses.RedirectResponse:
    return responses.RedirectResponse(
        url=f"{settings.SUBSCRIPTION_URL}/badges/{owner}/{repo}"
    )


def create_app(debug: bool = False) -> fastapi.FastAPI:
    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None, debug=debug)
    app.include_router(router)
    utils.setup_exception_handlers(app)
    return app
