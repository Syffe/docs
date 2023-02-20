import fastapi
from starlette import responses

from mergify_engine import config
from mergify_engine.middlewares import starlette_workaround
from mergify_engine.web import utils


router = fastapi.APIRouter()


def _get_badge_url(
    owner: str, repo: str, ext: str, style: str
) -> responses.RedirectResponse:
    return responses.RedirectResponse(
        url=f"https://img.shields.io/endpoint.{ext}?url={config.SUBSCRIPTION_BASE_URL}/badges/{owner}/{repo}&style={style}",
        status_code=302,
    )


@router.get("/{owner}/{repo}.png")  # noqa: FS003
async def badge_png(
    owner: str, repo: str, style: str = "flat"
) -> responses.RedirectResponse:  # pragma: no cover
    return _get_badge_url(owner, repo, "png", style)


@router.get("/{owner}/{repo}.svg")  # noqa: FS003
async def badge_svg(
    owner: str, repo: str, style: str = "flat"
) -> responses.RedirectResponse:  # pragma: no cover
    return _get_badge_url(owner, repo, "svg", style)


@router.get("/{owner}/{repo}")  # noqa: FS003
async def badge(owner: str, repo: str) -> responses.RedirectResponse:
    return responses.RedirectResponse(
        url=f"{config.SUBSCRIPTION_BASE_URL}/badges/{owner}/{repo}"
    )


def create_app(debug: bool = False) -> fastapi.FastAPI:
    app = fastapi.FastAPI(openapi_url=None, redoc_url=None, docs_url=None, debug=debug)
    app.include_router(router)
    app.add_middleware(starlette_workaround.StarletteWorkaroundMiddleware)
    utils.setup_exception_handlers(app)
    return app
