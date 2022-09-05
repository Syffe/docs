import fastapi
from starlette import responses


router = fastapi.APIRouter()


def _get_badge_url(
    owner: str, repo: str, ext: str, style: str
) -> responses.RedirectResponse:
    return responses.RedirectResponse(
        url=f"https://img.shields.io/endpoint.{ext}?url=https://dashboard.mergify.com/badges/{owner}/{repo}&style={style}",
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
        url=f"https://dashboard.mergify.com/badges/{owner}/{repo}"
    )
