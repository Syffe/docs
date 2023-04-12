import typing

import fastapi
from starlette import responses

from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.web import api


router = fastapi.APIRouter(
    tags=["badges"],
)


def _get_badge_url(
    owner: github_types.GitHubLogin,
    repo: github_types.GitHubRepositoryName,
    ext: str,
    style: str,
) -> responses.RedirectResponse:
    return responses.RedirectResponse(
        url=f"https://img.shields.io/endpoint.{ext}?url={settings.SUBSCRIPTION_URL}/badges/{owner}/{repo}&style={style}",
        status_code=302,
    )


@router.get(
    "/badges/{owner}/{repository}.png",
    summary="Get PNG badge",
    description="Get badge in PNG image format",
    response_class=fastapi.Response,  # to drop application/json from openapi media type
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "Not found"},
        200: {
            "content": {"image/png": {}},
            "description": "An PNG image.",
        },
    },
)
async def badge_png(
    owner: typing.Annotated[
        github_types.GitHubLogin,
        fastapi.Path(description="The owner of the repository"),
    ],
    repository: typing.Annotated[
        github_types.GitHubRepositoryName,
        fastapi.Path(description="The name of the repository"),
    ],
    # TODO(charly): we can't use typing.Annotated here, FastAPI 0.95.0 has a bug with APIRouter
    # https://github.com/tiangolo/fastapi/discussions/9279
    style: str = fastapi.Query(  # noqa: B008
        default="flat",
        description="The style of the button, more details on https://shields.io/.",
    ),
) -> responses.RedirectResponse:  # pragma: no cover
    return _get_badge_url(owner, repository, "png", style)


@router.get(
    "/badges/{owner}/{repository}.svg",
    summary="Get SVG badge",
    description="Get badge in SVG image format",
    response_class=fastapi.Response,  # to drop application/json from openapi media type
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "Not found"},
        200: {
            "content": {"image/png": {}},
            "description": "An SVG image.",
        },
    },
)
async def badge_svg(
    owner: typing.Annotated[
        github_types.GitHubLogin,
        fastapi.Path(description="The owner of the repository"),
    ],
    repository: typing.Annotated[
        github_types.GitHubRepositoryName,
        fastapi.Path(description="The name of the repository"),
    ],
    # TODO(charly): we can't use typing.Annotated here, FastAPI 0.95.0 has a bug with APIRouter
    # https://github.com/tiangolo/fastapi/discussions/9279
    style: str = fastapi.Query(  # noqa: B008
        default="flat",
        description="The style of the button, more details on https://shields.io/.",
    ),
) -> responses.RedirectResponse:  # pragma: no cover
    return _get_badge_url(owner, repository, "svg", style)


@router.get(
    "/badges/{owner}/{repository}",
    summary="Get shields.io badge config",
    description="Get shields.io badge JSON configuration",
    response_class=fastapi.Response,  # Allow to not document the shields.io format
    responses={
        **api.default_responses,  # type: ignore
        404: {"description": "Not found"},
        200: {
            "content": {"application/json": {}},
            "description": "The shields.io badge JSON configuration",
        },
    },
)
async def badge(
    owner: typing.Annotated[
        github_types.GitHubLogin,
        fastapi.Path(description="The owner of the repository"),
    ],
    repository: typing.Annotated[
        github_types.GitHubRepositoryName,
        fastapi.Path(description="The name of the repository"),
    ],
) -> responses.RedirectResponse:
    return responses.RedirectResponse(
        url=f"{settings.SUBSCRIPTION_URL}/badges/{owner}/{repository}"
    )
