import email.utils
import os

import fastapi
import starlette

from mergify_engine import date
from mergify_engine import settings


router = fastapi.APIRouter(tags=["react"])


async def serve_static_file(
    request: fastapi.Request, filepath: str
) -> fastapi.Response:
    # NOTE(sileht): Ensure the destination file is located in REACT_BUILD_DIR
    assert settings.DASHBOARD_UI_STATIC_FILES_DIRECTORY is not None
    base_path = os.path.abspath(settings.DASHBOARD_UI_STATIC_FILES_DIRECTORY)
    path = os.path.abspath(f"{base_path}/{filepath}")

    if path.startswith(base_path) and os.path.isfile(path):
        if request.method == "HEAD":
            return fastapi.responses.Response(status_code=200)
        elif request.method == "GET":
            response = starlette.responses.FileResponse(path)
            response.headers["Cache-control"] = "no-cache"
            response.headers["Expires"] = email.utils.format_datetime(date.utcnow())
            return response

    raise fastapi.HTTPException(404)


@router.api_route("/index.html", methods=["GET", "HEAD"])
@router.api_route("/asset-manifest.json", methods=["GET", "HEAD"])
@router.api_route("/favicon.ico", methods=["GET", "HEAD"])
@router.api_route("/manifest.json", methods=["GET", "HEAD"])
@router.api_route("/static/{filename:path}", methods=["GET", "HEAD"])  # noqa: FS003
async def react_static_files(request: fastapi.Request) -> fastapi.Response:
    return await serve_static_file(request, request.url.path)


@router.get("/")
@router.get("/auth/login")
@router.get("/auth/logout")
@router.get("/auth/callback")
@router.get("/github")
@router.get("/github/")
@router.get("/github/{remaning:path}")  # noqa: FS003
async def react_app(request: fastapi.Request) -> fastapi.Response:
    return await serve_static_file(request, "index.html")
