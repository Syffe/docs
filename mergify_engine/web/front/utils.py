from collections import abc

import daiquiri
import fastapi
import httpx


HEADERS_TO_FORWARD = ("content-type", "date", "etag", "link", "location")
HEADERS_TO_REWRITE = ("link", "location")

LOG = daiquiri.getLogger(__name__)


def headers_to_forward(request: fastapi.Request) -> dict[str, str]:
    return {
        k: v
        for k, v in request.headers.items()
        if k.lower().startswith("accept") or k.lower() == "content-type"
    }


def httpx_to_fastapi_headers(
    headers: httpx.Headers,
    rewrite_url: tuple[str, str] | None = None,
) -> abc.Iterator[tuple[str, str]]:
    for key, value in headers.items():
        if key in HEADERS_TO_FORWARD:
            if key in HEADERS_TO_REWRITE and rewrite_url is not None:
                value = value.replace(*rewrite_url)
            yield key, value


async def override_or_raise_unexpected_content_type(
    request: httpx.Request,
    response: httpx.Response,
    requests_to_override: list[tuple[str, str, str]] | None = None,
) -> None:
    # NOTE(sileht): Our API must never returned:
    # * text/html as this could be used to html injection
    # * application/* unknown by browsers as this could lead to unexpected file download
    # Some part of our API is just a proxy to third-party API, we don't control the content-type
    # So here an helper to detect this security issue.

    if "application/json" in response.headers.get("content-type", ""):
        return

    if (
        300 <= response.status_code < 400
        or response.status_code >= 500
        or response.status_code in (202, 204, 401, 403)
    ):
        return

    requests_to_override = requests_to_override or []
    for (
        request_method,
        request_path,
        override_content_type,
    ) in requests_to_override:
        if request.method == request_method and request.url.path == request_path:
            response.headers["content-type"] = override_content_type
            return

    raise fastapi.HTTPException(415, "Unsupported request")
