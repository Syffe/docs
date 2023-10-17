from collections import abc
import typing

import daiquiri
import httpx
import pydantic
from pydantic import functional_validators

from mergify_engine.clients import http


HEADERS_TO_FORWARD = ("content-type", "date", "etag", "link", "location")
HEADERS_TO_REWRITE = ("link", "location")

LOG = daiquiri.getLogger(__name__)


def CheckNullChar(v: str) -> str:
    assert "\x00" not in v, f"{v} is not a valid string"
    return v


PostgresText = typing.Annotated[
    str,
    pydantic.Field(
        min_length=1,
        max_length=255,
        json_schema_extra={"strip_whitespace": True},
    ),
    functional_validators.AfterValidator(CheckNullChar),
]


def httpx_to_fastapi_headers(
    headers: httpx.Headers,
    rewrite_url: tuple[str, str] | None = None,
) -> abc.Iterator[tuple[str, str]]:
    for key, value in headers.items():
        if key in HEADERS_TO_FORWARD:
            if key in HEADERS_TO_REWRITE and rewrite_url is not None:
                value = value.replace(*rewrite_url)
            yield key, value


async def override_and_warn_unexpected_content_type(
    request: httpx.Request,
    response: httpx.Response,
    requests_to_not_warn: list[tuple[str, str, str]] | None = None,
) -> None:
    # NOTE(sileht): Our API must never returned:
    # * text/html as this could be used to html injection
    # * application/* unknown by browsers as this could lead to unexpected file download
    # Some part of our API is just a proxy to third-party API, we don't control the content-type
    # So here an helper to detect this security issue.
    # TODO(sileht): replace LOG.error by an HTTPException once we are ready to block anything not json

    if (
        300 <= response.status_code < 400
        or response.status_code >= 500
        or response.status_code in (401, 403)
    ):
        return

    requests_to_not_warn = requests_to_not_warn or []
    if "application/json" not in response.headers.get("content-type", ""):
        for (
            request_method,
            request_path,
            override_content_type,
        ) in requests_to_not_warn:
            if request.method == request_method and request.url.path == request_path:
                response.headers["content-type"] = override_content_type
                break
        else:
            LOG.error(
                "an API proxy return unexpected content-type",
                content_type=response.headers.get("content-type"),
                curl=await http.to_curl(request, response),
            )
