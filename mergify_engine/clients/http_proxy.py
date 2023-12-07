from collections import abc

import daiquiri
import fastapi
import httpx

from mergify_engine.clients import http


HEADERS_TO_FORWARD = ("content-type", "date", "etag", "link", "location")
HEADERS_TO_REWRITE = ("link", "location")

LOG = daiquiri.getLogger(__name__)


def httpx_to_fastapi_headers(
    headers: httpx.Headers,
    rewrite_url: tuple[str, str] | None = None,
) -> abc.Iterator[tuple[str, str]]:
    for key, value in headers.items():
        if key in HEADERS_TO_FORWARD:
            if key in HEADERS_TO_REWRITE and rewrite_url is not None:
                value = value.replace(*rewrite_url)  # noqa: PLW2901
            yield key, value


async def override_or_raise_unexpected_content_type(
    request: httpx.Request,
    response: httpx.Response,
    requests_to_override: list[tuple[str, str, str]] | None = None,
) -> None:
    # NOTE(sileht): Our API must never return:
    # * text/html as this could be used for html injection
    # * application/* unknown by browsers as this could lead to unexpected file download
    # Some part of our API is just a proxy to third-party API, we don't control the content-type
    # So here is an helper to detect this security issue.

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


def headers_to_forward(request: fastapi.Request) -> dict[str, str]:
    return {
        k: v
        for k, v in request.headers.items()
        if k.lower().startswith("accept") or k.lower() == "content-type"
    }


async def proxy(
    client: http.AsyncClient,
    request: fastapi.Request,
    url: str,
    rewrite_url: tuple[str, str],
    follow_redirects: bool = True,
    extra_headers: dict[str, str] | None = None,
    requests_to_override_content_type: list[tuple[str, str, str]] | None = None,
) -> fastapi.Response:
    headers = headers_to_forward(request)
    if extra_headers:
        headers.update(extra_headers)

    proxy_request = client.build_request(
        method=request.method,
        url=url,
        params=request.url.query,
        headers=headers,
        content=await request.body(),
    )

    try:
        proxy_response = await client.send(
            proxy_request,
            follow_redirects=follow_redirects,
        )
        http.raise_for_status(proxy_response)
    except httpx.InvalidURL:
        raise fastapi.HTTPException(
            status_code=422,
            detail={"messages": "Invalid request"},
        )
    except httpx.HTTPStatusError as e:
        proxy_response = e.response
    except httpx.RequestError:
        proxy_response = None

    if proxy_response is None or proxy_response.status_code >= 500:
        proxy_response = httpx.Response(
            status_code=502,
            content="Bad Gateway",
            request=proxy_request,
            headers=dict[str, str](),
        )

    base_url = f"{request.url.scheme}://{request.url.hostname}"
    default_port = {"http": 80, "https": 443}[request.url.scheme]
    if request.url.port != default_port:
        base_url += f":{request.url.port}"

    await override_or_raise_unexpected_content_type(
        proxy_response.request,
        proxy_response,
        requests_to_override_content_type,
    )

    return fastapi.Response(
        status_code=proxy_response.status_code,
        content=proxy_response.content,
        headers=dict(
            httpx_to_fastapi_headers(
                proxy_response.headers,
                rewrite_url=(rewrite_url[0], f"{base_url}{rewrite_url[1]}"),
            ),
        ),
    )
