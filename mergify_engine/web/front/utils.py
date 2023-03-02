from collections import abc

import httpx


HEADERS_TO_FORWARD = ("content-type", "date", "etag", "link", "location")
HEADERS_TO_REWRITE = ("link", "location")


def httpx_to_fastapi_headers(
    headers: httpx.Headers,
    rewrite_url: tuple[str, str] | None = None,
) -> abc.Iterator[tuple[str, str]]:
    for key, value in headers.items():
        if key in HEADERS_TO_FORWARD:
            if key in HEADERS_TO_REWRITE and rewrite_url is not None:
                value = value.replace(*rewrite_url)
            yield key, value
