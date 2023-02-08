from collections import abc
import typing

import httpx


HEADERS_TO_FORWARD = ("content-type", "date", "etag", "link", "location")
HEADERS_TO_REWRITE = ("link", "location")


def httpx_to_fastapi_headers(
    headers: httpx.Headers,
    rewrite_url: tuple[str, str] | None = None,
) -> abc.Iterator[tuple[str, str]]:
    for key, values in headers.items():
        if key in HEADERS_TO_FORWARD:
            if isinstance(values, tuple):
                for value in values:
                    if key in HEADERS_TO_REWRITE and rewrite_url is not None:
                        value = typing.cast(str, value).replace(*rewrite_url)
                    yield key, value
            else:
                if key in HEADERS_TO_REWRITE and rewrite_url is not None:
                    values = values.replace(*rewrite_url)
                yield key, values
