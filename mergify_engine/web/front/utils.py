from collections import abc
import typing

import httpx


def httpx_to_fastapi_headers(
    headers: httpx.Headers,
    rewrite_url: tuple[str, str] | None = None,
) -> abc.Iterator[tuple[str, str]]:

    for key, values in headers.items():
        if key in ("content-type", "date", "etag", "link"):
            if isinstance(values, tuple):
                for value in values:
                    if key == "link" and rewrite_url is not None:
                        value = typing.cast(str, value).replace(*rewrite_url)
                    yield key, value
            else:
                if key == "link" and rewrite_url is not None:
                    values = values.replace(*rewrite_url)
                yield key, values
