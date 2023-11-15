import asyncio
import dataclasses
import logging
import sys
import typing

import daiquiri
from starlette import datastructures
from starlette import requests
from starlette import types


LOG = daiquiri.getLogger(__name__)


class ResponseInfo(typing.TypedDict):
    status: int
    headers: list[tuple[str, str]]


@dataclasses.dataclass
class LoggingMiddleware:
    app: types.ASGIApp

    async def __call__(
        self,
        scope: types.Scope,
        receive: types.Receive,
        send: types.Send,
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = requests.Request(scope, receive, send)
        request_info = {
            "method": request.method,
            "url": request.url,
            "headers": sorted(
                (key, value)
                for key, value in request.headers.items()
                if key != "authorization"
            ),
        }

        response_info = ResponseInfo({"status": 0, "headers": []})

        async def send_wrapper(message: types.Message) -> None:
            if message["type"] == "http.response.start":
                response_info["status"] = message["status"]
                response_info["headers"] = [
                    (key, value)
                    for key, value in datastructures.Headers(scope=message).items()
                    if key != "set-cookie"
                ]

            await send(message)

        exc_info = None
        try:
            await self.app(scope, receive, send_wrapper)
        except asyncio.CancelledError:
            raise
        except requests.ClientDisconnect:
            raise
        except Exception:
            exc_info = sys.exc_info()
            raise
        finally:
            if exc_info:
                level = logging.ERROR
            else:
                level = logging.INFO
            LOG.log(
                level,
                "request",
                request=request_info,
                response=response_info,
                exc_info=exc_info,
            )
