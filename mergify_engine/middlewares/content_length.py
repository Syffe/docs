import dataclasses

from fastapi import status
from starlette import requests
from starlette import types
import starlette.exceptions


@dataclasses.dataclass
class ContentLengthMiddleware:
    app: types.ASGIApp
    max_content_size: int = 1024 * 1024 * 1

    async def __call__(
        self,
        scope: types.Scope,
        receive: types.Receive,
        send: types.Send,
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request = requests.Request(scope=scope)

        if "content-length" in request.headers:
            # the header value is not really trustable, but we can fail fast
            # in such case.
            try:
                size = int(request.headers["content-length"])
            except ValueError:
                response = starlette.responses.Response(
                    status_code=status.HTTP_411_LENGTH_REQUIRED,
                )
                await response(scope, receive, send)
                return

            if size > self.max_content_size:
                response = starlette.responses.Response(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                )
                await response(scope, receive, send)
                return

        # Only these methods require a content-length, others are optional
        # https://datatracker.ietf.org/doc/html/rfc7230#section-3.3.2
        elif request.method in ("POST", "PUT", "PATCH"):
            response = starlette.responses.Response(
                status_code=status.HTTP_411_LENGTH_REQUIRED,
            )
            await response(scope, receive, send)
            return

        received = 0

        async def content_length_check_receiver() -> types.Message:
            nonlocal received
            message = await receive()
            if message["type"] != "http.request":
                return message
            body_len = len(message.get("body", b""))
            received += body_len
            if received > self.max_content_size:
                raise starlette.exceptions.HTTPException(
                    status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                )
            return message

        await self.app(scope, content_length_check_receiver, send)
