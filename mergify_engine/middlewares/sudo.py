import dataclasses

from starlette import datastructures
from starlette import types


@dataclasses.dataclass
class SudoMiddleware:
    app: types.ASGIApp

    async def __call__(
        self, scope: types.Scope, receive: types.Receive, send: types.Send
    ) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_wrapper(message: types.Message) -> None:
            if message["type"] == "http.response.start":
                if scope["session"] and "sudoGrantedTo" in scope["session"]:
                    headers = datastructures.MutableHeaders(scope=message)
                    headers["Mergify-Sudo-Granted-To"] = scope["session"][
                        "sudoGrantedTo"
                    ]
            await send(message)

        await self.app(scope, receive, send_wrapper)
