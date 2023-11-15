import dataclasses

from starlette import datastructures
from starlette import types

from mergify_engine import settings


@dataclasses.dataclass
class SecurityMiddleware:
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

        async def send_wrapper(message: types.Message) -> None:
            if message["type"] == "http.response.start":
                headers = datastructures.MutableHeaders(scope=message)
                headers.update(
                    {
                        "Strict-Transport-Security": "max-age=2592000",
                        "X-Frame-Options": "DENY",
                        "X-Content-Type-Options": "nosniff",
                        "Referrer-Policy": "no-referrer",
                        # https://github.com/w3c/webappsec-permissions-policy/issues/189
                        "Permissions-Policy": "accelerometer=(),ambient-light-sensor=(),attribution-reporting=(),autoplay=(),battery=(),camera=(),clipboard-read=(),clipboard-write=(),conversion-measurement=(),cross-origin-isolated=(),direct-sockets=(),display-capture=(),document-domain=(),encrypted-media=(),execution-while-not-rendered=(),execution-while-out-of-viewport=(),focus-without-user-activation=(),fullscreen=(),gamepad=(),geolocation=(),gyroscope=(),hid=(),idle-detection=(),interest-cohort=(),magnetometer=(),microphone=(),midi=(),navigation-override=(),otp-credentials=(),payment=(),picture-in-picture=(),publickey-credentials-get=(),screen-wake-lock=(),serial=(),shared-autofill=(),speaker-selection=(),storage-access-api=(),sync-script=(),sync-xhr=(),trust-token-redemption=(),usb=(),vertical-scroll=(),wake-lock=(),web-share=(),window-placement=(),xr-spatial-tracking=()",
                    },
                )

                # NOTE(sileht): On-Premise use need to access to the UI static files
                if settings.DASHBOARD_UI_STATIC_FILES_DIRECTORY is None:
                    headers["Content-Security-Policy"] = "default-src 'none'"  # type: ignore[unreachable]

            await send(message)

        await self.app(scope, receive, send_wrapper)
