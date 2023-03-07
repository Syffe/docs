import datetime
import email.utils
import sys
import typing

import anyio._core._exceptions
import daiquiri
import httpx
from httpx import _types as httpx_types
import tenacity
import tenacity.wait

from mergify_engine import date
from mergify_engine import service


LOG = daiquiri.getLogger(__name__)

PYTHON_VERSION = (
    f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
)
HTTPX_VERSION = httpx.__version__

DEFAULT_HEADERS = httpx.Headers(
    {
        "User-Agent": f"mergify-engine/{service.VERSION} python/{PYTHON_VERSION} httpx/{HTTPX_VERSION}"
    }
)

DEFAULT_TIMEOUT = httpx.Timeout(5.0, read=10.0)

HTTPStatusError = httpx.HTTPStatusError
RequestError = httpx.RequestError


class HTTPServerSideError(httpx.HTTPStatusError):
    @property
    def message(self) -> str:
        return self.response.text

    @property
    def status_code(self) -> int:
        return self.response.status_code


class HTTPClientSideError(httpx.HTTPStatusError):
    @property
    def message(self) -> str:
        # TODO(sileht): do something with errors and documentation_url when present
        # https://developer.github.com/v3/#client-errors
        response = self.response.json()
        message = response.get("message", "No error message provided by GitHub")

        if "errors" in response:
            if "message" in response["errors"][0]:
                message = response["errors"][0]["message"]
            elif isinstance(response["errors"][0], str):
                message = response["errors"][0]

        return typing.cast(str, message)

    @property
    def status_code(self) -> int:
        return self.response.status_code


class HTTPForbidden(HTTPClientSideError):
    pass


class HTTPUnauthorized(HTTPClientSideError):
    pass


class HTTPNotFound(HTTPClientSideError):
    pass


class HTTPTooManyRequests(HTTPClientSideError):
    pass


class HTTPServiceUnavailable(HTTPServerSideError):
    pass


STATUS_CODE_TO_EXC = {
    401: HTTPUnauthorized,
    403: HTTPForbidden,
    404: HTTPNotFound,
    429: HTTPTooManyRequests,
    503: HTTPServiceUnavailable,
}


def parse_date(value: str) -> datetime.datetime | None:
    try:
        dt = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)

    return dt


class wait_retry_after_header(tenacity.wait.wait_base):
    def __call__(self, retry_state: tenacity.RetryCallState) -> float:
        if retry_state.outcome is None:
            return 0

        exc = retry_state.outcome.exception()
        if exc is None or not isinstance(exc, HTTPStatusError):
            return 0

        value = exc.response.headers.get("retry-after")
        if value is None:
            return 0
        elif value.isdigit():
            return int(value)

        d = parse_date(value)
        if d is None:
            return 0
        return max(0, (d - date.utcnow()).total_seconds())


def extract_organization_login(client: httpx.AsyncClient) -> str | None:
    if client.auth and hasattr(client.auth, "_owner_login"):
        return client.auth._owner_login  # type: ignore[no-any-return]
    return None


def raise_for_status(resp: httpx.Response) -> None:
    if httpx.codes.is_client_error(resp.status_code):
        error_type = "Client Error"
        exc_class = STATUS_CODE_TO_EXC.get(resp.status_code, HTTPClientSideError)
    elif httpx.codes.is_server_error(resp.status_code):
        error_type = "Server Error"
        exc_class = STATUS_CODE_TO_EXC.get(resp.status_code, HTTPServerSideError)
    else:
        return

    details = resp.text if resp.text else "<empty-response>"
    message = f"{resp.status_code} {error_type}: {resp.reason_phrase} for url `{resp.url}`\nDetails: {details}"
    raise exc_class(message, request=resp.request, response=resp)


class AsyncClient(httpx.AsyncClient):
    def __init__(
        self,
        auth: httpx_types.AuthTypes | None = None,
        headers: httpx_types.HeaderTypes | None = None,
        timeout: httpx_types.TimeoutTypes = DEFAULT_TIMEOUT,
        base_url: httpx_types.URLTypes = "",
        transport: httpx.AsyncBaseTransport | None = None,
        retry_stop_after_attempt: int = 5,
        retry_exponential_multiplier: float = 0.2,
    ) -> None:
        self.retry_stop_after_attempt = retry_stop_after_attempt
        self.retry_exponential_multiplier = retry_exponential_multiplier
        final_headers = DEFAULT_HEADERS.copy()
        if headers is not None:
            final_headers.update(headers)
        super().__init__(
            auth=auth,
            base_url=base_url,
            headers=final_headers,
            timeout=timeout,
            follow_redirects=True,
        )

    async def request(self, *args: typing.Any, **kwargs: typing.Any) -> httpx.Response:
        async for attempt in tenacity.AsyncRetrying(
            reraise=True,
            retry=tenacity.retry_if_exception_type(
                (RequestError, HTTPServerSideError, HTTPTooManyRequests)
            ),
            wait=tenacity.wait_combine(
                wait_retry_after_header(),
                tenacity.wait_exponential(multiplier=self.retry_exponential_multiplier),
            ),
            stop=tenacity.stop_after_attempt(self.retry_stop_after_attempt),
        ):
            with attempt:
                try:
                    resp = await super().request(*args, **kwargs)
                except anyio._core._exceptions.ClosedResourceError:
                    # FIXME(sileht): https://github.com/encode/httpx/issues/2337
                    raise httpx.RequestError("Connection unexpectely closed")
                raise_for_status(resp)
        return resp
