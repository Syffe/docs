import dataclasses
import datetime
import email.utils
import sys
import types
import typing

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
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


def _header_to_str(key: str, value: str) -> str:
    if key == "authorization":
        value = "*****"
    return f'"{key}: {value}"'


async def to_curl(request: httpx.Request, response: httpx.Response | None) -> str:
    request_headers = [_header_to_str(k, v) for k, v in request.headers.items()]
    request_headers_str = " -H ".join(request_headers)
    request_body = await request.aread()
    request_body_str = ""
    if request_body:
        request_body_str = f"-d '{request_body.decode() if isinstance(request_body, bytes) else request_body}'"

    curl_command = f"curl -X {request.method} -H {request_headers_str} {request_body_str} {request.url}"

    if response is None:
        # nosemgrep: python.flask.security.audit.directly-returned-format-string.directly-returned-format-string
        return f"{curl_command}\n<no response>"

    host = request.url.netloc.decode()
    response_headers = [_header_to_str(k, v) for k, v in response.headers.items()]
    response_headers_str = "\n< ".join(response_headers)
    response_body = (await response.aread()).decode()

    # nosemgrep: python.flask.security.audit.directly-returned-format-string.directly-returned-format-string
    return f"""{curl_command}
< {response.http_version} {response.status_code}
< {response_headers_str}
<
* Connection #0 to host {host} left intact
{response_body}
"""


def extract_message(response: httpx.Response) -> str:
    # TODO(sileht): do something with errors and documentation_url when present
    # https://developer.github.com/v3/#client-errors
    json_response = response.json()

    # GitHub
    message = json_response.get("message")

    if "errors" in json_response:
        if "message" in json_response["errors"][0]:
            message = json_response["errors"][0]["message"]
        elif isinstance(json_response["errors"][0], str):
            message = json_response["errors"][0]

    # OpenAI
    if message is None:
        message = json_response.get("error", {}).get("message")

    if message is None:
        message = "No error message provided"

    return typing.cast(str, message)


class HTTPCustomStatusError(httpx.HTTPStatusError):
    @property
    def status_code(self) -> int:
        return self.response.status_code

    async def to_curl(self) -> str:
        return await to_curl(self.request, self.response)


class HTTPServerSideError(HTTPCustomStatusError):
    @property
    def message(self) -> str:
        return self.response.text


class HTTPClientSideError(HTTPCustomStatusError):
    @property
    def message(self) -> str:
        return extract_message(self.response)


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
        return dt.replace(tzinfo=datetime.UTC)

    return dt


@dataclasses.dataclass
class TryAgainOnCertainHTTPError(tenacity.TryAgain):
    response: httpx.Response

    class wait_from_headers(tenacity.wait.wait_base):
        def __call__(self, retry_state: tenacity.RetryCallState) -> float:
            if retry_state.outcome is None:
                return 0

            exc = retry_state.outcome.exception()
            if exc is None or not isinstance(exc, TryAgainOnCertainHTTPError):
                return 0

            value = exc.response.headers.get("retry-after")
            if value is None:
                return 0
            if value.isdigit():
                return int(value)

            d = parse_date(value)
            if d is None:
                return 0
            return max(0, (d - date.utcnow()).total_seconds())


def extract_organization_login(client: httpx.AsyncClient) -> str | None:
    if client.auth and hasattr(client.auth, "_owner_login"):
        return client.auth._owner_login  # type: ignore[no-any-return]
    return None


def response_to_exception(resp: httpx.Response) -> HTTPStatusError | None:
    if httpx.codes.is_client_error(resp.status_code):
        error_type = "Client Error"
        exc_class = STATUS_CODE_TO_EXC.get(resp.status_code, HTTPClientSideError)
    elif httpx.codes.is_server_error(resp.status_code):
        error_type = "Server Error"
        exc_class = STATUS_CODE_TO_EXC.get(resp.status_code, HTTPServerSideError)
    else:
        return None

    details = resp.text if resp.text else "<empty-response>"
    message = f"{resp.status_code} {error_type}: {resp.reason_phrase} for url `{resp.url}`\nDetails: {details}"
    return exc_class(message, request=resp.request, response=resp)


def raise_for_status(resp: httpx.Response) -> None:
    exception = response_to_exception(resp)
    if exception is not None:
        raise exception


async def retry_async_httpx_send(
    send_method: typing.Callable[..., typing.Awaitable[httpx.Response]],
    request: httpx.Request,
    retry_stop_after_attempt: int = 5,
    retry_exponential_multiplier: float = 0.2,
    *args: typing.Any,
    **kwargs: typing.Any,
) -> httpx.Response:
    # NOTE(sileht): this does cover the case where stream=True and a NetworkError is raised
    # while the caller will read the response as `aiter_bytes()` will not be called by send()

    custom_retry = request.extensions.get("retry")
    custom_retry_log = request.extensions.get("retry_log")
    try:
        async for attempt in tenacity.AsyncRetrying(
            reraise=True,
            retry=tenacity.retry_if_exception_type(
                (RequestError, TryAgainOnCertainHTTPError)
            ),
            wait=tenacity.wait_combine(
                TryAgainOnCertainHTTPError.wait_from_headers(),
                tenacity.wait_exponential(multiplier=retry_exponential_multiplier),
            ),
            stop=tenacity.stop_after_attempt(retry_stop_after_attempt),
        ):
            with attempt:
                response = await send_method(request, *args, **kwargs)
                if response.status_code >= 500 or response.status_code == 429:
                    raise TryAgainOnCertainHTTPError(response=response)

                if custom_retry is not None:
                    try:
                        should_retry = custom_retry(response)
                    except httpx.ResponseNotRead:
                        # retry code need the response body
                        await response.aread()
                        should_retry = custom_retry(response)

                    if should_retry:
                        if custom_retry_log is not None:
                            custom_retry_log(response)
                        raise TryAgainOnCertainHTTPError(response=response)

    except TryAgainOnCertainHTTPError as exc:
        return exc.response

    return response


@dataclasses.dataclass
class RequestHistory:
    request: httpx.Request
    response: httpx.Response | None

    async def to_curl(self) -> str:
        return await to_curl(self.request, self.response)


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
        self._requests: list[RequestHistory] = []
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
        resp = await super().request(*args, **kwargs)
        raise_for_status(resp)
        return resp

    async def send(
        self,
        request: httpx.Request,
        *,
        stream: bool = False,
        auth: httpx._types.AuthTypes
        | httpx._client.UseClientDefault
        | None = httpx._client.USE_CLIENT_DEFAULT,
        follow_redirects: bool
        | httpx._client.UseClientDefault = httpx._client.USE_CLIENT_DEFAULT,
    ) -> httpx.Response:
        response = None
        try:
            response = await retry_async_httpx_send(
                super().send,
                request,
                stream=stream,
                auth=auth,
                follow_redirects=follow_redirects,
                retry_stop_after_attempt=self.retry_stop_after_attempt,
                retry_exponential_multiplier=self.retry_exponential_multiplier,
            )
        finally:
            if response is None:
                # https://github.com/python/mypy/issues/13104
                status_code = "error"  # type: ignore[unreachable]
            else:
                status_code = str(response.status_code)

            tags = self._get_datadog_tags()
            tags.append(f"status_code:{status_code}")
            statsd.increment("http.client.requests", tags=tags)
            self._requests.append(RequestHistory(request, response))

        return response

    @property
    def last_request(self) -> RequestHistory:
        return self._requests[-1]

    def _get_datadog_tags(self) -> list[str]:
        return [f"hostname:{self.base_url.host}"]

    def _generate_metrics(self) -> None:
        tags = self._get_datadog_tags()
        statsd.histogram("http.client.session", len(self._requests), tags=tags)

    async def aclose(self) -> None:
        await super().aclose()
        self._generate_metrics()
        self._requests = []

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        await super().__aexit__(exc_type, exc_value, traceback)
        self._generate_metrics()
        self._requests = []
