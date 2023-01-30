from __future__ import annotations

from collections import abc
import contextlib
import dataclasses
import datetime
import json
import random
import types
import typing
from urllib import parse

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import first
import httpx
import msgpack

from mergify_engine import config
from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.clients import github_app
from mergify_engine.clients import http


RATE_LIMIT_THRESHOLD = 20
LOGGING_REQUESTS_THRESHOLD = 40
LOGGING_REQUESTS_THRESHOLD_ABSOLUTE = 400

LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class TooManyPages(exceptions.UnprocessablePullRequest):
    reason: str
    per_page: int
    page_limit: int
    last_page: int


@dataclasses.dataclass
class GraphqlError(Exception):
    message: str


@dataclasses.dataclass
class CachedToken:
    STORAGE: typing.ClassVar[
        dict[github_types.GitHubInstallationIdType, "CachedToken"]
    ] = {}

    installation_id: github_types.GitHubInstallationIdType
    token: str
    expiration: datetime.datetime

    def __post_init__(self) -> None:
        CachedToken.STORAGE[self.installation_id] = self

    @classmethod
    def get(
        cls, installation_id: github_types.GitHubInstallationIdType
    ) -> CachedToken | None:
        return cls.STORAGE.get(installation_id)

    def invalidate(self) -> None:
        CachedToken.STORAGE.pop(self.installation_id, None)


class InstallationInaccessible(Exception):
    message: str


class GithubTokenAuth(httpx.Auth):
    _token: str

    def __init__(self, token: str) -> None:
        self._token = token

    @contextlib.contextmanager
    def response_body_read(self) -> abc.Generator[None, None, None]:
        self.requires_response_body = True
        try:
            yield
        finally:
            self.requires_response_body = False

    def auth_flow(
        self, request: httpx.Request
    ) -> abc.Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"token {self._token}"
        yield request

    def build_request(self, method: str, url: str) -> httpx.Request:
        headers = http.DEFAULT_HEADERS.copy()
        headers["Authorization"] = f"token {self._token}"
        return httpx.Request(method, url, headers=headers)

    def get_access_token(self) -> str:
        return self._token


class GithubAppInstallationAuth(httpx.Auth):
    installation: github_types.GitHubInstallation

    def __init__(self, installation: github_types.GitHubInstallation) -> None:
        self._installation = installation
        self._cached_token = CachedToken.get(self._installation["id"])

    @property
    def _owner_id(self) -> int:
        return self._installation["account"]["id"]

    @property
    def _owner_login(self) -> str:
        return self._installation["account"]["login"]

    @contextlib.contextmanager
    def response_body_read(self) -> abc.Generator[None, None, None]:
        self.requires_response_body = True
        try:
            yield
        finally:
            self.requires_response_body = False

    def auth_flow(
        self, request: httpx.Request
    ) -> abc.Generator[httpx.Request, httpx.Response, None]:
        token = self.get_access_token()
        if token:
            request.headers["Authorization"] = f"token {token}"
            response = yield request
            if response.status_code != 401:  # due to access_token
                return

        with self.response_body_read():
            auth_response = yield self.build_access_token_request()
            if auth_response.status_code == 401:  # due to jwt
                auth_response = yield self.build_access_token_request(force=True)

            if auth_response.status_code == 404:
                raise exceptions.MergifyNotInstalled()
            elif auth_response.status_code == 403:
                error_message = auth_response.json()["message"]
                if "This installation has been suspended" in error_message:
                    LOG.debug(
                        "Mergify installation suspended",
                        gh_owner=self._owner_login,
                        error_message=error_message,
                    )
                    raise exceptions.MergifyNotInstalled()

            http.raise_for_status(auth_response)
            token = self._set_access_token(auth_response.json())

        request.headers["Authorization"] = f"token {token}"
        yield request

    def build_access_token_request(self, force: bool = False) -> httpx.Request:
        if self._installation is None:
            raise RuntimeError("No installation")

        return self.build_github_app_request(
            "POST",
            f"{config.GITHUB_REST_API_URL}/app/installations/{self._installation['id']}/access_tokens",
            force=force,
        )

    def build_github_app_request(
        self, method: str, url: str, force: bool = False
    ) -> httpx.Request:
        headers = http.DEFAULT_HEADERS.copy()
        headers["Authorization"] = f"Bearer {github_app.get_or_create_jwt(force)}"
        return httpx.Request(method, url, headers=headers)

    def _set_access_token(
        self, data: github_types.GitHubInstallationAccessToken
    ) -> str:
        if self._installation is None:
            raise RuntimeError("Cannot set access token, no installation")

        self._cached_token = CachedToken(
            self._installation["id"],
            data["token"],
            datetime.datetime.fromisoformat(data["expires_at"][:-1]),  # Remove the Z
        )
        LOG.debug(
            "New token acquired",
            gh_owner=self._owner_login,
            expire_at=self._cached_token.expiration,
        )
        return self._cached_token.token

    def get_access_token(self) -> str | None:
        now = datetime.datetime.utcnow()
        if not self._cached_token:
            return None
        elif self._cached_token.expiration <= now:
            LOG.debug(
                "Token expired",
                gh_owner=self._owner_login,
                expire_at=self._cached_token.expiration,
            )
            self._cached_token.invalidate()
            self._cached_token = None
            return None
        else:
            return self._cached_token.token


async def get_installation_from_account_id(
    account_id: github_types.GitHubAccountIdType,
) -> github_types.GitHubInstallation:
    async with AsyncGithubClient(auth=github_app.GithubBearerAuth()) as client:
        try:
            return typing.cast(
                github_types.GitHubInstallation,
                await client.item(
                    f"{config.GITHUB_REST_API_URL}/user/{account_id}/installation"
                ),
            )
        except http.HTTPNotFound as e:
            LOG.debug(
                "Mergify not installed",
                gh_owner_id=account_id,
                error_message=e.message,
            )
            raise exceptions.MergifyNotInstalled()


async def get_installation_from_login(
    login: github_types.GitHubLogin,
) -> github_types.GitHubInstallation:
    async with AsyncGithubClient(auth=github_app.GithubBearerAuth()) as client:
        try:
            return typing.cast(
                github_types.GitHubInstallation,
                await client.item(
                    f"{config.GITHUB_REST_API_URL}/users/{login}/installation"
                ),
            )
        except http.HTTPNotFound as e:
            LOG.debug(
                "Mergify not installed",
                gh_owner=login,
                error_message=e.message,
            )
            raise exceptions.MergifyNotInstalled()


def _check_rate_limit(response: httpx.Response) -> None:
    if response.status_code not in (403, 422, 429) or (
        response.status_code == 422
        and (
            "errors" not in response.json()
            or len(response.json()["errors"]) != 1
            or not isinstance(response.json()["errors"][0], dict)
            or response.json()["errors"][0].get("code", "") != "abuse"
        )
    ):
        return

    remaining = response.headers.get("X-RateLimit-Remaining")
    if remaining is None:
        return

    remaining = int(remaining)
    if remaining < RATE_LIMIT_THRESHOLD:
        reset = response.headers.get("X-RateLimit-Reset")
        if reset is None:
            delta = datetime.timedelta(minutes=5)
        else:
            delta = (
                date.fromtimestamp(int(reset))
                # NOTE(sileht): we add 5 seconds to not be subject to
                # time jitter and retry too early
                + datetime.timedelta(seconds=5)
                - date.utcnow_from_clock_realtime()
            )
        if response.url is not None:
            LOG.warning(
                "got ratelimited",
                retry_planned_in=str(delta),
                method=response.request.method,
                url=response.request.url,
                final_url=response.url,
                headers=response.headers,
                content=response.content,
            )
            statsd.increment(
                "http.client.rate_limited",
                tags=[f"hostname:{response.url.host}"],
            )
        raise exceptions.RateLimited(delta, remaining)


def _inject_options(func: typing.Any) -> typing.Any:
    async def wrapper(
        self: "AsyncGithubInstallationClient",
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> typing.Any:
        headers = kwargs.pop("headers", {})
        if api_version:
            headers["Accept"] = f"application/vnd.github.{api_version}-preview+json"
        if oauth_token:
            kwargs["auth"] = GithubTokenAuth(oauth_token)
        return await func(url, headers=headers, **kwargs)

    return wrapper


DEFAULT_GITHUB_TRANSPORT = httpx.AsyncHTTPTransport(
    limits=httpx.Limits(max_connections=None, max_keepalive_connections=20),
    http2=True,
)


class AsyncGithubClient(http.AsyncClient):
    auth: (github_app.GithubBearerAuth | GithubAppInstallationAuth | GithubTokenAuth)

    def __init__(
        self,
        auth: (
            github_app.GithubBearerAuth | GithubAppInstallationAuth | GithubTokenAuth
        ),
    ) -> None:
        super().__init__(
            base_url=config.GITHUB_REST_API_URL,
            auth=auth,
            headers={"Accept": "application/vnd.github.machine-man-preview+json"},
            transport=DEFAULT_GITHUB_TRANSPORT,
        )

    def _prepare_request_kwargs(
        self,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> typing.Any:
        if api_version:
            kwargs.setdefault("headers", {})[
                "Accept"
            ] = f"application/vnd.github.{api_version}-preview+json"
        if oauth_token:
            if isinstance(self.auth, github_app.GithubBearerAuth):
                raise TypeError(
                    "oauth_token is not supported for GithubBearerAuth auth"
                )
            kwargs["auth"] = GithubTokenAuth(oauth_token)
        return kwargs

    async def get(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().get(
            url, **self._prepare_request_kwargs(api_version, oauth_token, **kwargs)
        )

    async def post(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().post(
            url, **self._prepare_request_kwargs(api_version, oauth_token, **kwargs)
        )

    async def put(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().put(
            url, **self._prepare_request_kwargs(api_version, oauth_token, **kwargs)
        )

    async def patch(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().patch(
            url, **self._prepare_request_kwargs(api_version, oauth_token, **kwargs)
        )

    async def head(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().head(
            url, **self._prepare_request_kwargs(api_version, oauth_token, **kwargs)
        )

    async def delete(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().delete(
            url, **self._prepare_request_kwargs(api_version, oauth_token, **kwargs)
        )

    async def graphql_post(
        self,
        query: str,
        oauth_token: github_types.GitHubOAuthToken | None = None,
    ) -> typing.Any:
        response = await self.post(
            config.GITHUB_GRAPHQL_API_URL,
            json={"query": query},
            oauth_token=oauth_token,
        )
        data = response.json()
        if "data" not in data:
            raise GraphqlError(response.text)
        if len(data.get("errors", [])) > 0:
            raise GraphqlError(json.dumps(data["errors"]))
        return data

    async def item(
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        params: dict[str, str] | None = None,
    ) -> typing.Any:
        response = await self.get(
            url, api_version=api_version, oauth_token=oauth_token, params=params
        )
        return response.json()

    async def items(
        self,
        url: str,
        *,
        resource_name: str,
        page_limit: int | None,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        list_items: str | None = None,
        params: dict[str, str] | None = None,
        batch_size: int | None = None,
    ) -> typing.Any:

        # NOTE(sileht): can't be on the same line...
        # https://github.com/python/mypy/issues/10743
        final_params: dict[str, str] | None

        if batch_size is None:
            final_params = {"per_page": "100"}
        elif batch_size > 100:
            raise RuntimeError("items(batch_size=) must be < 100")
        else:
            final_params = {"per_page": str(batch_size)}

        if params is not None:
            final_params.update(params)

        per_page = int(final_params["per_page"])

        while True:
            response = await self.get(
                url,
                api_version=api_version,
                oauth_token=oauth_token,
                params=final_params,
            )
            last_url = response.links.get("last", {}).get("url")
            if last_url:
                last_page = int(
                    parse.parse_qs(parse.urlparse(last_url).query)["page"][0]
                )
                if page_limit is not None and last_page > page_limit:
                    raise TooManyPages(
                        f"The pull request reports more than {(last_page - 1) * per_page} {resource_name}, "
                        f"reaching the limit of {page_limit * per_page} {resource_name}.",
                        per_page,
                        page_limit,
                        last_page,
                    )

            items = response.json()
            if list_items:
                items = items[list_items]
            if batch_size is None:
                for item in items:
                    yield item
            else:
                yield items
            if "next" in response.links:
                url = response.links["next"]["url"]
                final_params = None
            else:
                break


@dataclasses.dataclass
class RequestHistory:
    request: httpx.Request
    response: httpx.Response | None

    @staticmethod
    def _header_to_str(key: str, value: str) -> str:
        if key == "authorization":
            value = "*****"
        return f'"{key}: {value}"'

    async def to_curl_request(self) -> str:
        headers = [self._header_to_str(k, v) for k, v in self.request.headers.items()]
        headers_str = " -H ".join(headers)
        body = await self.request.aread()
        if body:
            body_str = f"-d '{body.decode() if isinstance(body, bytes) else body}'"
        return f"curl -X {self.request.method} -H {headers_str} {body_str} {self.request.url}"

    async def to_curl_response(self) -> str:
        if self.response is None:
            return "<no response>"

        host = self.request.url.netloc.decode()
        headers = [self._header_to_str(k, v) for k, v in self.response.headers.items()]
        headers_str = "\n< ".join(headers)
        body = (await self.response.aread()).decode()
        return f"""< {self.response.http_version} {self.response.status_code}
< {headers_str}
<
* Connection #0 to host {host} left intact
{body}
"""


class AsyncGithubInstallationClient(AsyncGithubClient):
    auth: (GithubAppInstallationAuth | GithubTokenAuth)

    def __init__(
        self,
        auth: (GithubAppInstallationAuth | GithubTokenAuth),
        extra_metrics: bool = False,
    ) -> None:
        self._requests: list[RequestHistory] = []
        self._extra_metrics = extra_metrics
        super().__init__(auth=auth)

    def enable_extra_metrics(self) -> None:
        self._extra_metrics = True

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
            response = await super().send(
                request, auth=auth, follow_redirects=follow_redirects
            )
            _check_rate_limit(response)
        finally:
            if response is None:
                status_code = "error"
            else:
                status_code = str(response.status_code)
            statsd.increment(
                "http.client.requests",
                tags=[f"hostname:{self.base_url.host}", f"status_code:{status_code}"],
            )
            self._requests.append(RequestHistory(request, response))

        return response

    @property
    def last_request(self) -> RequestHistory:
        return self._requests[-1]

    async def aclose(self) -> None:
        await super().aclose()
        self._generate_metrics()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: types.TracebackType | None = None,
    ) -> None:
        await super().__aexit__(exc_type, exc_value, traceback)
        self._generate_metrics()

    def _generate_metrics(self) -> None:
        nb_requests = len(self._requests)
        gh_owner = http.extract_organization_login(self)

        tags = [f"hostname:{self.base_url.host}"]
        if self._extra_metrics:
            tags.append(f"gh_owner:{gh_owner}")

        statsd.histogram(
            "http.client.session",
            nb_requests,
            tags=tags,
        )

        request_with_ratelimit = first.first(
            r
            for r in self._requests
            if r.response and "x-ratelimit-remaining" in r.response.headers
        )
        if (
            self._extra_metrics
            and request_with_ratelimit is not None
            and request_with_ratelimit.response is not None
        ):
            statsd.histogram(
                "http.client.ratelimit.limit",
                int(request_with_ratelimit.response.headers["x-ratelimit-limit"]),
                tags=tags,
            )
            statsd.histogram(
                "http.client.ratelimit.remaining",
                int(request_with_ratelimit.response.headers["x-ratelimit-remaining"]),
                tags=tags,
            )

        self._requests = []

    async def get_access_token(self) -> str:
        token = self.auth.get_access_token()
        if token:
            return token

        # Token has expired, get a new one
        await self.get("/")
        token = self.auth.get_access_token()
        if token:
            return token

        raise RuntimeError("get_access_token() call on an unused client")


def aget_client(
    installation: github_types.GitHubInstallation, extra_metrics: bool = False
) -> AsyncGithubInstallationClient:
    return AsyncGithubInstallationClient(
        GithubAppInstallationAuth(installation), extra_metrics=extra_metrics
    )


@dataclasses.dataclass
class GitHubAppInfo:
    _app: typing.ClassVar[github_types.GitHubApp | None] = None
    _bot: typing.ClassVar[github_types.GitHubAccount | None] = None

    _redis_bot_key: typing.ClassVar[str] = "github-info-bot"
    _redis_app_key: typing.ClassVar[str] = "github-info-app"

    EXPIRATION_CACHE: int = int(datetime.timedelta(days=1).total_seconds())

    @staticmethod
    async def _get_random_installation_auths() -> abc.AsyncGenerator[
        GithubAppInstallationAuth, None
    ]:
        # NOTE(sileht): we can't query the /users endpoint with the
        # JWT, so just pick a random installation.
        async with AsyncGithubClient(auth=github_app.GithubBearerAuth()) as client:
            async for installations in typing.cast(
                abc.AsyncGenerator[list[github_types.GitHubInstallation], None],
                client.items(
                    "/app/installations",
                    page_limit=None,
                    resource_name="installation",
                    batch_size=100,
                ),
            ):
                random.shuffle(installations)
                for installation in installations:
                    if installation["suspended_at"]:
                        continue
                    yield GithubAppInstallationAuth(installation)

        raise RuntimeError(
            "Can't find an installation that can retrieve the GitHubApp bot account"
        )

    @classmethod
    async def _fetch_bot_from_redis(
        cls, redis_cache: redis_utils.RedisCacheBytes
    ) -> github_types.GitHubAccount | None:
        raw: bytes | None = await redis_cache.get(cls._redis_bot_key)
        if raw is None:
            return None
        return typing.cast(github_types.GitHubAccount, msgpack.unpackb(raw))

    @classmethod
    async def _fetch_bot_from_github(
        cls, redis_cache: redis_utils.RedisCacheBytes
    ) -> github_types.GitHubAccount:
        app = await cls.get_app(redis_cache)
        async for auth in cls._get_random_installation_auths():
            async with AsyncGithubClient(auth=auth) as client_install:
                try:
                    bot = await client_install.item(f"/users/{app['slug']}[bot]")
                except (http.HTTPForbidden, http.HTTPUnauthorized):
                    continue
                else:
                    await redis_cache.setex(
                        cls._redis_bot_key,
                        cls.EXPIRATION_CACHE,
                        msgpack.packb(bot),
                    )
                    return typing.cast(github_types.GitHubAccount, bot)

        raise RuntimeError(
            "Can't find an installation that can retrieve the GitHubApp bot account"
        )

    @classmethod
    async def get_bot(
        cls, redis_cache: redis_utils.RedisCacheBytes
    ) -> github_types.GitHubAccount:
        if cls._bot is None:
            cls._bot = await cls._fetch_bot_from_redis(redis_cache)
        if cls._bot is None:
            cls._bot = await cls._fetch_bot_from_github(redis_cache)
        return cls._bot

    @classmethod
    async def _fetch_app_from_redis(
        cls, redis_cache: redis_utils.RedisCacheBytes
    ) -> github_types.GitHubApp | None:
        raw: bytes | None = await redis_cache.get(cls._redis_app_key)
        if raw is None:
            return None
        return typing.cast(github_types.GitHubApp, msgpack.unpackb(raw))

    @classmethod
    async def _fetch_app_from_github(
        cls, redis_cache: redis_utils.RedisCacheBytes
    ) -> github_types.GitHubApp:
        async with AsyncGithubClient(auth=github_app.GithubBearerAuth()) as client:
            app = await client.item("/app")

        await redis_cache.setex(
            cls._redis_app_key,
            cls.EXPIRATION_CACHE,
            msgpack.packb(app),
        )
        return typing.cast(github_types.GitHubApp, app)

    @classmethod
    async def get_app(
        cls, redis_cache: redis_utils.RedisCacheBytes
    ) -> github_types.GitHubApp:
        if cls._app is None:
            cls._app = await cls._fetch_app_from_redis(redis_cache)
        if cls._app is None:
            cls._app = await cls._fetch_app_from_github(redis_cache)
        return cls._app

    @classmethod
    async def warm_cache(cls, redis_cache: redis_utils.RedisCacheBytes) -> None:
        try:
            await cls.get_bot(redis_cache)
        except Exception:
            LOG.warning("fail to warm GitHubAppInfo cache")
