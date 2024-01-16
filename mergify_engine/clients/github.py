from __future__ import annotations

from collections import abc
import contextlib
import dataclasses
import datetime
import json
import random
import typing
from urllib import parse

import daiquiri
from datadog import statsd  # type: ignore[attr-defined]
import first
import httpx
import msgpack
import typing_extensions

from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import logs
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.clients import github_app
from mergify_engine.clients import http


if typing.TYPE_CHECKING:
    import uuid


RATE_LIMIT_THRESHOLD = 20
LOGGING_REQUESTS_THRESHOLD = 40
LOGGING_REQUESTS_THRESHOLD_ABSOLUTE = 400

LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class TooManyPages(exceptions.UnprocessablePullRequestError):
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
        dict[github_types.GitHubInstallationIdType, CachedToken]
    ] = {}

    installation_id: github_types.GitHubInstallationIdType
    token: str
    expiration: datetime.datetime

    def __post_init__(self) -> None:
        CachedToken.STORAGE[self.installation_id] = self

    @classmethod
    def get(
        cls,
        installation_id: github_types.GitHubInstallationIdType,
    ) -> CachedToken | None:
        return cls.STORAGE.get(installation_id)

    def invalidate(self) -> None:
        CachedToken.STORAGE.pop(self.installation_id, None)


class InstallationInaccessibleError(Exception):
    message: str


class Actor(typing_extensions.TypedDict):
    type: typing.Literal["user", "application"]
    id: int | uuid.UUID
    name: str


class GitHubTokenAuth(httpx.Auth):
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
        self,
        request: httpx.Request,
    ) -> abc.Generator[httpx.Request, httpx.Response, None]:
        request.headers["Authorization"] = f"token {self._token}"
        yield request

    def build_request(self, method: str, url: str) -> httpx.Request:
        headers = http.DEFAULT_HEADERS.copy()
        headers["Authorization"] = f"token {self._token}"
        return httpx.Request(method, url, headers=headers)

    def get_access_token(self) -> str:
        return self._token


class GitHubAppInstallationAuth(httpx.Auth):
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
        self,
        request: httpx.Request,
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
                raise exceptions.MergifyNotInstalledError()
            elif auth_response.status_code == 403:
                error_message = auth_response.json()["message"]
                if "This installation has been suspended" in error_message:
                    LOG.debug(
                        "Mergify installation suspended",
                        gh_owner=self._owner_login,
                        error_message=error_message,
                    )
                    raise exceptions.MergifyNotInstalledError()

            http.raise_for_status(auth_response)
            token = self._set_access_token(auth_response.json())

        request.headers["Authorization"] = f"token {token}"
        yield request

    def build_access_token_request(self, force: bool = False) -> httpx.Request:
        if self._installation is None:
            raise RuntimeError("No installation")

        return self.build_github_app_request(
            "POST",
            f"{settings.GITHUB_REST_API_URL}/app/installations/{self._installation['id']}/access_tokens",
            force=force,
        )

    def build_github_app_request(
        self,
        method: str,
        url: str,
        force: bool = False,
    ) -> httpx.Request:
        headers = http.DEFAULT_HEADERS.copy()
        headers["Authorization"] = f"Bearer {github_app.get_or_create_jwt(force)}"
        return httpx.Request(method, url, headers=headers)

    def _set_access_token(
        self,
        data: github_types.GitHubInstallationAccessToken,
    ) -> str:
        if self._installation is None:
            raise RuntimeError("Cannot set access token, no installation")

        self._cached_token = CachedToken(
            self._installation["id"],
            data["token"],
            datetime.datetime.fromisoformat(data["expires_at"]),
        )
        LOG.debug(
            "New token acquired",
            gh_owner=self._owner_login,
            expire_at=self._cached_token.expiration,
        )
        return self._cached_token.token

    def get_access_token(self) -> str | None:
        if not self._cached_token:
            return None

        if self._cached_token.expiration <= date.utcnow():
            LOG.debug(
                "Token expired",
                gh_owner=self._owner_login,
                expire_at=self._cached_token.expiration,
            )
            self._cached_token.invalidate()
            self._cached_token = None
            return None

        return self._cached_token.token


async def _get_installation(
    endpoint: str,
    logging_extras: dict[str, typing.Any] | None = None,
) -> github_types.GitHubInstallation:
    if logging_extras is None:
        logging_extras = {}

    async with AsyncGitHubClient(auth=github_app.GitHubBearerAuth()) as client:
        try:
            installation = typing.cast(
                github_types.GitHubInstallation,
                await client.item(f"{settings.GITHUB_REST_API_URL}{endpoint}"),
            )
        except http.HTTPNotFoundError as e:
            LOG.debug(
                "Mergify not installed",
                error_message=e.message,
                **logging_extras,
            )
            raise exceptions.MergifyNotInstalledError()

    if installation["suspended_at"]:
        raise exceptions.MergifySuspendedError()
    return installation


async def get_installation_from_account_id(
    account_id: github_types.GitHubAccountIdType,
) -> github_types.GitHubInstallation:
    return await _get_installation(
        f"/user/{account_id}/installation",
        {"gh_owner_id": account_id},
    )


async def get_installation_from_repository(
    login: github_types.GitHubLogin,
    repository: github_types.GitHubRepositoryName,
) -> github_types.GitHubInstallation:
    return await _get_installation(
        f"/repos/{login}/{repository}/installation",
        {"gh_owner": login, "gh_repo": repository},
    )


async def get_installation_from_login(
    login: github_types.GitHubLogin,
) -> github_types.GitHubInstallation:
    return await _get_installation(
        f"/users/{login}/installation",
        {"gh_owner": login},
    )


def _check_rate_limit(client: http.AsyncClient, response: httpx.Response) -> None:
    if response.status_code not in {403, 422, 429} or (
        response.status_code == 422
        and (
            len(response.json().get("errors", [])) != 1
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
                gh_owner=http.extract_organization_login(client),
            )
            tags = [f"hostname:{response.url.host}"]
            worker_id = logs.WORKER_ID.get(None)
            if worker_id is not None:
                tags.append(f"worker_id:{worker_id}")
            statsd.increment(
                "http.client.rate_limited",
                tags=tags,
            )
        raise exceptions.RateLimitedError(delta, remaining)

    if (
        response.status_code == 403
        and "You have exceeded a secondary rate limit" in http.extract_message(response)
    ):
        raise exceptions.SecondaryRateLimitedError(datetime.timedelta(seconds=30), 0)


class AsyncGitHubClient(http.AsyncClient):
    auth: (github_app.GitHubBearerAuth | GitHubAppInstallationAuth | GitHubTokenAuth)

    def __init__(
        self,
        auth: (
            github_app.GitHubBearerAuth | GitHubAppInstallationAuth | GitHubTokenAuth
        ),
        retry_stop_after_attempt: int | None = None,
        retry_exponential_multiplier: float | None = None,
    ) -> None:
        super().__init__(
            base_url=settings.GITHUB_REST_API_URL,
            auth=auth,
            headers={"Accept": "application/vnd.github.machine-man-preview+json"},
            retry_stop_after_attempt=retry_stop_after_attempt,
            retry_exponential_multiplier=retry_exponential_multiplier,
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
            if isinstance(self.auth, github_app.GitHubBearerAuth):
                raise TypeError(
                    "oauth_token is not supported for GitHubBearerAuth auth",
                )
            kwargs["auth"] = GitHubTokenAuth(oauth_token)
        return kwargs

    async def get(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().get(
            url,
            **self._prepare_request_kwargs(api_version, oauth_token, **kwargs),
        )

    async def post(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().post(
            url,
            **self._prepare_request_kwargs(api_version, oauth_token, **kwargs),
        )

    async def put(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().put(
            url,
            **self._prepare_request_kwargs(api_version, oauth_token, **kwargs),
        )

    async def patch(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().patch(
            url,
            **self._prepare_request_kwargs(api_version, oauth_token, **kwargs),
        )

    async def head(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().head(
            url,
            **self._prepare_request_kwargs(api_version, oauth_token, **kwargs),
        )

    async def delete(  # type: ignore[override]
        self,
        url: str,
        api_version: github_types.GitHubApiVersion | None = None,
        oauth_token: github_types.GitHubOAuthToken | None = None,
        **kwargs: typing.Any,
    ) -> httpx.Response:
        return await super().delete(
            url,
            **self._prepare_request_kwargs(api_version, oauth_token, **kwargs),
        )

    async def graphql_post(
        self,
        query: str,
        oauth_token: github_types.GitHubOAuthToken | None = None,
    ) -> typing.Any:
        response = await self.post(
            settings.GITHUB_GRAPHQL_API_URL,
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
        extensions: dict[str, typing.Any] | None = None,
    ) -> typing.Any:
        response = await self.get(
            url,
            api_version=api_version,
            oauth_token=oauth_token,
            params=params,
            extensions=extensions,
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
        extensions: dict[str, typing.Any] | None = None,
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
                extensions=extensions,
            )
            last_url = response.links.get("last", {}).get("url")
            if last_url:
                last_page = int(
                    parse.parse_qs(parse.urlparse(last_url).query)["page"][0],
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


class AsyncGitHubInstallationClient(AsyncGitHubClient):
    auth: (GitHubAppInstallationAuth | GitHubTokenAuth)

    def __init__(
        self,
        auth: (GitHubAppInstallationAuth | GitHubTokenAuth),
        extra_metrics: bool = False,
        retry_stop_after_attempt: int | None = None,
        retry_exponential_multiplier: float | None = None,
    ) -> None:
        self._extra_metrics = extra_metrics
        super().__init__(
            auth=auth,
            retry_stop_after_attempt=retry_stop_after_attempt,
            retry_exponential_multiplier=retry_exponential_multiplier,
        )

    def enable_extra_metrics(self) -> None:
        self._extra_metrics = True

    async def send(
        self,
        request: httpx.Request,
        *,
        stream: bool = False,  # noqa: ARG002
        auth: httpx._types.AuthTypes
        | httpx._client.UseClientDefault
        | None = httpx._client.USE_CLIENT_DEFAULT,
        follow_redirects: bool
        | httpx._client.UseClientDefault = httpx._client.USE_CLIENT_DEFAULT,
    ) -> httpx.Response:
        response = await super().send(
            request,
            auth=auth,
            follow_redirects=follow_redirects,
        )
        _check_rate_limit(self, response)
        return response

    def _get_datadog_tags(self) -> list[str]:
        tags = super()._get_datadog_tags()
        if self._extra_metrics:
            gh_owner = http.extract_organization_login(self)
            tags.append(f"gh_owner:{gh_owner}")
            worker_id = logs.WORKER_ID.get(None)
            if worker_id is not None:
                tags.append(f"worker_id:{worker_id}")
            worker_task = logs.WORKER_TASK.get(None)
            if worker_task is not None:
                tags.append(f"worker_task:{worker_task}")
        return tags

    def _generate_metrics(self) -> None:
        super()._generate_metrics()
        tags = self._get_datadog_tags()

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
    installation: github_types.GitHubInstallation,
    extra_metrics: bool = False,
) -> AsyncGitHubInstallationClient:
    return AsyncGitHubInstallationClient(
        GitHubAppInstallationAuth(installation),
        extra_metrics=extra_metrics,
    )


@dataclasses.dataclass
class GitHubAppInfo:
    _app: typing.ClassVar[github_types.GitHubApp | None] = None
    _bot: typing.ClassVar[github_types.GitHubAccount | None] = None

    _redis_bot_key: typing.ClassVar[str] = "github-info-bot"
    _redis_app_key: typing.ClassVar[str] = "github-info-app"

    EXPIRATION_CACHE: typing.ClassVar[int] = int(
        datetime.timedelta(days=1).total_seconds(),
    )

    @staticmethod
    async def _get_random_installation_auths() -> (
        abc.AsyncGenerator[GitHubAppInstallationAuth, None]
    ):
        # NOTE(sileht): we can't query the /users endpoint with the
        # JWT, so just pick a random installation.
        async with AsyncGitHubClient(auth=github_app.GitHubBearerAuth()) as client:
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
                    yield GitHubAppInstallationAuth(installation)

        raise RuntimeError(
            "Can't find an installation that can retrieve the GitHubApp bot account",
        )

    @classmethod
    async def _fetch_bot_from_redis(
        cls,
        redis_cache: redis_utils.RedisCache,
    ) -> github_types.GitHubAccount | None:
        raw: bytes | None = await redis_cache.get(cls._redis_bot_key)
        if raw is None:
            return None
        return typing.cast(github_types.GitHubAccount, msgpack.unpackb(raw))

    @classmethod
    async def _fetch_bot_from_github(
        cls,
        redis_cache: redis_utils.RedisCache,
    ) -> github_types.GitHubAccount:
        app = await cls.get_app(redis_cache)
        async for auth in cls._get_random_installation_auths():
            async with AsyncGitHubClient(auth=auth) as client_install:
                try:
                    bot = await client_install.item(f"/users/{app['slug']}[bot]")
                except (http.HTTPForbiddenError, http.HTTPUnauthorizedError):
                    continue
                else:
                    await redis_cache.setex(
                        cls._redis_bot_key,
                        cls.EXPIRATION_CACHE,
                        msgpack.packb(bot),
                    )
                    return typing.cast(github_types.GitHubAccount, bot)

        raise RuntimeError(
            "Can't find an installation that can retrieve the GitHubApp bot account",
        )

    @classmethod
    async def get_bot(
        cls,
        redis_cache: redis_utils.RedisCache,
    ) -> github_types.GitHubAccount:
        if cls._bot is None:
            cls._bot = await cls._fetch_bot_from_redis(redis_cache)
        if cls._bot is None:
            cls._bot = await cls._fetch_bot_from_github(redis_cache)
        return cls._bot

    @classmethod
    async def _fetch_app_from_redis(
        cls,
        redis_cache: redis_utils.RedisCache,
    ) -> github_types.GitHubApp | None:
        raw: bytes | None = await redis_cache.get(cls._redis_app_key)
        if raw is None:
            return None
        return typing.cast(github_types.GitHubApp, msgpack.unpackb(raw))

    @classmethod
    async def _fetch_app_from_github(
        cls,
        redis_cache: redis_utils.RedisCache,
    ) -> github_types.GitHubApp:
        async with AsyncGitHubClient(auth=github_app.GitHubBearerAuth()) as client:
            app = await client.item("/app")

        await redis_cache.setex(
            cls._redis_app_key,
            cls.EXPIRATION_CACHE,
            msgpack.packb(app),
        )
        return typing.cast(github_types.GitHubApp, app)

    @classmethod
    async def get_app(
        cls,
        redis_cache: redis_utils.RedisCache,
    ) -> github_types.GitHubApp:
        if cls._app is None:
            cls._app = await cls._fetch_app_from_redis(redis_cache)
        if cls._app is None:
            cls._app = await cls._fetch_app_from_github(redis_cache)
        return cls._app

    @classmethod
    async def warm_cache(cls, redis_cache: redis_utils.RedisCache) -> None:
        try:
            await cls.get_bot(redis_cache)
        except Exception:
            LOG.warning("fail to warm GitHubAppInfo cache")
