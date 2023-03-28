import datetime
import email.utils
import typing
from unittest import mock
from urllib import parse

import httpx
import pytest
import respx

from mergify_engine import date
from mergify_engine import exceptions
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import github
from mergify_engine.clients import github_app
from mergify_engine.clients import http


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_installation_token_with_owner_id(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("/user/12345/installation").respond(
        200,
        json={
            "id": 12345,
            "target_type": "User",
            "permissions": {
                "checks": "write",
                "contents": "write",
                "pull_requests": "write",
            },
            "account": {"login": "testing", "id": 12345},
        },
    )

    respx_mock.post("/app/installations/12345/access_tokens").respond(
        200,
        json={"token": "<installation-token>", "expires_at": "2100-12-31T23:59:59Z"},
    )

    respx_mock.get(
        "/", headers__contains={"Authorization": "token <installation-token>"}
    ).respond(200, json={"work": True})

    installation_json = await github.get_installation_from_account_id(
        github_types.GitHubAccountIdType(12345)
    )
    async with github.AsyncGithubInstallationClient(
        github.GithubAppInstallationAuth(installation_json),
    ) as client:
        client.retry_exponential_multiplier = 0
        ret = await client.get("/")
        assert ret.json()["work"]
        assert isinstance(client.auth, github.GithubAppInstallationAuth)
        assert client.auth._installation is not None
        assert client.auth._installation["account"]["login"] == "testing"
        assert client.auth._installation["account"]["id"] == 12345


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_user_token(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/user/12345/installation").respond(
        200,
        json={
            "id": 12345,
            "target_type": "User",
            "permissions": {
                "checks": "write",
                "contents": "write",
                "pull_requests": "write",
            },
            "account": {"login": "testing", "id": 12345},
        },
    )

    respx_mock.get(
        "/", headers__contains={"Authorization": "token <user-token>"}
    ).respond(200, json={"work": True})

    installation_json = await github.get_installation_from_account_id(
        github_types.GitHubAccountIdType(12345)
    )
    async with github.AsyncGithubInstallationClient(
        github.GithubAppInstallationAuth(installation_json)
    ) as client:
        client.retry_exponential_multiplier = 0
        ret = await client.get(
            "/",
            oauth_token=github_types.GitHubOAuthToken("<user-token>"),
        )
        assert ret.json()["work"]


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_401_raise_ratelimit(respx_mock: respx.MockRouter) -> None:
    owner_id = github_types.GitHubAccountIdType(12345)
    owner_login = github_types.GitHubLogin("owner")
    repo = "repo"

    respx_mock.get("/user/12345/installation").respond(
        200,
        json={
            "id": 12345,
            "target_type": "User",
            "permissions": {
                "checks": "write",
                "contents": "write",
                "pull_requests": "write",
            },
            "account": {"login": "testing", "id": 12345},
        },
    )

    respx_mock.post("/app/installations/12345/access_tokens").respond(
        200,
        json={"token": "<token>", "expires_at": "2100-12-31T23:59:59Z"},
        headers={
            "X-RateLimit-Remaining": "5000",
            "X-RateLimit-Reset": "1234567890",
        },
    )

    respx_mock.get("/repos/owner/repo/pull/1").respond(
        403,
        json={"message": "quota !"},
        headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "1234567890"},
    )

    installation_json = await github.get_installation_from_account_id(owner_id)
    async with github.aget_client(installation_json) as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(exceptions.RateLimited):
            await client.item(f"/repos/{owner_login}/{repo}/pull/1")


async def test_client_HTTP_400(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("https://foobar/").respond(
        400, json={"message": "This is a 4XX error"}
    )

    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPClientSideError) as exc_info:
            await client.get("https://foobar/")

    assert exc_info.value.message == "This is a 4XX error"
    assert exc_info.value.status_code == 400
    assert exc_info.value.response.status_code == 400
    assert str(exc_info.value.request.url) == "https://foobar/"


async def test_message_format_client_HTTP_400(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("https://foobar/").respond(
        400, json={"message": "This is a 4XX error", "documentation_url": "fake_url"}
    )
    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPClientSideError) as exc_info:
            await client.get("https://foobar/")

    assert exc_info.value.message == "This is a 4XX error"

    respx_mock.get("https://foobar/").respond(
        400,
        json={
            "message": "error message",
            "errors": ["This is a 4XX error"],
            "documentation_url": "fake_url",
        },
    )
    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPClientSideError) as exc_info:
            await client.get("https://foobar/")

    assert exc_info.value.message == "This is a 4XX error"

    respx_mock.get("https://foobar/").respond(
        400,
        json={
            "message": "This is a 4XX error",
            "errors": [{"resource": "test", "field": "test", "code": "test"}],
            "documentation_url": "fake_url",
        },
    )
    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPClientSideError) as exc_info:
            await client.get("https://foobar/")

    assert exc_info.value.message == "This is a 4XX error"

    respx_mock.get("https://foobar/").respond(
        400,
        json={
            "message": "error message",
            "errors": [
                {
                    "resource": "test",
                    "code": "test",
                    "field": "test",
                    "message": "This is a 4XX error",
                }
            ],
            "documentation_url": "fake_url",
        },
    )
    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPClientSideError) as exc_info:
            await client.get("https://foobar/")

    assert exc_info.value.message == "This is a 4XX error"

    respx_mock.get("https://foobar/").respond(
        400,
        json={
            "not_message_key": "false_key",
            "documentation_url": "fake_url",
        },
    )
    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPClientSideError) as exc_info:
            await client.get("https://foobar/")

    assert exc_info.value.message == "No error message provided by GitHub"


async def test_client_HTTP_500(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("https://foobar/").respond(500, text="This is a 5XX error")

    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPServerSideError) as exc_info:
            await client.get("https://foobar/")

    assert exc_info.value.message == "This is a 5XX error"
    assert exc_info.value.status_code == 500
    assert exc_info.value.response.status_code == 500
    assert str(exc_info.value.request.url) == "https://foobar/"


@pytest.mark.respx(base_url="https://foobar/")
async def test_client_temporary_HTTP_500(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/").mock(
        side_effect=[
            httpx.Response(500, text="This is a 5XX error"),
            httpx.Response(500, text="This is a 5XX error"),
            httpx.Response(500, text="This is a 5XX error"),
            httpx.Response(200, text="It works now !"),
        ]
    )

    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        await client.get("https://foobar/")


async def test_client_connection_error() -> None:
    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.RequestError):
            await client.get("http://localhost:12345")


@pytest.mark.respx(base_url="https://foobar/")
async def _do_test_client_retry_429(
    respx_mock: respx.MockRouter, retry_after: str
) -> datetime.datetime:
    records: list[datetime.datetime] = []

    def record_date(_: typing.Any) -> httpx.Response:
        if records:
            records.append(date.utcnow())
            return httpx.Response(200, text="It works now !")
        else:
            records.append(date.utcnow())
            return httpx.Response(
                429,
                text="This is a 429 error",
                headers={"Retry-After": retry_after},
            )

    respx_mock.get("/").mock(side_effect=record_date)

    async with http.AsyncClient() as client:
        client.retry_exponential_multiplier = 0
        await client.get("https://foobar/")

    return records[1]


async def test_client_retry_429_retry_after_as_seconds(
    respx_mock: respx.MockRouter,
) -> None:
    now = date.utcnow()
    when = await _do_test_client_retry_429(respx_mock, "1")
    elapsed_seconds = (when - now).total_seconds()
    assert 0.97 < elapsed_seconds <= 1.5


async def test_client_retry_429_retry_after_as_absolute_date(
    respx_mock: respx.MockRouter,
) -> None:
    expected_retry = date.utcnow() + datetime.timedelta(seconds=2)
    retry_after = email.utils.format_datetime(expected_retry)
    when = await _do_test_client_retry_429(respx_mock, retry_after)
    # ms are cut by http_date, so we allow a 1 second delta :(
    assert when >= expected_retry - datetime.timedelta(seconds=1)


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_access_token_HTTP_500(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/user/12345/installation").respond(
        200,
        json={
            "id": 12345,
            "target_type": "User",
            "permissions": {
                "checks": "write",
                "contents": "write",
                "pull_requests": "write",
            },
            "account": {"login": "testing", "id": 12345},
        },
    )
    retries = [0]

    def error_500_tracker(_: typing.Any) -> httpx.Response:
        retries[0] += 1
        return httpx.Response(500, text="This is a 5XX error")

    respx_mock.post("/app/installations/12345/access_tokens").mock(
        side_effect=error_500_tracker
    )

    installation_json = await github.get_installation_from_account_id(
        github_types.GitHubAccountIdType(12345)
    )
    async with github.AsyncGithubInstallationClient(
        github.GithubAppInstallationAuth(installation_json)
    ) as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPServerSideError) as exc_info:
            await client.get("/")

    assert exc_info.value.message == "This is a 5XX error"
    assert exc_info.value.status_code == 500
    assert exc_info.value.response.status_code == 500
    assert (
        str(exc_info.value.request.url)
        == f"{settings.GITHUB_REST_API_URL}/app/installations/12345/access_tokens"
    )


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_installation_HTTP_500(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/user/12345/installation").mock(
        side_effect=[
            httpx.Response(500, text="This is a 5XX error"),
            httpx.Response(500, text="This is a 5XX error"),
            httpx.Response(500, text="This is a 5XX error"),
            httpx.Response(500, text="This is a 5XX error"),
            httpx.Response(500, text="This is a 5XX error"),
        ]
    )

    real_init = github.AsyncGithubClient.__init__

    def mocked_init(
        self: github.AsyncGithubClient,
        auth: (
            github_app.GithubBearerAuth
            | github.GithubAppInstallationAuth
            | github.GithubTokenAuth
        ),
    ) -> None:
        real_init(self, auth)
        self.retry_exponential_multiplier = 0

    with mock.patch.object(github.AsyncGithubClient, "__init__", mocked_init):
        with pytest.raises(http.HTTPServerSideError) as exc_info:
            await github.get_installation_from_account_id(
                github_types.GitHubAccountIdType(12345)
            )

    assert exc_info.value.message == "This is a 5XX error"
    assert exc_info.value.status_code == 500
    assert exc_info.value.response.status_code == 500
    assert (
        str(exc_info.value.request.url)
        == f"{settings.GITHUB_REST_API_URL}/user/12345/installation"
    )


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_installation_HTTP_404(respx_mock: respx.MockRouter) -> None:
    respx_mock.get("/user/12345/installation").respond(
        404, json={"message": "Repository not found"}
    )

    with pytest.raises(exceptions.MergifyNotInstalled):
        await github.get_installation_from_account_id(
            github_types.GitHubAccountIdType(12345)
        )


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_installation_HTTP_301(respx_mock: respx.MockRouter) -> None:
    url_prefix = parse.urlparse(settings.GITHUB_REST_API_URL).path
    respx_mock.get("/user/12345/installation").respond(
        301,
        headers={"Location": f"{url_prefix}/repositories/12345/installation"},
    )

    respx_mock.get("/repositories/12345/installation").respond(
        404, json={"message": "Repository not found"}
    )
    with pytest.raises(exceptions.MergifyNotInstalled):
        await github.get_installation_from_account_id(
            github_types.GitHubAccountIdType(12345)
        )


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_client_abuse_403_no_header(respx_mock: respx.MockRouter) -> None:
    abuse_message = (
        "You have triggered an abuse detection mechanism. "
        "Please wait a few minutes before you try again."
    )
    respx_mock.get("/user/12345/installation").respond(
        200,
        json={
            "id": 12345,
            "target_type": "User",
            "permissions": {
                "checks": "write",
                "contents": "write",
                "pull_requests": "write",
            },
            "account": {"login": "testing", "id": 12345},
        },
    )
    respx_mock.post("/app/installations/12345/access_tokens").respond(
        200, json={"token": "<token>", "expires_at": "2100-12-31T23:59:59Z"}
    )
    respx_mock.get("/").respond(
        403,
        json={"message": abuse_message},
    )

    installation_json = await github.get_installation_from_account_id(
        github_types.GitHubAccountIdType(12345)
    )
    async with github.AsyncGithubInstallationClient(
        github.GithubAppInstallationAuth(installation_json)
    ) as client:
        client.retry_exponential_multiplier = 0
        with pytest.raises(http.HTTPClientSideError) as exc_info:
            await client.get("/")

    assert exc_info.value.message == abuse_message
    assert exc_info.value.status_code == 403
    assert exc_info.value.response.status_code == 403
    assert str(exc_info.value.request.url) == f"{settings.GITHUB_REST_API_URL}/"


@mock.patch.object(github.CachedToken, "STORAGE", {})
@pytest.mark.respx(base_url=settings.GITHUB_REST_API_URL)
async def test_to_curl(
    respx_mock: respx.MockRouter,
) -> None:
    respx_mock.get("/user/12345/installation").respond(
        200,
        json={
            "id": 12345,
            "target_type": "User",
            "permissions": {
                "checks": "write",
                "contents": "write",
                "pull_requests": "write",
            },
            "account": {"login": "testing", "id": 12345},
        },
    )

    respx_mock.post("/app/installations/12345/access_tokens").respond(
        200,
        json={"token": "<installation-token>", "expires_at": "2100-12-31T23:59:59Z"},
    )

    respx_mock.post(
        "/",
        headers__contains={"Authorization": "token <installation-token>", "Foo": "Bar"},
    ).respond(200, json={"work": True})

    installation_json = await github.get_installation_from_account_id(
        github_types.GitHubAccountIdType(12345)
    )
    async with github.AsyncGithubInstallationClient(
        github.GithubAppInstallationAuth(installation_json)
    ) as client:
        client.retry_exponential_multiplier = 0
        await client.post("/", headers={"Foo": "Bar"}, json={"ask": "What?"})
        assert await client.last_request.to_curl_request() == (
            "curl -X POST "
            '-H "host: api.github.com" '
            '-H "accept-encoding: gzip, deflate" '
            '-H "connection: keep-alive" '
            f'-H "user-agent: mergify-engine/unknown python/{http.PYTHON_VERSION} httpx/{http.HTTPX_VERSION}" '
            '-H "accept: application/vnd.github.machine-man-preview+json" '
            '-H "foo: Bar" '
            '-H "content-length: 16" '
            '-H "content-type: application/json" '
            '-H "authorization: *****" '
            """-d '{"ask": "What?"}' """  # noqa: FS003
            "https://api.github.com/"
        )
        assert (
            await client.last_request.to_curl_response()
            == """< HTTP/1.1 200
< "content-length: 14"
< "content-type: application/json"
<
* Connection #0 to host api.github.com left intact
{"work": true}
"""
        )
