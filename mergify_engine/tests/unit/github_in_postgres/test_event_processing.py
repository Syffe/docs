import typing

import respx
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.github_in_postgres import process_events
from mergify_engine.models import github as gh_models
from mergify_engine.tests.unit import conftest


async def test_pull_request_event_with_missing_data(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    invalid_event = typing.cast(
        github_types.GitHubPullRequest,
        {
            "diff_url": "https://github.com/foo/bar/pull/123.diff",
            "html_url": "https://github.com/foo/bar/pull/123",
            "id": 1234567890,
            "issue_url": "https://api.github.com/repos/foo/bar/issues/123",
            "locked": False,
            "node_id": "foobarbarfoo",
            "number": 123,
            "patch_url": "https://github.com/foo/bar/pull/123.patch",
            "state": "closed",
            "url": "https://api.github.com/repos/foo/bar/pulls/123",
        },
    )

    await worker_pusher.push_github_in_pg_event(
        redis_links.stream,
        "pull_request",
        "12345",
        invalid_event,
    )

    await process_events.store_redis_events_in_pg(redis_links)

    pull_requests = list(await db.scalars(sqlalchemy.select(gh_models.PullRequest)))
    assert len(pull_requests) == 0


async def test_event_with_nul_bytes_in_body(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    context_getter: conftest.ContextGetterFixture,
    _mock_gh_pull_request_commits_insert_in_pg: None,
) -> None:
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(123))

    pull = ctxt.pull
    pull["body"] = "Test with nul \x00 bytes"
    await worker_pusher.push_github_in_pg_event(
        redis_links.stream,
        "pull_request",
        "12345",
        pull,
    )

    await process_events.store_redis_events_in_pg(redis_links)

    pull_requests = list(await db.scalars(sqlalchemy.select(gh_models.PullRequest)))
    assert len(pull_requests) == 1
    assert pull_requests[0].body is not None
    assert "\x00" not in pull_requests[0].body


async def test_pull_request_event_invalid_commits_from_http(
    redis_links: redis_utils.RedisLinks,
    respx_mock: respx.MockRouter,
    a_pull_request: github_types.GitHubPullRequest,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    respx_mock.get(
        "https://api.github.com/repos/user/repo/pulls/6/commits?per_page=100",
    ).respond(
        200,
        json=[
            {
                "author": None,
                "comments_url": "someurl",
                "commit": {
                    "author": None,
                    "comment_count": 0,
                    "committer": {
                        "date": "2023-11-28T09:15:11Z",
                        "email": "",
                        "name": "",
                    },
                },
            },
        ],
    )

    user = gh_models.GitHubUser(
        id=github_types.GitHubAccountIdType(0),
        login=github_types.GitHubLogin("user"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    respx_mock.get("https://api.github.com/users/user/installation").respond(
        200,
        json={
            "id": 1234,
            "account": user.to_github_account(),
            "suspended_at": None,
        },
    )
    respx_mock.post(
        "https://api.github.com/app/installations/1234/access_tokens",
    ).respond(
        200,
        json=github_types.GitHubInstallationAccessToken(
            {
                "token": "gh_token",
                "expires_at": "2111-09-08T17:26:27Z",
            },
        ),  # type: ignore[arg-type]
    )

    await worker_pusher.push_github_in_pg_event(
        redis_links.stream,
        "pull_request",
        "12345",
        a_pull_request,
    )
    await process_events.store_redis_events_in_pg(redis_links)

    pull_requests = list(await db.scalars(sqlalchemy.select(gh_models.PullRequest)))
    assert len(pull_requests) == 1

    pull_request_commits = list(
        await db.scalars(sqlalchemy.select(gh_models.PullRequestCommit)),
    )
    assert len(pull_request_commits) == 0


async def test_pull_request_event_commits_endpoint_return_404(
    redis_links: redis_utils.RedisLinks,
    respx_mock: respx.MockRouter,
    a_pull_request: github_types.GitHubPullRequest,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    respx_mock.get(
        "https://api.github.com/repos/user/repo/pulls/6/commits?per_page=100",
    ).respond(404)

    user = gh_models.GitHubUser(
        id=github_types.GitHubAccountIdType(0),
        login=github_types.GitHubLogin("user"),
        oauth_access_token=github_types.GitHubOAuthToken("user-token"),
    )
    db.add(user)
    await db.commit()

    respx_mock.get("https://api.github.com/users/user/installation").respond(
        200,
        json={
            "id": 1234,
            "account": user.to_github_account(),
            "suspended_at": None,
        },
    )
    respx_mock.post(
        "https://api.github.com/app/installations/1234/access_tokens",
    ).respond(
        200,
        json=github_types.GitHubInstallationAccessToken(
            {
                "token": "gh_token",
                "expires_at": "2111-09-08T17:26:27Z",
            },
        ),  # type: ignore[arg-type]
    )

    await worker_pusher.push_github_in_pg_event(
        redis_links.stream,
        "pull_request",
        "12345",
        a_pull_request,
    )
    await process_events.store_redis_events_in_pg(redis_links)

    pull_requests = list(await db.scalars(sqlalchemy.select(gh_models.PullRequest)))
    assert len(pull_requests) == 1

    pull_request_commits = list(
        await db.scalars(sqlalchemy.select(gh_models.PullRequestCommit)),
    )
    assert len(pull_request_commits) == 0
