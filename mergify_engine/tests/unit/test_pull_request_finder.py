import json
import pathlib
import typing
from unittest import mock

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import pull_request_finder
from mergify_engine import redis_utils
from mergify_engine import settings
from mergify_engine.tests.unit import conftest


async def _do_test_event_to_pull(
    redis_links: redis_utils.RedisLinks,
    event_type: github_types.GitHubEventType,
    filename: str,
    expected_pulls: set[github_types.GitHubPullRequestNumber],
    mocked_pulls: list[github_types.GitHubPullRequest] | None = None,
) -> None:
    with (pathlib.Path(__file__).parent / "events" / filename).open() as f:
        data = json.loads(
            f.read()
            .replace("https://github.com", settings.GITHUB_URL)
            .replace("https://api.github.com", settings.GITHUB_REST_API_URL),
        )

    gh_owner = github_types.GitHubAccount(
        {
            "type": "User",
            "id": github_types.GitHubAccountIdType(12345),
            "login": github_types.GitHubLogin("CytopiaTeam"),
            "avatar_url": "",
        },
    )
    installation_json = github_types.GitHubInstallation(
        {
            "id": github_types.GitHubInstallationIdType(12345),
            "target_type": gh_owner["type"],
            "permissions": {},
            "account": gh_owner,
            "suspended_at": None,
        },
    )
    client = mock.Mock()
    items_mock = mock.MagicMock()
    items_mock.__aiter__.return_value = iter(mocked_pulls or [])
    client.items.return_value = items_mock
    installation = context.Installation(
        installation_json,
        mock.Mock(),
        client,
        redis_links,
    )
    pulls_finder = pull_request_finder.PullRequestFinder(installation)
    pulls = await pulls_finder.extract_pull_numbers_from_event(
        github_types.GitHubRepositoryIdType(
            130312164,
        ),
        event_type,
        data,
    )
    assert pulls == expected_pulls


async def test_event_to_pull_check_run_forked_repo(
    redis_links: redis_utils.RedisLinks,
) -> None:
    await _do_test_event_to_pull(
        redis_links,
        "check_run",
        "check_run.event_from_forked_repo.json",
        typing.cast(set[github_types.GitHubPullRequestNumber], set()),
    )


async def test_event_to_pull_check_run_same_repo(
    redis_links: redis_utils.RedisLinks,
) -> None:
    await _do_test_event_to_pull(
        redis_links,
        "check_run",
        "check_run.event_from_same_repo.json",
        {github_types.GitHubPullRequestNumber(409)},
    )


async def test_event_status(
    redis_links: redis_utils.RedisLinks,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    await _do_test_event_to_pull(redis_links, "status", "status.json", set())

    ctxt = await context_getter(409)
    ctxt.pull["head"]["sha"] = github_types.SHAType(
        "dcdb7375b887ab3094eb6c1555f26c7090809c89",
    )

    ctxt.pull["base"]["repo"]["id"] = github_types.GitHubRepositoryIdType(
        130312164,
    )
    await pull_request_finder.PullRequestFinder.sync(redis_links.cache, ctxt.pull)

    await _do_test_event_to_pull(
        redis_links,
        "status",
        "status.json",
        {github_types.GitHubPullRequestNumber(409)},
    )


async def test_fetch_open_pull_requests_fallback(
    redis_links: redis_utils.RedisLinks,
) -> None:
    await _do_test_event_to_pull(
        redis_links,
        "status",
        "status.json",
        {github_types.GitHubPullRequestNumber(409)},
        mocked_pulls=[
            {  # type: ignore [typeddict-item]
                "number": 409,
                "head": {"sha": "dcdb7375b887ab3094eb6c1555f26c7090809c89"},
                "base": {"ref": "some-ref"},
            },
        ],
    )
