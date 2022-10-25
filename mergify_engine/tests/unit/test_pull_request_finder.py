import json
import os
import typing
from unittest import mock

from mergify_engine import config
from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import pull_request_finder
from mergify_engine import redis_utils


async def _do_test_event_to_pull_check_run(
    redis_links: redis_utils.RedisLinks,
    filename: str,
    expected_pulls: set[github_types.GitHubPullRequestNumber],
) -> None:
    with open(
        os.path.join(os.path.dirname(__file__), "events", filename),
    ) as f:
        data = json.loads(
            f.read()
            .replace("https://github.com", config.GITHUB_URL)
            .replace("https://api.github.com", config.GITHUB_REST_API_URL)
        )

    gh_owner = github_types.GitHubAccount(
        {
            "type": "User",
            "id": github_types.GitHubAccountIdType(12345),
            "login": github_types.GitHubLogin("CytopiaTeam"),
            "avatar_url": "",
        }
    )
    installation_json = github_types.GitHubInstallation(
        {
            "id": github_types.GitHubInstallationIdType(12345),
            "target_type": gh_owner["type"],
            "permissions": {},
            "account": gh_owner,
        }
    )
    client = mock.Mock()
    client.item = mock.AsyncMock(return_value=[])
    installation = context.Installation(
        installation_json, mock.Mock(), client, redis_links
    )
    pulls_finder = pull_request_finder.PullRequestFinder(installation)
    pulls = await pulls_finder.extract_pull_numbers_from_event(
        github_types.GitHubRepositoryIdType(
            130312164,
        ),
        "check_run",
        data,
    )
    assert pulls == expected_pulls


async def test_event_to_pull_check_run_forked_repo(
    redis_links: redis_utils.RedisLinks,
) -> None:
    await _do_test_event_to_pull_check_run(
        redis_links,
        "check_run.event_from_forked_repo.json",
        typing.cast(set[github_types.GitHubPullRequestNumber], set()),
    )


async def test_event_to_pull_check_run_same_repo(
    redis_links: redis_utils.RedisLinks,
) -> None:
    await _do_test_event_to_pull_check_run(
        redis_links,
        "check_run.event_from_same_repo.json",
        {github_types.GitHubPullRequestNumber(409)},
    )
