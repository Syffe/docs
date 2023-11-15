from unittest import mock

import pytest

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.clients import github_app
from mergify_engine.console_scripts import admin_cli
from mergify_engine.tests import utils


async def test_stream_status(
    redis_links: redis_utils.RedisLinks,
    setup_database: None,
) -> None:
    for installation_id in range(8):
        for pull_number in range(2):
            for data in range(3):
                owner = f"owner-{installation_id}"
                repo = f"repo-{installation_id}"
                await worker_pusher.push(
                    redis_links.stream,
                    github_types.GitHubAccountIdType(123),
                    github_types.GitHubLogin(owner),
                    github_types.GitHubRepositoryIdType(123),
                    github_types.GitHubRepositoryName(repo),
                    github_types.GitHubPullRequestNumber(pull_number),
                    "pull_request",
                    github_types.GitHubEvent({"payload": data}),  # type: ignore[typeddict-item]
                )

    result = utils.test_console_scripts(admin_cli.admin_cli, ["stream-status"])
    assert result.exit_code == 0
    assert result.output.endswith(" 123: 2 pull requests, 48 events\n")


@mock.patch("mergify_engine.worker.stream.subscription.Subscription.get_subscription")
@mock.patch("mergify_engine.clients.github.get_installation_from_account_id")
@mock.patch("mergify_engine.worker.stream.run_engine")
async def test_stream_reschedule(
    run_engine: mock.Mock,
    get_installation_from_account_id: mock.Mock,
    get_subscription: mock.AsyncMock,
    redis_links: redis_utils.RedisLinks,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    get_installation_from_account_id.return_value = {
        "id": github_types.GitHubInstallationIdType(12345),
        "account": {
            "id": "123",
            "login": github_types.GitHubLogin("owner-123"),
            "type": "User",
            "avatar_url": "",
        },
        "target_type": "Organization",
        "permissions": github_app.EXPECTED_MINIMAL_PERMISSIONS["Organization"],
        "suspended_at": None,
    }

    monkeypatch.setattr("mergify_engine.worker_pusher.WORKER_PROCESSING_DELAY", 3000)
    await worker_pusher.push(
        redis_links.stream,
        github_types.GitHubAccountIdType(123),
        github_types.GitHubLogin("owner-123"),
        github_types.GitHubRepositoryIdType(123),
        github_types.GitHubRepositoryName("repo"),
        github_types.GitHubPullRequestNumber(123),
        "pull_request",
        github_types.GitHubEvent({"payload": "whatever"}),  # type: ignore[typeddict-item]
    )

    score = (await redis_links.stream.zrange("streams", 0, -1, withscores=True))[0][1]
    planned_for = date.fromtimestamp(score)

    result = utils.test_console_scripts(
        admin_cli.admin_cli,
        ["stream-reschedule-now", "other"],
    )
    assert result.exit_code == 0
    assert result.output == "Stream for bucket~other not found\n"

    score_not_rescheduled = (
        await redis_links.stream.zrange("streams", 0, -1, withscores=True)
    )[0][1]
    planned_for_not_rescheduled = date.fromtimestamp(score_not_rescheduled)
    assert planned_for == planned_for_not_rescheduled

    result = utils.test_console_scripts(
        admin_cli.admin_cli,
        ["stream-reschedule-now", "123"],
    )
    assert result.exit_code == 0
    assert result.output == "Stream for bucket~123 rescheduled now\n"

    score_rescheduled = (
        await redis_links.stream.zrange("streams", 0, -1, withscores=True)
    )[0][1]
    planned_for_rescheduled = date.fromtimestamp(score_rescheduled)
    assert planned_for > planned_for_rescheduled
