import typing

from mergify_engine import dashboard
from mergify_engine import github_types
from mergify_engine import settings


def test_get_queue_pull_request_details_url() -> None:
    fake_pr = typing.cast(
        github_types.GitHubPullRequest,
        {
            "base": {"repo": {"name": "h2g2", "owner": {"login": "ADent"}}},
            "number": 42,
        },
    )

    assert (
        dashboard.get_queue_pull_request_details_url(fake_pr)
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/ADent/repo/h2g2/queues?pull=42"
    )


def test_get_eventlogs_url() -> None:
    fake_login = typing.cast(github_types.GitHubLogin, "Bar")
    fake_repo_name = typing.cast(github_types.GitHubRepositoryName, "Foo")
    assert (
        dashboard.get_eventlogs_url(fake_login, fake_repo_name)
        == f"{settings.DASHBOARD_UI_FRONT_URL}/github/{fake_login}/repo/{fake_repo_name}/event-logs"
    )
