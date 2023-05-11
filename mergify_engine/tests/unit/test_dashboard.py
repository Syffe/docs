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
