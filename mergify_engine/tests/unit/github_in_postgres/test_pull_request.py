from unittest import mock

from mergify_engine.models.github import pull_request as pr_model
from mergify_engine.models.github import pull_request_commit as pr_commit_model
from mergify_engine.models.github import pull_request_file as pr_file_model


async def test_dont_insert_commits_if_pydantic_error() -> None:
    async def fake_items(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        yield {"abc": 123}

    client = mock.Mock()
    client.items = mock.MagicMock(side_effect=fake_items)
    with mock.patch.object(
        pr_commit_model.PullRequestCommit,
        "insert_or_update",
    ) as mocked_insert_or_update:
        await pr_model.PullRequest._update_commits(
            client,
            mock.Mock(),
            123,
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
        )

    assert not mocked_insert_or_update.called


async def test_dont_insert_files_if_pydantic_error() -> None:
    async def fake_items(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        yield {"abc": 123}

    client = mock.Mock()
    client.items = mock.MagicMock(side_effect=fake_items)
    with mock.patch.object(
        pr_file_model.PullRequestFile,
        "insert_or_update",
    ) as mocked_insert_or_update:
        await pr_model.PullRequest._update_files(
            client,
            mock.AsyncMock(),
            123,
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
        )

    assert not mocked_insert_or_update.called
