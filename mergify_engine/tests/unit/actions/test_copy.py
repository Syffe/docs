from unittest import mock

import pytest

from mergify_engine import check_api
from mergify_engine import github_types
from mergify_engine.actions import copy
from mergify_engine.tests.unit import conftest


async def test_copy_conclusion_success(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    client = mock.MagicMock()
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    action = copy.CopyAction({"branches": ["stable"]})
    await action.load_context(ctxt, mock.Mock())

    mocked_copy_result = [
        copy.CopyResult(
            branch=github_types.GitHubRefType("stable"),
            status=check_api.Conclusion.SUCCESS,
            details="#2 has been created for branch `stable`",
            job_id=None,
            pull_request_number=None,
            conflicting=False,
        )
    ]

    with mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._do_copies",
        return_value=mocked_copy_result,
    ), mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._state_redis_key",
        "whatever",
    ):
        result = await action.executor.run()

    assert result.conclusion == check_api.Conclusion.SUCCESS
    assert result.title == "Pull request copies have been created"
    assert result.summary == "* #2 has been created for branch `stable`"


async def test_copy_conclusion_failure(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    client = mock.MagicMock()
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    action = copy.CopyAction({"branches": ["stable", "feature"]})
    await action.load_context(ctxt, mock.Mock())

    mocked_copy_result = [
        copy.CopyResult(
            branch=github_types.GitHubRefType("stable"),
            status=check_api.Conclusion.SUCCESS,
            details="#2 has been created for branch `stable`",
            job_id=None,
            pull_request_number=None,
            conflicting=False,
        ),
        copy.CopyResult(
            branch=github_types.GitHubRefType("feature"),
            status=check_api.Conclusion.FAILURE,
            details="Copy to branch `feature` failed",
            job_id=None,
            pull_request_number=None,
            conflicting=False,
        ),
    ]

    with mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._do_copies",
        return_value=mocked_copy_result,
    ), mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._state_redis_key",
        "whatever",
    ):
        result = await action.executor.run()

    assert result.conclusion == check_api.Conclusion.FAILURE
    assert result.title == "No copy have been created"
    assert (
        result.summary
        == "* #2 has been created for branch `stable`\n* Copy to branch `feature` failed"
    )


async def test_copy_conclusion_pending(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    client = mock.MagicMock()
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    action = copy.CopyAction({"branches": ["stable", "feature"]})
    await action.load_context(ctxt, mock.Mock())

    mocked_copy_result = [
        copy.CopyResult(
            branch=github_types.GitHubRefType("stable"),
            status=check_api.Conclusion.SUCCESS,
            details="#2 has been created for branch `stable`",
            job_id=None,
            pull_request_number=None,
            conflicting=False,
        ),
        copy.CopyResult(
            branch=github_types.GitHubRefType("feature"),
            status=check_api.Conclusion.PENDING,
            details="Copy to branch `feature` in progress",
            job_id=None,
            pull_request_number=None,
            conflicting=False,
        ),
    ]

    with mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._do_copies",
        return_value=mocked_copy_result,
    ), mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._state_redis_key",
        "whatever",
    ):
        result = await action.executor.run()

    assert result.conclusion == check_api.Conclusion.PENDING
    assert result.title == "Pending"
    assert (
        result.summary
        == "* #2 has been created for branch `stable`\n* Copy to branch `feature` in progress"
    )


async def test_copy_unknown_conclusion(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    client = mock.MagicMock()
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    action = copy.CopyAction({"branches": ["stable"]})
    await action.load_context(ctxt, mock.Mock())

    mocked_copy_result = [
        copy.CopyResult(
            branch=github_types.GitHubRefType("stable"),
            status=check_api.Conclusion.SKIPPED,
            details="wtf",
            job_id=None,
            pull_request_number=None,
            conflicting=False,
        )
    ]

    with mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._do_copies",
        return_value=mocked_copy_result,
    ), mock.patch(
        "mergify_engine.actions.copy.CopyExecutor._state_redis_key",
        "whatever",
    ):
        with pytest.raises(
            RuntimeError,
            match="Unknown copy conclusion: {<Conclusion.SKIPPED: 'skipped'>}",
        ):
            await action.executor.run()
