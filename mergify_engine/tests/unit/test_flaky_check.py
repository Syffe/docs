import typing
from unittest import mock

import pytest

from mergify_engine import condition_value_querier
from mergify_engine import context
from mergify_engine import date
from mergify_engine import flaky_check
from mergify_engine import github_types
from mergify_engine import rules
from mergify_engine.flaky_check.utils import NeedRerunStatus


@mock.patch(
    "mergify_engine.flaky_check.is_gha_job_rerun_needed",
    new_callable=mock.AsyncMock,
)
@pytest.mark.parametrize(
    (
        "mergify_conf",
        "mock_is_gha_job_rerun_needed_value",
        "pending_checks",
        "failure_cheks",
        "flaky_checks_to_rerun",
    ),
    [
        (
            {"_checks_to_retry_on_failure": {"unit-test": 3}},
            NeedRerunStatus.NEED_RERUN,
            ["unit-test"],
            ["Summary"],
            [
                flaky_check.CheckToRerunResult(
                    check_name="unit-test",
                    check_id=5,
                    check_app_slug="github-actions",
                    status=NeedRerunStatus.NEED_RERUN,
                ),
            ],
        ),
        (
            {"_checks_to_retry_on_failure": {"unit-test": 3}},
            NeedRerunStatus.UNKONWN,
            ["unit-test"],
            ["Summary"],
            [
                flaky_check.CheckToRerunResult(
                    check_name="unit-test",
                    check_id=5,
                    check_app_slug="github-actions",
                    status=NeedRerunStatus.UNKONWN,
                ),
            ],
        ),
        (
            {"_checks_to_retry_on_failure": {"unit-test": 3}},
            NeedRerunStatus.DONT_NEED_RERUN,
            [],
            ["Summary", "unit-test"],
            [],
        ),
        (
            {"_checks_to_retry_on_failure": {"Summary": 3}},
            NeedRerunStatus.NEED_RERUN,
            [],
            ["Summary", "unit-test"],
            [],
        ),
        (
            {"_checks_to_retry_on_failure": {"Dummy job": 3}},
            NeedRerunStatus.NEED_RERUN,
            [],
            ["Summary", "unit-test"],
            [],
        ),
        ({}, NeedRerunStatus.NEED_RERUN, [], ["Summary", "unit-test"], []),
    ],
)
async def test_flaky_check_get_consolidate_data(
    mock_is_gha_job_rerun_needed: mock.AsyncMock,
    a_pull_request: github_types.GitHubPullRequest,
    mergify_conf: dict[str, typing.Any],
    mock_is_gha_job_rerun_needed_value: bool | None,
    pending_checks: list[str],
    failure_cheks: list[str],
    flaky_checks_to_rerun: list[flaky_check.CheckToRerunResult],
    fake_repository: context.Repository,
) -> None:
    repo = mock.Mock()
    repo.repo = fake_repository.repo
    repo.get_branch_protection.side_effect = mock.AsyncMock(return_value=None)
    repo.installation.client.items = mock.MagicMock(__aiter__=[])
    repo.mergify_config = rules.UserConfigurationSchema(mergify_conf)
    ctxt = context.Context(repo, a_pull_request)

    summary_check = github_types.CachedGitHubCheckRun(
        name="Summary",
        id=1,
        app_id=1,
        app_name="mergify",
        app_avatar_url="",
        app_slug="mergify",
        external_id="",
        head_sha=github_types.SHAType("azertyio"),
        status="completed",
        output={
            "title": "",
            "summary": "",
            "text": None,
            "annotations_count": 0,
            "annotations_url": "",
        },
        conclusion="failure",
        completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
        html_url="",
    )
    unit_test_check = github_types.CachedGitHubCheckRun(
        name="unit-test",
        id=5,
        app_id=10,
        app_name="Github Actions",
        app_avatar_url="",
        app_slug="github-actions",
        external_id="",
        head_sha=github_types.SHAType("azertyio"),
        status="completed",
        output={
            "title": "",
            "summary": "",
            "text": None,
            "annotations_count": 0,
            "annotations_url": "",
        },
        conclusion="failure",
        completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
        html_url="",
    )

    ctxt._caches.pull_check_runs.set([summary_check, unit_test_check])

    mock_is_gha_job_rerun_needed.return_value = mock_is_gha_job_rerun_needed_value

    pull_request = condition_value_querier.PullRequest(ctxt)
    assert (await pull_request.get_attribute_value("check_pending")) == pending_checks
    assert (await pull_request.get_attribute_value("check_failure")) == failure_cheks

    assert ctxt.flaky_checks_to_rerun == flaky_checks_to_rerun

    # NOTE(Kontrolix): Checked that real check_run are untouched and can still be accessed
    for check in await ctxt.pull_check_runs:
        assert check["status"] == "completed"
        assert check["conclusion"] == "failure"
