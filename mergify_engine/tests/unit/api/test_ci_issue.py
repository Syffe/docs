from collections import abc
from unittest import mock

import anys
import pytest
import respx
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import date
from mergify_engine import pagination
from mergify_engine import settings
from mergify_engine.config import types as config_types
from mergify_engine.models import ci_issue
from mergify_engine.models import github as gh_models
from mergify_engine.tests import conftest
from mergify_engine.tests import utils as tests_utils
from mergify_engine.tests.db_populator import DbPopulator


@pytest.fixture(autouse=True, scope="module")
def _enable_ci_issues() -> abc.Generator[None, None, None]:
    with mock.patch.object(
        settings,
        "LOG_EMBEDDER_ENABLED_ORGS",
        config_types.StrListFromStrWithComma(["OneAccount", "colliding-account-1"]),
    ):
        yield


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueGPTDataset")
async def test_api_ci_issue_get_ci_issues_without_pr(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues",
    )

    failed_job_with_flaky_nghb = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/failed_job_with_flaky_nghb"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )
    flaky_failed_job_attempt_1 = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )
    failed_job_with_no_flaky_nghb = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/failed_job_with_no_flaky_nghb"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert reply.json() == {
        "issues": [
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                    {
                        "failed_run_count": 3,
                        "flaky": "flaky",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/flaky_failed_job_attempt_2/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                    {
                        "failed_run_count": 3,
                        "flaky": "flaky",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "events_count": 3,
                "id": flaky_failed_job_attempt_1.ci_issues_gpt[0].short_id_suffix,
                "job_name": "A job",
                "name": "Error on test: my_awesome_test",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
                "flaky": "flaky",
                "last_seen": date.to_isoformat_with_Z(
                    failed_job_with_flaky_nghb.completed_at,
                ),
                "first_seen": date.to_isoformat_with_Z(
                    flaky_failed_job_attempt_1.completed_at,
                ),
                "first_event_id": 1,
                "pull_requests_count": 0,
            },
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/2"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "events_count": 1,
                "id": failed_job_with_flaky_nghb.ci_issues_gpt[1].short_id_suffix,
                "job_name": "A job",
                "name": "Error on test: my_fucking_awesome_test",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
                "flaky": "unknown",
                "last_seen": date.to_isoformat_with_Z(
                    failed_job_with_flaky_nghb.completed_at,
                ),
                "first_seen": date.to_isoformat_with_Z(
                    failed_job_with_flaky_nghb.completed_at,
                ),
                "first_event_id": 4,
                "pull_requests_count": 0,
            },
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/failed_job_with_no_flaky_nghb/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "events_count": 1,
                "id": failed_job_with_no_flaky_nghb.ci_issues_gpt[0].short_id_suffix,
                "job_name": "A job",
                "name": "Error on test: my_cypress_test",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
                "flaky": "unknown",
                "last_seen": date.to_isoformat_with_Z(
                    failed_job_with_no_flaky_nghb.completed_at,
                ),
                "first_seen": date.to_isoformat_with_Z(
                    failed_job_with_no_flaky_nghb.completed_at,
                ),
                "first_event_id": 5,
                "pull_requests_count": 0,
            },
        ],
        "per_page": 10,
        "size": 3,
    }

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues",
    )

    failed_job_with_no_flaky_nghb = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref[
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"
        ],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )
    assert reply.json() == {
        "issues": [
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "events_count": 1,
                "id": failed_job_with_no_flaky_nghb.ci_issues_gpt[0].short_id_suffix,
                "job_name": "A job",
                "name": "Error on test: my_awesome_test",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
                "flaky": "unknown",
                "last_seen": date.to_isoformat_with_Z(
                    failed_job_with_no_flaky_nghb.completed_at,
                ),
                "first_seen": date.to_isoformat_with_Z(
                    failed_job_with_no_flaky_nghb.completed_at,
                ),
                "first_event_id": 6,
                "pull_requests_count": 0,
            },
        ],
        "per_page": 10,
        "size": 1,
    }


@pytest.mark.populated_db_datasets(
    "TestGhaFailedJobsLinkToCissueGPTDataset",
    "TestGhaFailedJobsPullRequestsDataset",
)
async def test_api_ci_issue_get_ci_issues_with_pr(
    _mock_gh_pull_request_commits_insert_in_pg: None,
    _mock_gh_pull_request_files_insert_in_pg: None,
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues",
    )

    failed_job_with_flaky_nghb = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/failed_job_with_flaky_nghb"],
    )
    flaky_failed_job_attempt_1 = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert reply.json() == {
        "issues": [
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                    {
                        "failed_run_count": 3,
                        "flaky": "flaky",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/flaky_failed_job_attempt_2/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                    {
                        "failed_run_count": 3,
                        "flaky": "flaky",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "events_count": 3,
                "id": flaky_failed_job_attempt_1.ci_issues_gpt[0].short_id_suffix,
                "job_name": "A job",
                "name": "Error on test: my_awesome_test",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
                "flaky": "flaky",
                "last_seen": date.to_isoformat_with_Z(
                    failed_job_with_flaky_nghb.completed_at,
                ),
                "first_seen": date.to_isoformat_with_Z(
                    flaky_failed_job_attempt_1.completed_at,
                ),
                "first_event_id": 1,
                "pull_requests_count": 3,
            },
        ],
        "per_page": 10,
        "size": 1,
    }

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues",
    )

    failed_job_with_no_flaky_nghb = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref[
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"
        ],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert reply.json() == {
        "issues": [
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb/metadata/1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "events_count": 1,
                "id": failed_job_with_no_flaky_nghb.ci_issues_gpt[0].short_id_suffix,
                "job_name": "A job",
                "name": "Error on test: my_awesome_test",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
                "flaky": "unknown",
                "last_seen": date.to_isoformat_with_Z(
                    failed_job_with_no_flaky_nghb.completed_at,
                ),
                "first_seen": date.to_isoformat_with_Z(
                    failed_job_with_no_flaky_nghb.completed_at,
                ),
                "pull_requests_count": 0,
                "first_event_id": 6,
            },
        ],
        "per_page": 10,
        "size": 1,
    }


@pytest.mark.populated_db_datasets(
    "TestGhaFailedJobsLinkToCissueGPTDataset",
    "TestGhaFailedJobsPullRequestsDataset",
)
async def test_api_ci_issue_get_ci_issue_events(
    _mock_gh_pull_request_commits_insert_in_pg: None,
    _mock_gh_pull_request_files_insert_in_pg: None,
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert len(job.ci_issues_gpt) == 1
    ci_issue = job.ci_issues_gpt[0]
    populated_db.expunge_all()

    assert job.steps is not None

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events",
    )

    assert reply.json() == {
        "per_page": 10,
        "size": 3,
        "events": [
            {
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/1"
                ],
                "name": "A job",
                "failed_run_count": 1,
                "failed_step_number": 1,
                "run_attempt": 1,
                "flaky": "unknown",
                "run_id": anys.ANY_INT,
                "job_id": anys.ANY_INT,
                "metadata": {
                    "language": "Python",
                    "lineno": 325,
                    "problem_type": "Error on test: my_awesome_test",
                    "stack_trace": "some traceback",
                    "test_framework": "pytest",
                },
                "started_at": anys.ANY_DATETIME_STR,
                "completed_at": anys.ANY_DATETIME_STR,
                "steps": [anys.ANY_DICT],
            },
            {
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/flaky_failed_job_attempt_2/metadata/1"
                ],
                "name": "A job",
                "metadata": {
                    "language": "Python",
                    "lineno": 325,
                    "problem_type": "Error on test: my_awesome_test",
                    "stack_trace": "some traceback",
                    "test_framework": "pytest",
                },
                "failed_run_count": 3,
                "failed_step_number": 1,
                "run_attempt": 2,
                "flaky": "flaky",
                "run_id": anys.ANY_INT,
                "job_id": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
                "completed_at": anys.ANY_DATETIME_STR,
                "steps": [anys.ANY_DICT],
            },
            {
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1"
                ],
                "name": "A job",
                "flaky": "flaky",
                "failed_run_count": 3,
                "failed_step_number": 1,
                "run_attempt": 1,
                "run_id": job.workflow_run_id,
                "job_id": job.id,
                "metadata": {
                    "language": "Python",
                    "lineno": 325,
                    "problem_type": "Error on test: my_awesome_test",
                    "stack_trace": "some traceback",
                    "test_framework": "pytest",
                },
                "started_at": job.started_at.isoformat().replace("+00:00", "Z"),
                "completed_at": job.completed_at.isoformat().replace("+00:00", "Z"),
                "steps": [
                    {
                        "name": "Run a step",
                        "status": "completed",
                        "conclusion": "failure",
                        "number": 1,
                        "started_at": job.steps[0]["started_at"],
                        "completed_at": job.steps[0]["completed_at"],
                    },
                ],
            },
        ],
    }

    expected_last_event_page_json = {
        "per_page": 1,
        "size": 1,
        "events": [
            {
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1"
                ],
                "name": "A job",
                "flaky": "flaky",
                "failed_run_count": 3,
                "failed_step_number": 1,
                "run_attempt": 1,
                "job_id": job.id,
                "run_id": job.workflow_run_id,
                "metadata": {
                    "language": "Python",
                    "lineno": 325,
                    "problem_type": "Error on test: my_awesome_test",
                    "stack_trace": "some traceback",
                    "test_framework": "pytest",
                },
                "started_at": job.started_at.isoformat().replace("+00:00", "Z"),
                "completed_at": job.completed_at.isoformat().replace("+00:00", "Z"),
                "steps": [
                    {
                        "name": "Run a step",
                        "status": "completed",
                        "conclusion": "failure",
                        "number": 1,
                        "started_at": job.steps[0]["started_at"],
                        "completed_at": job.steps[0]["completed_at"],
                    },
                ],
            },
        ],
    }

    expected_first_event_page_json = {
        "per_page": 1,
        "size": 1,
        "events": [
            {
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/1"
                ],
                "name": "A job",
                "failed_run_count": 1,
                "failed_step_number": 1,
                "run_attempt": 1,
                "flaky": "unknown",
                "run_id": anys.ANY_INT,
                "job_id": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
                "completed_at": anys.ANY_DATETIME_STR,
                "steps": [anys.ANY_DICT],
                "metadata": {
                    "language": "Python",
                    "lineno": 325,
                    "problem_type": "Error on test: my_awesome_test",
                    "stack_trace": "some traceback",
                    "test_framework": "pytest",
                },
            },
        ],
    }
    expected_second_event_page_json = {
        "per_page": 1,
        "size": 1,
        "events": [
            {
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/flaky_failed_job_attempt_2/metadata/1"
                ],
                "name": "A job",
                "failed_run_count": 3,
                "failed_step_number": 1,
                "run_attempt": 2,
                "flaky": "flaky",
                "run_id": anys.ANY_INT,
                "job_id": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
                "completed_at": anys.ANY_DATETIME_STR,
                "steps": [anys.ANY_DICT],
                "metadata": {
                    "language": "Python",
                    "lineno": 325,
                    "problem_type": "Error on test: my_awesome_test",
                    "stack_trace": "some traceback",
                    "test_framework": "pytest",
                },
            },
        ],
    }

    # Get first event
    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events?per_page=1",
    )
    assert reply.json() == expected_first_event_page_json

    # Get next event
    reply = await web_client.get(reply.links["next"]["url"])
    assert reply.json() == expected_second_event_page_json

    # Get last event
    reply = await web_client.get(reply.links["next"]["url"])
    assert reply.json() == expected_last_event_page_json
    reply = await web_client.get(
        reply.links["last"]["url"],
    )
    assert reply.json() == expected_last_event_page_json

    # Get second event in reverted order
    reply = await web_client.get(reply.links["next"]["url"])
    assert reply.json() == expected_second_event_page_json

    # Get first event in reverted order
    reply = await web_client.get(reply.links["next"]["url"])
    assert reply.json() == expected_first_event_page_json

    # Get two events per page
    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events?per_page=2",
    )
    assert reply.json() == {
        "per_page": 2,
        "size": 2,
        "events": [anys.ANY_DICT, anys.ANY_DICT],
    }

    # Get last event
    reply = await web_client.get(reply.links["next"]["url"])
    assert reply.json() == {
        "per_page": 2,
        "size": 1,
        "events": expected_last_event_page_json["events"],
    }

    # We reached the end there should be no next link
    reply = await web_client.get(reply.links["next"]["url"])
    assert reply.json() == {
        "per_page": 2,
        "size": 0,
        "events": [],
    }
    assert "next" not in reply.links

    # Invalid cursor
    invalid_cursor = pagination.Cursor("abcd", forward=True).to_string()
    response = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events?per_page=2&cursor={invalid_cursor}",
    )
    assert response.status_code == 422
    assert response.json() == {
        "message": "Invalid cursor",
        "cursor": invalid_cursor,
    }


@pytest.mark.populated_db_datasets(
    "TestGhaFailedJobsLinkToCissueGPTDataset",
    "TestGhaFailedJobsPullRequestsDataset",
)
async def test_api_ci_issue_get_ci_issue(
    _mock_gh_pull_request_commits_insert_in_pg: None,
    _mock_gh_pull_request_files_insert_in_pg: None,
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert len(job.ci_issues_gpt) == 1
    ci_issue = job.ci_issues_gpt[0]
    populated_db.expunge_all()

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}",
    )

    # NOTE(Kontrolix): We have 3 events in this payload but the sum of events_count is 5
    # on pull_request_impacted, because PR 123 and 789 share the same Head SHA.
    # Therefore, events OneAccount/OneRepo/flaky_failed_job_attempt_2/metadata/1 and
    # OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1 are counted twice, once for
    # each PR.
    assert reply.json() == {
        "events": [
            {
                "failed_run_count": 1,
                "flaky": "unknown",
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/1"
                ],
                "run_id": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
            },
            {
                "failed_run_count": 3,
                "flaky": "flaky",
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/flaky_failed_job_attempt_2/metadata/1"
                ],
                "run_id": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
            },
            {
                "failed_run_count": 3,
                "flaky": "flaky",
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1"
                ],
                "run_id": job.workflow_run_id,
                "started_at": date.to_isoformat_with_Z(job.started_at),
            },
        ],
        "id": ci_issue.short_id_suffix,
        "job_name": "A job",
        "name": "Error on test: my_awesome_test",
        "short_id": ci_issue.short_id,
        "status": "unresolved",
        "pull_requests_impacted": [
            {
                "author": "contributor",
                "events_count": 2,
                "number": 123,
                "title": "awesome",
            },
            {
                "author": "contributor",
                "events_count": 2,
                "number": 789,
                "title": "awesome",
            },
            {
                "author": "contributor",
                "events_count": 1,
                "number": 234,
                "title": "awesome",
            },
        ],
        "events_count": 3,
        "flaky": "flaky",
        "last_seen": anys.ANY_DATETIME_STR,
        "first_seen": anys.ANY_DATETIME_STR,
        "first_event_id": 1,
    }

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref[
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"
        ],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert len(job.ci_issues_gpt) == 1
    ci_issue = job.ci_issues_gpt[0]
    populated_db.expunge_all()

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues/{ci_issue.short_id_suffix}",
    )

    assert reply.json() == {
        "events": [
            {
                "failed_run_count": 1,
                "flaky": "unknown",
                "id": DbPopulator.internal_ref[
                    "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb/metadata/1"
                ],
                "run_id": job.workflow_run_id,
                "started_at": date.to_isoformat_with_Z(job.started_at),
            },
        ],
        "id": ci_issue.short_id_suffix,
        "job_name": "A job",
        "name": "Error on test: my_awesome_test",
        "short_id": ci_issue.short_id,
        "status": "unresolved",
        "pull_requests_impacted": [],
        "last_seen": anys.ANY_DATETIME_STR,
        "first_seen": anys.ANY_DATETIME_STR,
        "events_count": 1,
        "flaky": "unknown",
        "first_event_id": 6,
    }


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueGPTDataset")
async def test_api_ci_issue_get_ci_issue_accross_repository(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    # NOTE(Kontrolix): We use this job because it has a short_id_suffix that does not
    # exists in the colliding-account-1/colliding_repo_name repository
    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/failed_job_with_no_flaky_nghb"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert len(job.ci_issues_gpt) == 1
    ci_issue = job.ci_issues_gpt[0]
    populated_db.expunge_all()

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}",
    )

    assert reply.status_code == 200

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues/{ci_issue.short_id_suffix}",
    )

    assert reply.status_code == 404


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueGPTDataset")
async def test_api_ci_issue_get_ci_issue_event_log(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    event = await populated_db.get_one(
        gh_models.WorkflowJobLogMetadata,
        DbPopulator.internal_ref[
            "OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1"
        ],
    )
    issue = await populated_db.get_one(ci_issue.CiIssueGPT, event.ci_issue_id)

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{issue.short_id_suffix}/events/{event.id}/log",
    )
    assert reply.text == "Some logs"

    # Check 404 behavior
    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{issue.short_id_suffix}/events/666/log",
    )
    assert reply.status_code == 404

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/666/events/666/log",
    )
    assert reply.status_code == 404


@pytest.mark.populated_db_datasets(
    "TestGhaFailedJobsLinkToCissueGPTDataset",
)
async def test_api_ci_issue_get_ci_issue_event_detail(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    first_job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/failed_job_with_flaky_nghb"],
        options=[
            orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt),
            orm.joinedload(gh_models.WorkflowJob.log_metadata),
        ],
    )
    second_job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_2"],
        options=[
            orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt),
            orm.joinedload(gh_models.WorkflowJob.log_metadata),
        ],
    )
    third_job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[
            orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt),
            orm.joinedload(gh_models.WorkflowJob.log_metadata),
        ],
    )
    assert len(first_job.ci_issues_gpt) == 2
    assert len(second_job.ci_issues_gpt) == 1
    assert len(third_job.ci_issues_gpt) == 1

    assert len(first_job.log_metadata) == 2
    assert len(second_job.log_metadata) == 1
    assert len(third_job.log_metadata) == 1

    assert first_job.steps is not None
    assert second_job.steps is not None

    assert first_job.ci_issues_gpt[0].id == second_job.ci_issues_gpt[0].id
    assert first_job.ci_issues_gpt[0].id == third_job.ci_issues_gpt[0].id

    ci_issue = first_job.ci_issues_gpt[0]
    first_event_id = first_job.log_metadata[0].id
    second_event_id = second_job.log_metadata[0].id
    third_event_id = third_job.log_metadata[0].id

    assert first_event_id > second_event_id
    assert second_event_id > third_event_id

    assert ci_issue.short_id_suffix is not None

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events/{first_event_id}",
    )

    assert reply.json() == {
        "id": first_event_id,
        "name": "A job",
        "job_id": first_job.id,
        "run_id": first_job.workflow_run_id,
        "steps": [
            {
                "name": "Run a step",
                "status": "completed",
                "conclusion": "failure",
                "number": 1,
                "started_at": first_job.steps[0]["started_at"],
                "completed_at": first_job.steps[0]["completed_at"],
            },
        ],
        "failed_step_number": 1,
        "started_at": date.to_isoformat_with_Z(first_job.started_at),
        "completed_at": date.to_isoformat_with_Z(first_job.completed_at),
        "flaky": "unknown",
        "run_attempt": 1,
        "failed_run_count": 1,
        "log_extract": "Some similar logs",
        "metadata": {
            "language": "Python",
            "lineno": 325,
            "problem_type": "Error on test: my_awesome_test",
            "stack_trace": "some traceback",
            "test_framework": "pytest",
        },
    }
    assert reply.links == {
        "next": {
            "url": f"http://localhost:3000/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events/{second_event_id}",
            "rel": "next",
        },
    }

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events/{second_event_id}",
    )
    assert reply.links == {
        "prev": {
            "url": f"http://localhost:3000/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events/{first_event_id}",
            "rel": "prev",
        },
        "next": {
            "url": f"http://localhost:3000/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events/{third_event_id}",
            "rel": "next",
        },
    }

    assert reply.json() == {
        "id": second_event_id,
        "name": "A job",
        "job_id": second_job.id,
        "run_id": second_job.workflow_run_id,
        "steps": [
            {
                "name": "Run a step",
                "status": "completed",
                "conclusion": "failure",
                "number": 1,
                "started_at": second_job.steps[0]["started_at"],
                "completed_at": second_job.steps[0]["completed_at"],
            },
        ],
        "failed_step_number": 1,
        "started_at": date.to_isoformat_with_Z(second_job.started_at),
        "completed_at": date.to_isoformat_with_Z(second_job.completed_at),
        "flaky": "flaky",
        "run_attempt": 2,
        "failed_run_count": 3,
        "log_extract": "Some logs",
        "metadata": {
            "language": "Python",
            "lineno": 325,
            "problem_type": "Error on test: my_awesome_test",
            "stack_trace": "some traceback",
            "test_framework": "pytest",
        },
    }

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}/events/9999999",
    )
    assert reply.status_code == 404

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/9999999/events/{second_job.id}",
    )
    assert reply.status_code == 404

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues/{ci_issue.short_id_suffix}/events/{second_job.id}",
    )
    assert reply.status_code == 404


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueGPTDataset")
async def test_api_ci_issue_patch_ci_issue(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt)],
    )

    assert len(job.ci_issues_gpt) == 1
    ci_issue = job.ci_issues_gpt[0]

    response = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}",
    )

    assert response.json() == {
        "id": ci_issue.short_id_suffix,
        "name": "Error on test: my_awesome_test",
        "short_id": ci_issue.short_id,
        "job_name": "A job",
        "status": "unresolved",
        "events": anys.ANY_LIST,
        "pull_requests_impacted": [],
        "events_count": 3,
        "first_seen": anys.ANY_DATETIME_STR,
        "flaky": "flaky",
        "last_seen": anys.ANY_DATETIME_STR,
        "first_event_id": anys.ANY_INT,
    }

    response = await web_client.patch(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}",
        json={"status": "resolved"},
    )

    assert response.status_code == 200, response.text

    response = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{ci_issue.short_id_suffix}",
    )

    assert response.json() == {
        "id": ci_issue.short_id_suffix,
        "name": "Error on test: my_awesome_test",
        "short_id": ci_issue.short_id,
        "job_name": "A job",
        "status": "resolved",
        "events": anys.ANY_LIST,
        "pull_requests_impacted": [],
        "events_count": 3,
        "first_seen": anys.ANY_DATETIME_STR,
        "flaky": "flaky",
        "last_seen": anys.ANY_DATETIME_STR,
        "first_event_id": anys.ANY_INT,
    }

    # Patch unknown issue
    response = await web_client.patch(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/0",
        json={"status": "resolved"},
    )

    assert response.status_code == 404, response.text


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueGPTDataset")
async def test_api_ci_issue_patch_ci_issues(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    response = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues",
    )

    assert response.json() == {
        "issues": [
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Error on test: my_awesome_test",
                "job_name": "A job",
                "status": "unresolved",
                "events_count": 3,
                "events": anys.ANY_LIST,
                "flaky": "flaky",
                "last_seen": anys.ANY_DATETIME_STR,
                "first_seen": anys.ANY_DATETIME_STR,
                "pull_requests_count": 0,
                "first_event_id": anys.ANY_INT,
            },
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Error on test: my_fucking_awesome_test",
                "job_name": "A job",
                "status": "unresolved",
                "events_count": 1,
                "events": anys.ANY_LIST,
                "flaky": "unknown",
                "last_seen": anys.ANY_DATETIME_STR,
                "first_seen": anys.ANY_DATETIME_STR,
                "pull_requests_count": 0,
                "first_event_id": anys.ANY_INT,
            },
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Error on test: my_cypress_test",
                "job_name": "A job",
                "status": "unresolved",
                "events_count": 1,
                "events": anys.ANY_LIST,
                "flaky": "unknown",
                "last_seen": anys.ANY_DATETIME_STR,
                "first_seen": anys.ANY_DATETIME_STR,
                "pull_requests_count": 0,
                "first_event_id": anys.ANY_INT,
            },
        ],
        "per_page": 10,
        "size": 3,
    }

    issue1 = response.json()["issues"][0]["id"]
    issue2 = response.json()["issues"][1]["id"]
    issue3 = response.json()["issues"][2]["id"]

    response = await web_client.patch(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues?id={issue1}&id={issue2}&id={issue3}",
        json={"status": "resolved"},
    )

    assert response.status_code == 200, response.text

    # Resolved issues are filtered out by default
    response = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues",
    )

    assert response.json() == {"issues": [], "per_page": 10, "size": 0}

    response = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues?status=resolved&status=unresolved",
    )

    assert response.json() == {
        "issues": [
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Error on test: my_awesome_test",
                "job_name": "A job",
                "status": "resolved",
                "events_count": 3,
                "events": anys.ANY_LIST,
                "flaky": "flaky",
                "last_seen": anys.ANY_DATETIME_STR,
                "first_seen": anys.ANY_DATETIME_STR,
                "pull_requests_count": 0,
                "first_event_id": anys.ANY_INT,
            },
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Error on test: my_fucking_awesome_test",
                "job_name": "A job",
                "status": "resolved",
                "events_count": 1,
                "events": anys.ANY_LIST,
                "flaky": "unknown",
                "last_seen": anys.ANY_DATETIME_STR,
                "first_seen": anys.ANY_DATETIME_STR,
                "pull_requests_count": 0,
                "first_event_id": anys.ANY_INT,
            },
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Error on test: my_cypress_test",
                "job_name": "A job",
                "status": "resolved",
                "events_count": 1,
                "events": anys.ANY_LIST,
                "flaky": "unknown",
                "last_seen": anys.ANY_DATETIME_STR,
                "first_seen": anys.ANY_DATETIME_STR,
                "pull_requests_count": 0,
                "first_event_id": anys.ANY_INT,
            },
        ],
        "per_page": 10,
        "size": 3,
    }


@pytest.mark.populated_db_datasets("TestBigCiIssueDataset")
async def test_api_ci_issue_get_ci_issues_pagination(
    _mock_gh_pull_request_commits_insert_in_pg: None,
    _mock_gh_pull_request_files_insert_in_pg: None,
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "OneAccount/OneRepo",
    )

    # Get the first page containing 8 issues
    first_page = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues?per_page=8",
    )

    assert first_page.status_code == 200, first_page.text
    assert len(first_page.json()["issues"]) == 8
    assert "next" in first_page.links

    # Going to the second and last page, containing only one issue (the tenth
    # issue has only one pull request, so it is ignored)
    second_page = await web_client.get(
        first_page.links["next"]["url"],
    )

    assert second_page.status_code == 200, second_page.text
    assert len(second_page.json()["issues"]) == 1
    assert "prev" in second_page.links

    # We reached the end there should be no next link
    reply = await web_client.get(second_page.links["next"]["url"])
    assert reply.json() == {
        "per_page": 8,
        "size": 0,
        "issues": [],
    }
    assert "next" not in reply.links

    first_page_issue_ids = {i["id"] for i in first_page.json()["issues"]}
    second_page_issue_ids = {i["id"] for i in second_page.json()["issues"]}
    assert first_page_issue_ids != second_page_issue_ids

    # Going back to the first page
    first_page_again = await web_client.get(
        second_page.links["prev"]["url"],
    )

    assert first_page_again.status_code == 200, first_page_again.text
    assert len(first_page_again.json()["issues"]) == 8
    assert first_page_issue_ids == {i["id"] for i in first_page_again.json()["issues"]}

    # Invalid cursor
    invalid_cursor = pagination.Cursor("abcd", forward=True).to_string()
    response = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues?per_page=2&cursor={invalid_cursor}",
    )
    assert response.status_code == 422
    assert response.json() == {
        "message": "Invalid cursor",
        "cursor": invalid_cursor,
    }
