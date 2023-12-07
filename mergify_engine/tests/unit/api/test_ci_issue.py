import anys
import pytest
import respx
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine.models import github as gh_models
from mergify_engine.tests import conftest
from mergify_engine.tests import utils as tests_utils
from mergify_engine.tests.db_populator import DbPopulator


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueDataset")
async def test_api_ci_issue_get_ci_issues(
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

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues",
        follow_redirects=False,
    )

    assert reply.json() == {
        "issues": [
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/failed_job_with_flaky_nghb"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                    {
                        "failed_run_count": 3,
                        "flaky": "flaky",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/flaky_failed_job_attempt_2"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                    {
                        "failed_run_count": 3,
                        "flaky": "flaky",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/flaky_failed_job_attempt_1"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "id": anys.ANY_INT,
                "job_name": "A job",
                "name": "Failure of A job",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
            },
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "OneAccount/OneRepo/failed_job_with_no_flaky_nghb"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "id": anys.ANY_INT,
                "job_name": "A job",
                "name": "Failure of A job",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
            },
        ],
    }

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues",
        follow_redirects=False,
    )

    assert reply.json() == {
        "issues": [
            {
                "events": [
                    {
                        "failed_run_count": 1,
                        "flaky": "unknown",
                        "id": DbPopulator.internal_ref[
                            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"
                        ],
                        "run_id": anys.ANY_INT,
                        "started_at": anys.ANY_DATETIME_STR,
                    },
                ],
                "id": anys.ANY_INT,
                "job_name": "A job",
                "name": "Failure of A job",
                "short_id": anys.ANY_STR,
                "status": "unresolved",
            },
        ],
    }


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueDataset")
async def test_api_ci_issue_get_ci_issue(
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

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issue)],
    )

    assert job.ci_issue_id is not None

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{job.ci_issue_id}",
        follow_redirects=False,
    )

    assert reply.json() == {
        "events": [
            {
                "failed_run_count": 1,
                "flaky": "unknown",
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/failed_job_with_flaky_nghb"
                ],
                "run_id": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
            },
            {
                "failed_run_count": 3,
                "flaky": "flaky",
                "id": DbPopulator.internal_ref[
                    "OneAccount/OneRepo/flaky_failed_job_attempt_2"
                ],
                "run_id": anys.ANY_INT,
                "started_at": anys.ANY_DATETIME_STR,
            },
            {
                "failed_run_count": 3,
                "flaky": "flaky",
                "id": job.id,
                "run_id": job.workflow_run_id,
                "started_at": job.started_at.isoformat(),
            },
        ],
        "id": job.ci_issue_id,
        "job_name": "A job",
        "name": "Failure of A job",
        "short_id": job.ci_issue.short_id,
        "status": "unresolved",
    }

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues/{job.ci_issue_id}",
        follow_redirects=False,
    )

    assert reply.status_code == 404

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref[
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"
        ],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issue)],
    )

    assert job.ci_issue_id is not None

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues/{job.ci_issue_id}",
        follow_redirects=False,
    )

    assert reply.json() == {
        "events": [
            {
                "failed_run_count": 1,
                "flaky": "unknown",
                "id": job.id,
                "run_id": job.workflow_run_id,
                "started_at": job.started_at.isoformat(),
            },
        ],
        "id": job.ci_issue_id,
        "job_name": "A job",
        "name": "Failure of A job",
        "short_id": job.ci_issue.short_id,
        "status": "unresolved",
    }


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueDataset")
async def test_api_ci_issue_get_ci_issue_event_detail(
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

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issue)],
    )

    assert job.ci_issue_id is not None

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{job.ci_issue_id}/events/{job.id}",
        follow_redirects=False,
    )

    assert job.steps is not None

    assert reply.json() == {
        "name": "A job",
        "id": job.id,
        "run_id": job.workflow_run_id,
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
        "failed_step_number": 1,
        "started_at": job.started_at.isoformat(),
        "completed_at": job.completed_at.isoformat(),
        "flaky": "flaky",
        "run_attempt": 1,
        "failed_run_count": 3,
        "embedded_log": "Some logs",
    }

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_2"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issue)],
    )

    assert job.ci_issue_id is not None

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{job.ci_issue_id}/events/{job.id}",
        follow_redirects=False,
    )

    assert job.steps is not None

    assert reply.json() == {
        "name": "A job",
        "id": job.id,
        "run_id": job.workflow_run_id,
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
        "failed_step_number": 1,
        "started_at": job.started_at.isoformat(),
        "completed_at": job.completed_at.isoformat(),
        "flaky": "flaky",
        "run_attempt": 2,
        "failed_run_count": 3,
        "embedded_log": "Some logs",
    }

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{job.ci_issue_id}/events/9999999",
        follow_redirects=False,
    )

    assert reply.status_code == 404

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/9999999/events/{job.id}",
        follow_redirects=False,
    )

    assert reply.status_code == 404

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock,
        populated_db,
        web_client,
        "colliding-account-1/colliding_repo_name",
    )

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/colliding-account-1/colliding_repo_name/ci_issues/{job.ci_issue_id}/events/{job.id}",
        follow_redirects=False,
    )

    assert reply.status_code == 404


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueDataset")
async def test_api_ci_issue_put_ci_issue(
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

    job = await populated_db.get_one(
        gh_models.WorkflowJob,
        DbPopulator.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"],
        options=[orm.joinedload(gh_models.WorkflowJob.ci_issue)],
    )

    assert job.ci_issue_id is not None

    response = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{job.ci_issue_id}",
        follow_redirects=False,
    )

    assert response.json() == {
        "id": job.ci_issue_id,
        "name": "Failure of A job",
        "short_id": job.ci_issue.short_id,
        "job_name": "A job",
        "status": "unresolved",
        "events": anys.ANY_LIST,
    }

    response = await web_client.patch(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{job.ci_issue_id}",
        json={"status": "resolved"},
        follow_redirects=False,
    )

    assert response.status_code == 200, response.text

    response = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/{job.ci_issue_id}",
        follow_redirects=False,
    )

    assert response.json() == {
        "id": job.ci_issue_id,
        "name": "Failure of A job",
        "short_id": job.ci_issue.short_id,
        "job_name": "A job",
        "status": "resolved",
        "events": anys.ANY_LIST,
    }

    # Patch unknown issue
    response = await web_client.patch(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues/0",
        json={"status": "resolved"},
        follow_redirects=False,
    )

    assert response.status_code == 404, response.text


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueDataset")
async def test_api_ci_issue_put_ci_issues(
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
        follow_redirects=False,
    )

    assert response.json() == {
        "issues": [
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Failure of A job",
                "job_name": "A job",
                "status": "unresolved",
                "events": anys.ANY_LIST,
            },
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Failure of A job",
                "job_name": "A job",
                "status": "unresolved",
                "events": anys.ANY_LIST,
            },
        ],
    }
    issue1 = response.json()["issues"][0]["id"]
    issue2 = response.json()["issues"][1]["id"]

    response = await web_client.patch(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues?id={issue1}&id={issue2}",
        json={"status": "resolved"},
        follow_redirects=False,
    )

    assert response.status_code == 200, response.text

    # Resolved issues are filtered out by default
    response = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues",
        follow_redirects=False,
    )

    assert response.json() == {"issues": []}

    response = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/ci_issues?status=resolved&status=unresolved",
        follow_redirects=False,
    )

    assert response.json() == {
        "issues": [
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Failure of A job",
                "job_name": "A job",
                "status": "resolved",
                "events": anys.ANY_LIST,
            },
            {
                "id": anys.ANY_INT,
                "short_id": anys.ANY_STR,
                "name": "Failure of A job",
                "job_name": "A job",
                "status": "resolved",
                "events": anys.ANY_LIST,
            },
        ],
    }
