import anys
import pytest
import respx
import sqlalchemy
import sqlalchemy.ext.asyncio
import sqlalchemy.orm as orm

from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue
from mergify_engine.tests import conftest
from mergify_engine.tests import utils as tests_utils
from mergify_engine.tests.db_populator import DbPopulator


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_api_ci_issue_get_ci_issues(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    for job in await populated_db.scalars(
        sqlalchemy.select(gh_models.WorkflowJob).where(
            gh_models.WorkflowJob.conclusion == gh_models.WorkflowJobConclusion.FAILURE,
            gh_models.WorkflowJob.log_embedding.isnot(None),
            gh_models.WorkflowJob.ci_issue_id.is_(None),
        )
    ):
        await CiIssue.link_job_to_ci_issue(populated_db, job)
    await populated_db.commit()

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "OneAccount/OneRepo"
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
                    }
                ],
                "id": anys.ANY_INT,
                "job_name": "A job",
                "name": "Failure of A job",
                "short_id": anys.ANY_STR,
            },
        ],
    }

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "colliding-account-1/colliding_repo_name"
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
                    }
                ],
                "id": anys.ANY_INT,
                "job_name": "A job",
                "name": "Failure of A job",
                "short_id": anys.ANY_STR,
            },
        ],
    }


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_api_ci_issue_get_ci_issue(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    for job in await populated_db.scalars(
        sqlalchemy.select(gh_models.WorkflowJob).where(
            gh_models.WorkflowJob.conclusion == gh_models.WorkflowJobConclusion.FAILURE,
            gh_models.WorkflowJob.log_embedding.isnot(None),
            gh_models.WorkflowJob.ci_issue_id.is_(None),
        )
    ):
        await CiIssue.link_job_to_ci_issue(populated_db, job)
    await populated_db.commit()

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "OneAccount/OneRepo"
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
    }

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "colliding-account-1/colliding_repo_name"
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
    }
