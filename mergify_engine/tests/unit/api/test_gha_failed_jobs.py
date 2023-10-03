import datetime

import anys
from dateutil.relativedelta import relativedelta
import pytest
import respx
import sqlalchemy

from mergify_engine.models import github as gh_models
from mergify_engine.tests import conftest
from mergify_engine.tests import utils as tests_utils


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_api_gha_failed_jobs_get_gha_failed_jobs(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()

    jobs = (
        (
            await populated_db.execute(
                sqlalchemy.select(gh_models.WorkflowJob).order_by(
                    gh_models.WorkflowJob.id
                )
            )
        )
        .scalars()
        .all()
    )

    # NOTE(Kontrolix): We do that to please mypy and tell it that these are not None
    assert jobs[0].steps is not None
    assert jobs[2].steps is not None
    assert jobs[3].steps is not None
    assert jobs[4].steps is not None

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "OneAccount/OneRepo"
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/gha-failed-jobs",
        follow_redirects=False,
    )

    assert reply.json() == {
        "repository": {
            "id": anys.ANY_INT,
            "name": "OneRepo",
            "owner": {
                "id": anys.ANY_INT,
                "login": "OneAccount",
            },
        },
        "start_at": None,
        "min_similarity": 0.01,
        "workflow_job_groups": [
            {
                "workflow_jobs": [
                    {
                        "name": "A job",
                        "error_description": None,
                        "id": jobs[0].id,
                        "run_id": jobs[0].workflow_run_id,
                        "steps": [
                            {
                                "name": "Run a step",
                                "status": "completed",
                                "conclusion": "failure",
                                "number": 1,
                                "started_at": jobs[0].steps[0]["started_at"],
                                "completed_at": jobs[0].steps[0]["completed_at"],
                            }
                        ],
                        "failed_step_number": 1,
                        "started_at": jobs[0].started_at.isoformat(),
                        "completed_at": jobs[0].completed_at.isoformat(),
                        "flaky": "yes",
                        "run_attempt": 1,
                        "failed_retry_count": 2,
                    },
                    {
                        "name": "A job",
                        "error_description": None,
                        "id": jobs[2].id,
                        "run_id": jobs[2].workflow_run_id,
                        "steps": [
                            {
                                "name": "Run a step",
                                "status": "completed",
                                "conclusion": "failure",
                                "number": 1,
                                "started_at": jobs[2].steps[0]["started_at"],
                                "completed_at": jobs[2].steps[0]["completed_at"],
                            }
                        ],
                        "failed_step_number": 1,
                        "started_at": jobs[2].started_at.isoformat(),
                        "completed_at": jobs[2].completed_at.isoformat(),
                        "flaky": "unknown",
                        "run_attempt": 1,
                        "failed_retry_count": 1,
                    },
                ]
            },
            {
                "workflow_jobs": [
                    {
                        "name": "A job",
                        "error_description": None,
                        "id": jobs[3].id,
                        "run_id": jobs[3].workflow_run_id,
                        "steps": [
                            {
                                "name": "Run a step",
                                "status": "completed",
                                "conclusion": "failure",
                                "number": 1,
                                "started_at": jobs[3].steps[0]["started_at"],
                                "completed_at": jobs[3].steps[0]["completed_at"],
                            }
                        ],
                        "failed_step_number": 1,
                        "started_at": jobs[3].started_at.isoformat(),
                        "completed_at": jobs[3].completed_at.isoformat(),
                        "flaky": "unknown",
                        "run_attempt": 1,
                        "failed_retry_count": 1,
                    },
                ]
            },
        ],
    }

    # Change parameters
    test_date = (datetime.datetime.utcnow() - relativedelta(days=2)).strftime(
        "%Y-%m-%d"
    )
    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/gha-failed-jobs?neighbour_cosine_similarity_threshold=1&start_at={test_date}",
        follow_redirects=False,
    )

    assert reply.json() == {
        "repository": {
            "id": anys.ANY_INT,
            "name": "OneRepo",
            "owner": {
                "id": anys.ANY_INT,
                "login": "OneAccount",
            },
        },
        "start_at": test_date,
        "min_similarity": 1,
        "workflow_job_groups": [
            {
                "workflow_jobs": [
                    {
                        "name": "A job",
                        "error_description": None,
                        "id": jobs[0].id,
                        "run_id": jobs[0].workflow_run_id,
                        "steps": [
                            {
                                "name": "Run a step",
                                "status": "completed",
                                "conclusion": "failure",
                                "number": 1,
                                "started_at": jobs[0].steps[0]["started_at"],
                                "completed_at": jobs[0].steps[0]["completed_at"],
                            }
                        ],
                        "failed_step_number": 1,
                        "started_at": jobs[0].started_at.isoformat(),
                        "completed_at": jobs[0].completed_at.isoformat(),
                        "flaky": "yes",
                        "run_attempt": 1,
                        "failed_retry_count": 2,
                    }
                ]
            },
            {
                "workflow_jobs": [
                    {
                        "name": "A job",
                        "error_description": None,
                        "id": jobs[2].id,
                        "run_id": jobs[2].workflow_run_id,
                        "steps": [
                            {
                                "name": "Run a step",
                                "status": "completed",
                                "conclusion": "failure",
                                "number": 1,
                                "started_at": jobs[2].steps[0]["started_at"],
                                "completed_at": jobs[2].steps[0]["completed_at"],
                            }
                        ],
                        "failed_step_number": 1,
                        "started_at": jobs[2].started_at.isoformat(),
                        "completed_at": jobs[2].completed_at.isoformat(),
                        "flaky": "unknown",
                        "run_attempt": 1,
                        "failed_retry_count": 1,
                    },
                ]
            },
        ],
    }

    # Request another repo
    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "colliding_acount_1/colliding_repo_name"
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/colliding_acount_1/colliding_repo_name/gha-failed-jobs?neighbour_cosine_similarity_threshold=1&start_at=2023-01-01",
        follow_redirects=False,
    )

    assert reply.json() == {
        "repository": {
            "id": anys.ANY_INT,
            "name": "colliding_repo_name",
            "owner": {
                "id": anys.ANY_INT,
                "login": "colliding_acount_1",
            },
        },
        "start_at": "2023-01-01",
        "min_similarity": 1,
        "workflow_job_groups": [
            {
                "workflow_jobs": [
                    {
                        "name": "A job",
                        "error_description": None,
                        "id": jobs[4].id,
                        "run_id": jobs[4].workflow_run_id,
                        "steps": [
                            {
                                "name": "Run a step",
                                "status": "completed",
                                "conclusion": "failure",
                                "number": 1,
                                "started_at": jobs[4].steps[0]["started_at"],
                                "completed_at": jobs[4].steps[0]["completed_at"],
                            }
                        ],
                        "failed_step_number": 1,
                        "started_at": jobs[4].started_at.isoformat(),
                        "completed_at": jobs[4].completed_at.isoformat(),
                        "flaky": "unknown",
                        "run_attempt": 1,
                        "failed_retry_count": 1,
                    }
                ]
            },
        ],
    }


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_api_get_gha_failed_jobs_no_steps(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()

    job = await populated_db.scalar(
        sqlalchemy.select(gh_models.WorkflowJob)
        .order_by(gh_models.WorkflowJob.id.desc())
        .limit(1)
    )

    assert job is not None

    job.steps = None
    job.failed_step_number = None

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "colliding_acount_1/colliding_repo_name"
    )

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/colliding_acount_1/colliding_repo_name/gha-failed-jobs",
        follow_redirects=False,
    )

    assert reply.json() == {
        "repository": {
            "id": anys.ANY_INT,
            "name": "colliding_repo_name",
            "owner": {
                "id": anys.ANY_INT,
                "login": "colliding_acount_1",
            },
        },
        "start_at": None,
        "min_similarity": 0.01,
        "workflow_job_groups": [
            {
                "workflow_jobs": [
                    {
                        "name": "A job",
                        "error_description": None,
                        "id": job.id,
                        "run_id": job.workflow_run_id,
                        "steps": [],
                        "failed_step_number": None,
                        "started_at": job.started_at.isoformat(),
                        "completed_at": job.completed_at.isoformat(),
                        "flaky": "unknown",
                        "run_attempt": 1,
                        "failed_retry_count": 1,
                    }
                ]
            },
        ],
    }


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_api_gha_failed_jobs_get_gha_failed_job_detail(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()

    jobs = (
        (
            await populated_db.execute(
                sqlalchemy.select(gh_models.WorkflowJob).order_by(
                    gh_models.WorkflowJob.id
                )
            )
        )
        .scalars()
        .all()
    )

    # NOTE(Kontrolix): We do that to please mypy and tell it that these are not None
    assert jobs[0].steps is not None
    assert jobs[2].steps is not None
    assert jobs[3].steps is not None
    assert jobs[4].steps is not None

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "OneAccount/OneRepo"
    )

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/gha-failed-jobs/{jobs[0].id}",
        follow_redirects=False,
    )

    assert reply.json() == {
        "name": "A job",
        "error_description": None,
        "id": jobs[0].id,
        "run_id": jobs[0].workflow_run_id,
        "steps": [
            {
                "name": "Run a step",
                "status": "completed",
                "conclusion": "failure",
                "number": 1,
                "started_at": jobs[0].steps[0]["started_at"],
                "completed_at": jobs[0].steps[0]["completed_at"],
            }
        ],
        "failed_step_number": 1,
        "started_at": jobs[0].started_at.isoformat(),
        "completed_at": jobs[0].completed_at.isoformat(),
        "flaky": "yes",
        "run_attempt": 1,
        "failed_retry_count": 2,
        "embedded_log": "Some logs",
        "neighbour_job_ids": [jobs[2].id],
    }

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/gha-failed-jobs/{jobs[0].id}?neighbour_cosine_similarity_threshold=-1",
        follow_redirects=False,
    )

    assert reply.json() == {
        "name": "A job",
        "error_description": None,
        "id": jobs[0].id,
        "run_id": jobs[0].workflow_run_id,
        "steps": [
            {
                "name": "Run a step",
                "status": "completed",
                "conclusion": "failure",
                "number": 1,
                "started_at": jobs[0].steps[0]["started_at"],
                "completed_at": jobs[0].steps[0]["completed_at"],
            }
        ],
        "failed_step_number": 1,
        "started_at": jobs[0].started_at.isoformat(),
        "completed_at": jobs[0].completed_at.isoformat(),
        "flaky": "yes",
        "run_attempt": 1,
        "failed_retry_count": 2,
        "embedded_log": "Some logs",
        "neighbour_job_ids": [jobs[2].id, jobs[3].id],
    }

    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/gha-failed-jobs/5000",
        follow_redirects=False,
    )

    assert reply.status_code == 404

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/OneAccount/OneRepo/gha-failed-jobs/{jobs[4].id}",
        follow_redirects=False,
    )

    assert reply.status_code == 404

    await tests_utils.configure_web_client_to_work_with_a_repo(
        respx_mock, populated_db, web_client, "colliding_acount_1/colliding_repo_name"
    )

    reply = await web_client.get(
        f"/front/proxy/engine/v1/repos/colliding_acount_1/colliding_repo_name/gha-failed-jobs/{jobs[4].id}",
        follow_redirects=False,
    )

    assert reply.json() == {
        "name": "A job",
        "error_description": None,
        "id": jobs[4].id,
        "run_id": jobs[4].workflow_run_id,
        "steps": [
            {
                "name": "Run a step",
                "status": "completed",
                "conclusion": "failure",
                "number": 1,
                "started_at": jobs[4].steps[0]["started_at"],
                "completed_at": jobs[4].steps[0]["completed_at"],
            }
        ],
        "failed_step_number": 1,
        "started_at": jobs[4].started_at.isoformat(),
        "completed_at": jobs[4].completed_at.isoformat(),
        "flaky": "unknown",
        "run_attempt": 1,
        "failed_retry_count": 1,
        "embedded_log": "Some logs",
        "neighbour_job_ids": [],
    }
