import typing

import anys
import pytest
import respx
import sqlalchemy

from mergify_engine import github_types
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository
from mergify_engine.tests import conftest
from mergify_engine.tests.unit import test_utils


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_api_flaky_jobs_get_gha_failed_jobs(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
    respx_mock: respx.MockRouter,
    web_client: conftest.CustomTestClient,
) -> None:
    await populated_db.commit()

    async def work_with_repo(full_name: str) -> None:
        repo_info = typing.cast(
            github_types.GitHubRepository,
            (
                (
                    await populated_db.execute(
                        sqlalchemy.select(github_repository.GitHubRepository)
                        .where(
                            github_repository.GitHubRepository.full_name == full_name
                        )
                        .limit(1)
                    )
                ).scalar_one()
            ).as_dict(),
        )

        user = await test_utils.mock_user_authorization_on_repo(
            respx_mock, repo_info, populated_db
        )

        await web_client.log_as(user.id)

    jobs = (
        (
            await populated_db.execute(
                sqlalchemy.select(github_actions.WorkflowJob).order_by(
                    github_actions.WorkflowJob.id
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

    await work_with_repo("OneAccount/OneRepo")

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
                        "started_at": str(jobs[0].started_at),
                        "completed_at": str(jobs[0].completed_at),
                        "flaky": "yes",
                        "run_attempt": 1,
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
                        "started_at": str(jobs[2].started_at),
                        "completed_at": str(jobs[2].completed_at),
                        "flaky": "unknown",
                        "run_attempt": 1,
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
                        "started_at": str(jobs[3].started_at),
                        "completed_at": str(jobs[3].completed_at),
                        "flaky": "unknown",
                        "run_attempt": 1,
                    },
                ]
            },
        ],
    }

    # Change parameters
    reply = await web_client.get(
        "/front/proxy/engine/v1/repos/OneAccount/OneRepo/gha-failed-jobs?neighbour_cosine_similarity_threshold=1&start_at=2023-01-01",
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
        "start_at": "2023-01-01",
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
                        "started_at": str(jobs[0].started_at),
                        "completed_at": str(jobs[0].completed_at),
                        "flaky": "yes",
                        "run_attempt": 1,
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
                        "started_at": str(jobs[2].started_at),
                        "completed_at": str(jobs[2].completed_at),
                        "flaky": "unknown",
                        "run_attempt": 1,
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
                        "started_at": str(jobs[3].started_at),
                        "completed_at": str(jobs[3].completed_at),
                        "flaky": "unknown",
                        "run_attempt": 1,
                    },
                ]
            },
        ],
    }

    # Request another repo
    await work_with_repo("colliding_acount_1/colliding_repo_name")

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
                        "started_at": str(jobs[4].started_at),
                        "completed_at": str(jobs[4].completed_at),
                        "flaky": "unknown",
                        "run_attempt": 1,
                    }
                ]
            },
        ],
    }
