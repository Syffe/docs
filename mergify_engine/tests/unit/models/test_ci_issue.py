import pytest
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.exc
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue
from mergify_engine.tests.db_populator import DbPopulator


@pytest.mark.populated_db_datasets("AccountAndRepo")
async def test_ci_issue_compute_short_id(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    issue = await CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"]
        ),
    )

    assert issue.short_id == "ONEREPO-1"

    issue = await CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"]
        ),
    )

    assert issue.short_id == "ONEREPO-2"

    issue = await CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["colliding_repo_1"],
        ),
    )

    assert issue.short_id == "COLLIDING_REPO_NAME-1"

    issue = await CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"]
        ),
    )

    assert issue.short_id == "ONEREPO-3"


@pytest.mark.populated_db_datasets("AccountAndRepo")
async def test_ci_issue_short_id_unicity(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    issue = await CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"]
        ),
    )

    assert issue.short_id_suffix == 1

    issue = await CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["colliding_repo_1"],
        ),
    )

    assert issue.short_id_suffix == 1

    issue = await CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"]
        ),
    )

    assert issue.short_id_suffix == 2

    await populated_db.commit()

    issue.short_id_suffix = 1

    with pytest.raises(
        sqlalchemy.exc.IntegrityError,
        match=r"\(psycopg\.errors\.UniqueViolation\) duplicate key value violates unique constraint",
    ):
        await populated_db.commit()


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_link_job_to_ci_issue(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    assert (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count())
            .select_from(gh_models.WorkflowJob)
            .where(
                gh_models.WorkflowJob.log_status
                == gh_models.WorkflowJobLogStatus.EMBEDDED,
                gh_models.WorkflowJob.ci_issue_id.is_(None),
            )
        )
    ).scalar_one() == 5

    assert (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(CiIssue)
        )
    ).scalar_one() == 0

    for embedded_job in await populated_db.scalars(
        sqlalchemy.select(gh_models.WorkflowJob).where(
            gh_models.WorkflowJob.log_status == gh_models.WorkflowJobLogStatus.EMBEDDED
        )
    ):
        await CiIssue.link_job_to_ci_issue(populated_db, embedded_job)

    assert (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count())
            .select_from(gh_models.WorkflowJob)
            .where(
                gh_models.WorkflowJob.log_status
                == gh_models.WorkflowJobLogStatus.EMBEDDED,
                gh_models.WorkflowJob.ci_issue_id.is_(None),
            )
        )
    ).scalar_one() == 0

    assert (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(CiIssue)
        )
    ).scalar_one() == 3

    populated_db.expunge_all()

    params: list[tuple[str, list[str]]] = [
        (
            "OneAccount/OneRepo/flaky_failed_job_attempt_1",
            [
                "OneAccount/OneRepo/flaky_failed_job_attempt_2",
                "OneAccount/OneRepo/failed_job_with_flaky_nghb",
            ],
        ),
        (
            "OneAccount/OneRepo/failed_job_with_no_flaky_nghb",
            [],
        ),
        (
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb",
            [],
        ),
    ]

    for internal_ref_job, internal_ref_nghb_jobs in params:
        ref_job = await populated_db.get_one(
            gh_models.WorkflowJob,
            DbPopulator.internal_ref[internal_ref_job],
            options=[
                orm.joinedload(gh_models.WorkflowJob.ci_issue).selectinload(
                    CiIssue.jobs
                )
            ],
        )

        assert ref_job.ci_issue is not None

        assert [job.id for job in ref_job.ci_issue.jobs if job.id != ref_job.id] == [
            DbPopulator.internal_ref[nghb_job] for nghb_job in internal_ref_nghb_jobs
        ]
