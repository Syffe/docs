import pytest
import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.exc
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import ci_issue
from mergify_engine.models import github as gh_models
from mergify_engine.tests.db_populator import DbPopulator


@pytest.mark.populated_db_datasets("AccountAndRepo")
async def test_ci_issue_compute_short_id(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    issue = await ci_issue.CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"],
        ),
    )

    assert issue.short_id == "ONEREPO-1"

    issue = await ci_issue.CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"],
        ),
    )

    assert issue.short_id == "ONEREPO-2"

    issue = await ci_issue.CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["colliding_repo_1"],
        ),
    )

    assert issue.short_id == "COLLIDING_REPO_NAME-1"

    issue = await ci_issue.CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"],
        ),
    )

    assert issue.short_id == "ONEREPO-3"


@pytest.mark.populated_db_datasets("AccountAndRepo")
async def test_ci_issue_short_id_unicity(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    issue = await ci_issue.CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"],
        ),
    )

    assert issue.short_id_suffix == 1

    issue = await ci_issue.CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["colliding_repo_1"],
        ),
    )

    assert issue.short_id_suffix == 1

    issue = await ci_issue.CiIssue.insert(
        populated_db,
        repository_id=github_types.GitHubRepositoryIdType(
            DbPopulator.internal_ref["OneRepo"],
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
                gh_models.WorkflowJob.log_embedding_status
                == gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                gh_models.WorkflowJob.ci_issue_id.is_(None),
            ),
        )
    ).scalar_one() == 5

    assert (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(ci_issue.CiIssue),
        )
    ).scalar_one() == 0

    for embedded_job in await populated_db.scalars(
        sqlalchemy.select(gh_models.WorkflowJob).where(
            gh_models.WorkflowJob.log_embedding_status
            == gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
        ),
    ):
        await ci_issue.CiIssue.link_job_to_ci_issue(populated_db, embedded_job)

    assert (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count())
            .select_from(gh_models.WorkflowJob)
            .where(
                gh_models.WorkflowJob.log_embedding_status
                == gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED,
                gh_models.WorkflowJob.ci_issue_id.is_(None),
            ),
        )
    ).scalar_one() == 0

    assert (
        await populated_db.execute(
            sqlalchemy.select(sqlalchemy.func.count()).select_from(ci_issue.CiIssue),
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
                    ci_issue.CiIssue.jobs,
                ),
            ],
        )

        assert ref_job.ci_issue is not None

        assert [job.id for job in ref_job.ci_issue.jobs if job.id != ref_job.id] == [
            DbPopulator.internal_ref[nghb_job] for nghb_job in internal_ref_nghb_jobs
        ]


@pytest.mark.populated_db_datasets("TestApiGhaFailedJobsDataset")
async def test_link_job_to_ci_issues_gpt(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    count_job_ready_with_no_ci_issues_stmt = (
        sqlalchemy.select(sqlalchemy.func.count())
        .select_from(gh_models.WorkflowJob)
        .where(
            gh_models.WorkflowJob.log_metadata.any(),
            ~gh_models.WorkflowJob.ci_issues_gpt.any(),
        )
    )
    assert (
        await populated_db.execute(count_job_ready_with_no_ci_issues_stmt)
    ).scalar_one() == 5

    count_ci_issue_created_stmt = sqlalchemy.select(
        sqlalchemy.func.count(),
    ).select_from(ci_issue.CiIssueGPT)

    assert (await populated_db.execute(count_ci_issue_created_stmt)).scalar_one() == 0

    for job in (
        (
            await populated_db.execute(
                sqlalchemy.select(gh_models.WorkflowJob)
                .options(
                    orm.joinedload(gh_models.WorkflowJob.log_metadata),
                    orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt),
                )
                .where(
                    gh_models.WorkflowJob.log_metadata.any(),
                ),
            )
        )
        .unique()
        .scalars()
    ):
        await ci_issue.CiIssueGPT.link_job_to_ci_issues(populated_db, job)

    assert (
        await populated_db.execute(count_job_ready_with_no_ci_issues_stmt)
    ).scalar_one() == 0
    await populated_db.commit()
    assert (await populated_db.execute(count_ci_issue_created_stmt)).scalar_one() == 4
    populated_db.expunge_all()

    params: list[tuple[int, list[str]]] = [
        (
            1,
            [
                "OneAccount/OneRepo/flaky_failed_job_attempt_1",
                "OneAccount/OneRepo/flaky_failed_job_attempt_2",
                "OneAccount/OneRepo/failed_job_with_flaky_nghb",
            ],
        ),
        (
            2,
            ["OneAccount/OneRepo/failed_job_with_no_flaky_nghb"],
        ),
        (
            3,
            ["colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"],
        ),
        (
            4,
            ["OneAccount/OneRepo/failed_job_with_flaky_nghb"],
        ),
    ]

    for issue_id, internal_ref_nghb_jobs in params:
        issue = (
            (
                await populated_db.execute(
                    sqlalchemy.select(ci_issue.CiIssueGPT)
                    .options(orm.joinedload(ci_issue.CiIssueGPT.jobs))
                    .where(ci_issue.CiIssueGPT.id == issue_id),
                )
            )
            .unique()
            .scalar_one()
        )

        assert issue.jobs is not None

        assert {job.id for job in issue.jobs} == {
            DbPopulator.internal_ref[nghb_job] for nghb_job in internal_ref_nghb_jobs
        }


@pytest.mark.populated_db_datasets("TestGhaFailedJobsLinkToCissueGPTDataset")
async def test_ci_issue_last_seen_first_seen(
    populated_db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await populated_db.commit()
    populated_db.expunge_all()

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

    # NOTE(Kontrolix): We took this ci_issue because it has a several jobs linked to it
    ci_issue_id = flaky_failed_job_attempt_1.ci_issues_gpt[0].id

    issue = (
        await populated_db.execute(
            sqlalchemy.select(ci_issue.CiIssueGPT)
            .where(
                ci_issue.CiIssueGPT.id == ci_issue_id,
            )
            .options(
                ci_issue.CiIssueGPT.with_first_seen_column(),
                ci_issue.CiIssueGPT.with_last_seen_column(),
            ),
        )
    ).scalar_one()

    assert (
        flaky_failed_job_attempt_1.completed_at
        < failed_job_with_flaky_nghb.completed_at
    )

    assert issue.first_seen == flaky_failed_job_attempt_1.completed_at
    assert issue.last_seen == failed_job_with_flaky_nghb.completed_at
