from datetime import datetime
import typing

import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.tests.db_populator import DbPopulator


class WorkflowJob(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        await cls.load(session, {"OneAccountAndOneRepo"})
        await cls.load(session, {"TestApiGhaFailedJobsDataset"})

        repo = typing.cast(
            github_types.GitHubRepository,
            (
                (
                    await session.execute(
                        sqlalchemy.select(gh_models.GitHubRepository)
                        .where(
                            gh_models.GitHubRepository.full_name == "OneAccount/OneRepo"
                        )
                        .limit(1)
                    )
                ).scalar_one()
            ).as_github_dict(),
        )

        # Failed flaky job
        await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=1,
                name="Flaky job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(datetime.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(
                    datetime.utcnow().isoformat()
                ),
                conclusion="failure",
                labels=[],
                run_attempt=1,
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="toto",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            datetime.utcnow().isoformat()
                        ),
                        completed_at=github_types.ISODateTimeType(
                            datetime.utcnow().isoformat()
                        ),
                        status="completed",
                    )
                ],
                runner_id=1,
            ),
            repo,
        )

        # Successful flaky job
        await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=1,
                name="Flaky job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(datetime.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(
                    datetime.utcnow().isoformat()
                ),
                conclusion="success",
                labels=[],
                run_attempt=2,
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="toto",
                        conclusion="success",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            datetime.utcnow().isoformat()
                        ),
                        completed_at=github_types.ISODateTimeType(
                            datetime.utcnow().isoformat()
                        ),
                        status="completed",
                    )
                ],
                runner_id=1,
            ),
            repo,
        )

        # Failed job with no step
        await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=2,
                name="Failed job no step",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(datetime.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(
                    datetime.utcnow().isoformat()
                ),
                conclusion="failure",
                labels=[],
                run_attempt=1,
                steps=[],
                runner_id=1,
            ),
            repo,
        )
