from datetime import datetime
import typing

from dateutil.relativedelta import relativedelta
import numpy as np
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository
from mergify_engine.tests.db_populator import DbPopulator


class TestApiGhaFailedJobsDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        await cls.load(session, {"AccountAndRepo"})

        repo = typing.cast(
            github_types.GitHubRepository,
            (
                (
                    await session.execute(
                        sqlalchemy.select(github_repository.GitHubRepository)
                        .where(
                            github_repository.GitHubRepository.full_name
                            == "OneAccount/OneRepo"
                        )
                        .limit(1)
                    )
                ).scalar_one()
            ).as_github_dict(),
        )

        # Failed job
        job1 = await github_actions.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(github_actions.WorkflowJob),
                run_id=cls.next_id(github_actions.WorkflowRun),
                name="A job",
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
                        name="Run a step",
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

        job1.log_embedding = np.array(list(map(np.float32, [1] * 1536)))
        job1.embedded_log = "Some logs"
        job1.log_status = github_actions.WorkflowJobLogStatus.EMBEDDED

        # Successful job
        await github_actions.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(github_actions.WorkflowJob),
                run_id=cls.current_id(github_actions.WorkflowRun),
                name="A job",
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
                        name="Run a step",
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

        # Failed job similar to the job1
        job2 = await github_actions.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(github_actions.WorkflowJob),
                run_id=cls.next_id(github_actions.WorkflowRun),
                name="A job",
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
                        name="Run a step",
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

        job2.log_embedding = np.array(list(map(np.float32, ([1] * 1535) + [-1])))
        job2.embedded_log = "Some similar logs"
        job2.log_status = github_actions.WorkflowJobLogStatus.EMBEDDED

        await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
            session, [job2.id]
        )

        # Failed job completly different to the job1
        job3 = await github_actions.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(github_actions.WorkflowJob),
                run_id=cls.next_id(github_actions.WorkflowRun),
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(
                    (datetime.utcnow() - relativedelta(days=10)).isoformat()
                ),
                completed_at=github_types.ISODateTimeType(
                    (datetime.utcnow() - relativedelta(days=10)).isoformat()
                ),
                conclusion="failure",
                labels=[],
                run_attempt=1,
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            (datetime.utcnow() - relativedelta(days=10)).isoformat()
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

        job3.log_embedding = np.array(list(map(np.float32, [-1] * 1536)))
        job3.embedded_log = "Some different logs"
        job3.log_status = github_actions.WorkflowJobLogStatus.EMBEDDED

        await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
            session, [job3.id]
        )

        # Failed job similar to the job1 but on another repo
        repo = typing.cast(
            github_types.GitHubRepository,
            (
                (
                    await session.execute(
                        sqlalchemy.select(github_repository.GitHubRepository)
                        .where(
                            github_repository.GitHubRepository.full_name
                            == "colliding_acount_1/colliding_repo_name"
                        )
                        .limit(1)
                    )
                ).scalar_one()
            ).as_github_dict(),
        )

        job4 = await github_actions.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(github_actions.WorkflowJob),
                run_id=cls.next_id(github_actions.WorkflowRun),
                name="A job",
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
                        name="Run a step",
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

        job4.log_embedding = np.array(list(map(np.float32, [1] * 1536)))
        job4.embedded_log = "Some logs"
        job4.log_status = github_actions.WorkflowJobLogStatus.EMBEDDED

        await github_actions.WorkflowJob.compute_logs_embedding_cosine_similarity(
            session, [job4.id]
        )
