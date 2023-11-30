import datetime
import typing

import numpy as np
import sqlalchemy.ext.asyncio

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue
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
                        sqlalchemy.select(gh_models.GitHubRepository)
                        .where(
                            gh_models.GitHubRepository.full_name
                            == "OneAccount/OneRepo",
                        )
                        .limit(1),
                    )
                ).scalar_one()
            ).as_github_dict(),
        )

        # Failed job
        job1 = await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=cls.next_id(gh_models.WorkflowRun),
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                conclusion="failure",
                labels=[],
                run_attempt=1,
                head_sha=github_types.SHAType(""),
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        completed_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        status="completed",
                    ),
                ],
                runner_id=1,
            ),
            repo,
        )

        job1.log_embedding = np.array(list(map(np.float32, [1] * 1536)))
        job1.embedded_log = "Some logs"
        job1.log_status = gh_models.WorkflowJobLogStatus.EMBEDDED

        cls.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"] = job1.id

        # Another failed job
        job2 = await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=cls.current_id(gh_models.WorkflowRun),
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                conclusion="failure",
                labels=[],
                run_attempt=2,
                head_sha=github_types.SHAType(""),
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        completed_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        status="completed",
                    ),
                ],
                runner_id=1,
            ),
            repo,
        )

        job2.log_embedding = np.array(list(map(np.float32, [1] * 1536)))
        job2.embedded_log = "Some logs"
        job2.log_status = gh_models.WorkflowJobLogStatus.EMBEDDED

        cls.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_2"] = job2.id

        # Successful job
        succesful_job = await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=cls.current_id(gh_models.WorkflowRun),
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                conclusion="success",
                labels=[],
                run_attempt=3,
                head_sha=github_types.SHAType(""),
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="success",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        completed_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        status="completed",
                    ),
                ],
                runner_id=1,
            ),
            repo,
        )

        cls.internal_ref["OneAccount/OneRepo/successful_flaky_job"] = succesful_job.id

        # Failed job similar to the job1
        job3 = await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=cls.next_id(gh_models.WorkflowRun),
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                conclusion="failure",
                labels=[],
                run_attempt=1,
                head_sha=github_types.SHAType(""),
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        completed_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        status="completed",
                    ),
                ],
                runner_id=1,
            ),
            repo,
        )

        job3.log_embedding = np.array(list(map(np.float32, ([1] * 1535) + [-1])))
        job3.embedded_log = "Some similar logs"
        job3.log_status = gh_models.WorkflowJobLogStatus.EMBEDDED

        cls.internal_ref["OneAccount/OneRepo/failed_job_with_flaky_nghb"] = job3.id

        # Failed job completly different to the job1
        job4 = await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=cls.next_id(gh_models.WorkflowRun),
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(
                    (date.utcnow() - datetime.timedelta(days=10)).isoformat(),
                ),
                completed_at=github_types.ISODateTimeType(
                    (date.utcnow() - datetime.timedelta(days=10)).isoformat(),
                ),
                conclusion="failure",
                labels=[],
                run_attempt=1,
                head_sha=github_types.SHAType(""),
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            (date.utcnow() - datetime.timedelta(days=10)).isoformat(),
                        ),
                        completed_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        status="completed",
                    ),
                ],
                runner_id=1,
            ),
            repo,
        )

        job4.log_embedding = np.array(list(map(np.float32, [-1] * 1536)))
        job4.embedded_log = "Some different logs"
        job4.log_status = gh_models.WorkflowJobLogStatus.EMBEDDED

        cls.internal_ref["OneAccount/OneRepo/failed_job_with_no_flaky_nghb"] = job4.id

        # Failed job similar to the job1 but on another repo
        colliding_repo = typing.cast(
            github_types.GitHubRepository,
            (
                (
                    await session.execute(
                        sqlalchemy.select(gh_models.GitHubRepository)
                        .where(
                            gh_models.GitHubRepository.full_name
                            == "colliding-account-1/colliding_repo_name",
                        )
                        .limit(1),
                    )
                ).scalar_one()
            ).as_github_dict(),
        )

        job5 = await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=cls.next_id(gh_models.WorkflowRun),
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                conclusion="failure",
                labels=[],
                run_attempt=1,
                head_sha=github_types.SHAType(""),
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        completed_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        status="completed",
                    ),
                ],
                runner_id=1,
            ),
            colliding_repo,
        )

        job5.log_embedding = np.array(list(map(np.float32, [1] * 1536)))
        job5.embedded_log = "Some logs"
        job5.log_status = gh_models.WorkflowJobLogStatus.EMBEDDED

        cls.internal_ref[
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"
        ] = job5.id

        # Failed job similar to the first one but not yet computed, it should be excluded
        uncomputed_job = await gh_models.WorkflowJob.insert(
            session,
            github_types.GitHubWorkflowJob(
                id=cls.next_id(gh_models.WorkflowJob),
                run_id=job1.workflow_run_id,
                name="A job",
                workflow_name="unit-test",
                started_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                completed_at=github_types.ISODateTimeType(date.utcnow().isoformat()),
                conclusion="failure",
                labels=[],
                run_attempt=2,
                head_sha=github_types.SHAType(""),
                steps=[
                    github_types.GitHubWorkflowJobStep(
                        name="Run a step",
                        conclusion="failure",
                        number=1,
                        started_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        completed_at=github_types.ISODateTimeType(
                            date.utcnow().isoformat(),
                        ),
                        status="completed",
                    ),
                ],
                runner_id=1,
            ),
            repo,
        )

        uncomputed_job.log_embedding = None
        uncomputed_job.embedded_log = None
        uncomputed_job.log_status = gh_models.WorkflowJobLogStatus.UNKNOWN

        cls.internal_ref["OneAccount/OneRepo/failed_job_uncomputed"] = uncomputed_job.id


class TestGhaFailedJobsLinkToCissueDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        await cls.load(session, {"TestApiGhaFailedJobsDataset"})
        jobs = await session.scalars(
            sqlalchemy.select(gh_models.WorkflowJob).where(
                gh_models.WorkflowJob.conclusion
                == gh_models.WorkflowJobConclusion.FAILURE,
                gh_models.WorkflowJob.log_embedding.isnot(None),
                gh_models.WorkflowJob.ci_issue_id.is_(None),
            ),
        )
        for job in jobs:
            await CiIssue.link_job_to_ci_issue(session, job)
