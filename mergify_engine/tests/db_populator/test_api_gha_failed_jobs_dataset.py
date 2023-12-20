import datetime
import typing

import numpy as np
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.models.ci_issue import CiIssue
from mergify_engine.models.ci_issue import CiIssueGPT
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
                head_sha=github_types.SHAType("two_pr_with_the_same_head_sha"),
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
        job1.log_extract = "Some logs"
        job1.log_embedding_status = gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED

        job1_metadata = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job1.id,
            problem_type="Error on test: my_awesome_test",
            language="Python",
            filename="test.py",
            lineno="325",
            error="AssertionError: True is False",
            test_framework="pytest",
            stack_trace="some traceback",
        )
        session.add(job1_metadata)
        await session.flush()
        await session.refresh(job1_metadata)

        cls.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_1"] = job1.id
        cls.internal_ref[
            "OneAccount/OneRepo/flaky_failed_job_attempt_1/metadata/1"
        ] = job1_metadata.id

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
                head_sha=github_types.SHAType("two_pr_with_the_same_head_sha"),
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
        job2.log_extract = "Some logs"
        job2.log_embedding_status = gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED

        job2_metadata = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job2.id,
            problem_type="Error on test: my_awesome_test",
            language="Python",
            filename="test.py",
            lineno="325",
            error="AssertionError: True is False",
            test_framework="pytest",
            stack_trace="some traceback",
        )
        session.add(job2_metadata)
        await session.flush()
        await session.refresh(job2_metadata)

        cls.internal_ref["OneAccount/OneRepo/flaky_failed_job_attempt_2"] = job2.id
        cls.internal_ref[
            "OneAccount/OneRepo/flaky_failed_job_attempt_2/metadata/1"
        ] = job2_metadata.id

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
                head_sha=github_types.SHAType("two_pr_with_the_same_head_sha"),
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
                head_sha=github_types.SHAType("OneRepo_pr_234_sha_1"),
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
        job3.log_extract = "Some similar logs"
        job3.log_embedding_status = gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED

        job3_metadata1 = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job3.id,
            problem_type="Error on test: my_awesome_test",
            language="Python",
            filename="test.py",
            lineno="325",
            error="AssertionError: True is False",
            test_framework="pytest",
            stack_trace="some traceback",
        )
        session.add(job3_metadata1)
        job3_metadata2 = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job3.id,
            problem_type="Error on test: my_fucking_awesome_test",
            language="Python",
            filename="test.py",
            lineno="455",
            error="AssertionError: 3 == 0",
            test_framework="pytest",
            stack_trace="some other traceback",
        )
        session.add(job3_metadata2)
        await session.flush()
        await session.refresh(job3_metadata1)
        await session.refresh(job3_metadata2)

        cls.internal_ref["OneAccount/OneRepo/failed_job_with_flaky_nghb"] = job3.id
        cls.internal_ref[
            "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/1"
        ] = job3_metadata1.id
        cls.internal_ref[
            "OneAccount/OneRepo/failed_job_with_flaky_nghb/metadata/2"
        ] = job3_metadata2.id

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
                head_sha=github_types.SHAType("OneRepo_pr_456_sha_1"),
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
        job4.log_extract = "Some different logs"
        job4.log_embedding_status = gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED

        job4_metadata = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job4.id,
            problem_type="Error on test: my_cypress_test",
            language="JavaScript",
            filename="test.js",
            lineno="15",
            error="Some Js error",
            test_framework="Cypress",
            stack_trace="",
        )
        session.add(job4_metadata)
        await session.flush()
        await session.refresh(job4_metadata)

        cls.internal_ref["OneAccount/OneRepo/failed_job_with_no_flaky_nghb"] = job4.id
        cls.internal_ref[
            "OneAccount/OneRepo/failed_job_with_no_flaky_nghb/metadata/1"
        ] = job4_metadata.id

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
                head_sha=github_types.SHAType("sha1_linked_to_no_pr"),
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
        job5.log_extract = "Some logs"
        job5.log_embedding_status = gh_models.WorkflowJobLogEmbeddingStatus.EMBEDDED

        job5_metadata = gh_models.WorkflowJobLogMetadata(
            workflow_job_id=job5.id,
            problem_type="Error on test: my_awesome_test",
            language="Python",
            filename="test.py",
            lineno="325",
            error="AssertionError: True is False",
            test_framework="pytest",
            stack_trace="some traceback",
        )
        session.add(job5_metadata)
        await session.flush()
        await session.refresh(job5_metadata)

        cls.internal_ref[
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb"
        ] = job5.id
        cls.internal_ref[
            "colliding_acount_1/colliding_repo_name/failed_job_with_no_flaky_nghb/metadata/1"
        ] = job5_metadata.id

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
        uncomputed_job.log_extract = None
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


class TestGhaFailedJobsLinkToCissueGPTDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        await cls.load(session, {"TestApiGhaFailedJobsDataset"})
        jobs = await session.scalars(
            sqlalchemy.select(gh_models.WorkflowJob)
            .options(
                orm.joinedload(gh_models.WorkflowJob.log_metadata),
                orm.joinedload(gh_models.WorkflowJob.ci_issues_gpt),
            )
            .where(
                gh_models.WorkflowJob.conclusion
                == gh_models.WorkflowJobConclusion.FAILURE,
                gh_models.WorkflowJob.log_embedding.isnot(None),
                gh_models.WorkflowJob.ci_issue_id.is_(None),
            ),
        )
        for job in jobs.unique():
            await CiIssueGPT.link_job_to_ci_issues(session, job)


class TestGhaFailedJobsPullRequestsDataset(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        # NOTE(Kontrolix): circular import
        from mergify_engine.tests.utils import fake_full_pull_request

        await cls.load(session, {"AccountAndRepo"})
        repo = typing.cast(
            github_types.GitHubRepository,
            (
                await session.get_one(
                    gh_models.GitHubRepository,
                    DbPopulator.internal_ref["OneRepo"],
                )
            ).as_github_dict(),
        )

        repo.update(
            {
                "url": "https://blabla.com",
                "html_url": "https://blabla.com",
                "default_branch": github_types.GitHubRefType("main"),
            },
        )

        # PRs with a head sha linked to jobs:
        # OneAccount/OneRepo/flaky_failed_job_attempt_1
        # OneAccount/OneRepo/flaky_failed_job_attempt_2
        # OneAccount/OneRepo/successful_flaky_job
        pr = fake_full_pull_request(
            github_types.GitHubPullRequestId(cls.next_id(gh_models.PullRequest)),
            github_types.GitHubPullRequestNumber(123),
            repo,
        )
        pr["head"]["sha"] = github_types.SHAType("two_pr_with_the_same_head_sha")
        await gh_models.PullRequest.insert_or_update(
            session,
            pr,
        )
        pr = fake_full_pull_request(
            github_types.GitHubPullRequestId(cls.next_id(gh_models.PullRequest)),
            github_types.GitHubPullRequestNumber(789),
            repo,
        )
        pr["head"]["sha"] = github_types.SHAType("two_pr_with_the_same_head_sha")
        await gh_models.PullRequest.insert_or_update(
            session,
            pr,
        )

        # PR with a head sha linked to jobs:
        # OneAccount/OneRepo/failed_job_with_flaky_nghb
        pr = fake_full_pull_request(
            github_types.GitHubPullRequestId(cls.next_id(gh_models.PullRequest)),
            github_types.GitHubPullRequestNumber(234),
            repo,
        )
        pr["head"]["sha"] = github_types.SHAType("OneRepo_pr_234_sha_1")
        await gh_models.PullRequest.insert_or_update(
            session,
            pr,
        )

        # PR with a head sha linked to jobs:
        # OneAccount/OneRepo/failed_job_with_no_flaky_nghb
        pr = fake_full_pull_request(
            github_types.GitHubPullRequestId(cls.next_id(gh_models.PullRequest)),
            github_types.GitHubPullRequestNumber(456),
            repo,
        )
        pr["head"]["sha"] = github_types.SHAType("OneRepo_pr_456_sha_1")
        await gh_models.PullRequest.insert_or_update(
            session,
            pr,
        )
