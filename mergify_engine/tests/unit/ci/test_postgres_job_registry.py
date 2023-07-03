import datetime

import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine.ci import job_registries
from mergify_engine.models import github_account
from mergify_engine.models import github_actions as sql_models
from mergify_engine.tests.unit.ci import utils


async def test_insert(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    job_run = utils.create_job(
        job_id=1,
        name="some-job",
        owner="some-owner",
        repository="some-repo",
        conclusion="success",
        triggering_event="push",
        triggering_actor="some-user",
        run_attempt=2,
        operating_system="Windows",
        cores=4,
    )
    registry = job_registries.PostgresJobRegistry()

    await registry.insert(job_run)

    sql = sqlalchemy.select(sql_models.JobRun)
    result = await db.scalars(sql)
    job_runs = list(result)
    assert len(job_runs) == 1
    actual_job_run = job_runs[0]
    assert actual_job_run.id == 1
    assert actual_job_run.name == "some-job"
    assert actual_job_run.owner.login == "some-owner"
    assert actual_job_run.repository == "some-repo"
    assert actual_job_run.conclusion == sql_models.JobRunConclusion.SUCCESS
    assert actual_job_run.triggering_event == sql_models.JobRunTriggerEvent.PUSH
    assert actual_job_run.triggering_actor.login == "some-user"
    assert actual_job_run.run_attempt == 2
    assert actual_job_run.operating_system == sql_models.JobRunOperatingSystem.WINDOWS
    assert actual_job_run.cores == 4


async def test_filter_if_exist(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    registry = job_registries.PostgresJobRegistry()
    assert await registry.filter_if_exist(1) == {1}

    await _insert_data(db)

    assert await registry.filter_if_exist(1) == set()


async def _insert_data(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    await db.execute(
        sqlalchemy.insert(github_account.GitHubAccount).values(id=1, login="some-owner")
    )
    await db.execute(
        sqlalchemy.insert(github_account.GitHubAccount).values(
            id=2, login="some-other-owner"
        )
    )
    await db.execute(
        sqlalchemy.insert(github_account.GitHubAccount).values(id=3, login="some-user")
    )

    # Insert a record matching the request
    sql = sqlalchemy.insert(sql_models.JobRun).values(
        id=1,
        workflow_run_id=1,
        workflow_id=1,
        name="some-job-1",
        owner_id=1,
        repository="some-repo",
        conclusion=sql_models.JobRunConclusion.FAILURE,
        triggering_event=sql_models.JobRunTriggerEvent.PULL_REQUEST,
        triggering_actor_id=3,
        started_at=datetime.datetime.now(),
        completed_at=datetime.datetime.now(),
        run_attempt=1,
        operating_system=sql_models.JobRunOperatingSystem.LINUX,
        cores=4,
    )
    await db.execute(sql)

    # Another owner
    sql = sqlalchemy.insert(sql_models.JobRun).values(
        id=2,
        workflow_run_id=1,
        workflow_id=1,
        name="some-job-2",
        owner_id=2,
        repository="some-repo",
        conclusion=sql_models.JobRunConclusion.FAILURE,
        triggering_event=sql_models.JobRunTriggerEvent.PULL_REQUEST,
        triggering_actor_id=3,
        started_at=datetime.datetime.now(),
        completed_at=datetime.datetime.now(),
        run_attempt=1,
        operating_system=sql_models.JobRunOperatingSystem.LINUX,
        cores=4,
    )
    await db.execute(sql)

    # Another repository
    sql = sqlalchemy.insert(sql_models.JobRun).values(
        id=3,
        workflow_run_id=1,
        workflow_id=1,
        name="some-job-3",
        owner_id=1,
        repository="some-other-repo",
        conclusion=sql_models.JobRunConclusion.FAILURE,
        triggering_event=sql_models.JobRunTriggerEvent.PULL_REQUEST,
        triggering_actor_id=3,
        started_at=datetime.datetime.now(),
        completed_at=datetime.datetime.now(),
        run_attempt=1,
        operating_system=sql_models.JobRunOperatingSystem.LINUX,
        cores=4,
    )
    await db.execute(sql)

    # Another date
    sql = sqlalchemy.insert(sql_models.JobRun).values(
        id=4,
        workflow_run_id=1,
        workflow_id=1,
        name="some-job-4",
        owner_id=1,
        repository="some-repo",
        conclusion=sql_models.JobRunConclusion.FAILURE,
        triggering_event=sql_models.JobRunTriggerEvent.PULL_REQUEST,
        triggering_actor_id=3,
        started_at=datetime.datetime.now() + datetime.timedelta(days=2),
        completed_at=datetime.datetime.now() + datetime.timedelta(days=2),
        run_attempt=1,
        operating_system=sql_models.JobRunOperatingSystem.LINUX,
        cores=4,
    )
    await db.execute(sql)
    await db.commit()
