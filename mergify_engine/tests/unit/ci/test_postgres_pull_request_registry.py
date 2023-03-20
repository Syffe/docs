from freezegun import freeze_time
import sqlalchemy
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine.ci import models
from mergify_engine.ci import pull_registries
from mergify_engine.models import github_actions as sql_models
from mergify_engine.tests.unit.ci import utils


async def test_insert(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    pull = models.PullRequest(id=1, number=2, title="some title", state="open")
    registry = pull_registries.PostgresPullRequestRegistry()

    await registry.insert(pull)

    sql = sqlalchemy.select(sql_models.PullRequest)
    result = await db.scalars(sql)
    pulls = list(result)
    assert len(pulls) == 1
    actual_pull = pulls[0]
    assert actual_pull.id == 1
    assert actual_pull.number == 2
    assert actual_pull.title == "some title"

    # Doing it twice should update the record
    await registry.insert(pull)

    sql = sqlalchemy.select(sql_models.PullRequest)
    result = await db.scalars(sql)
    pulls = list(result)
    assert len(pulls) == 1
    actual_pull = pulls[0]
    assert actual_pull.id == 1
    assert actual_pull.number == 2
    assert actual_pull.title == "some title"


async def test_get_job_run_position(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    await db.execute(
        sqlalchemy.insert(sql_models.PullRequest).values(
            id=1, number=2, title="some title", state="open"
        )
    )
    with freeze_time("2023-02-24 12:00:00"):
        job_run1 = utils.create_job(
            job_id=1, workflow_run_id=1, workflow_id=1, name="some job"
        )
        await _insert_job_run(db, job_run1)
    with freeze_time("2023-02-24 13:00:00"):
        job_run2 = utils.create_job(
            job_id=2, workflow_run_id=2, workflow_id=1, name="some job"
        )
        await _insert_job_run(db, job_run2)
    await db.commit()
    registry = pull_registries.PostgresPullRequestRegistry()

    await registry.register_job_run(1, job_run1)
    await registry.register_job_run(1, job_run2)

    assert await registry.get_job_run_position(1, job_run1) == 0
    assert await registry.get_job_run_position(1, job_run2) == 1


async def _insert_job_run(
    db: sqlalchemy.ext.asyncio.AsyncSession, job_run: models.JobRun
) -> None:
    await db.execute(
        postgresql.insert(sql_models.Account)  # type: ignore [no-untyped-call]
        .values(id=job_run.owner.id, login=job_run.owner.login)
        .on_conflict_do_nothing()
    )
    await db.execute(
        postgresql.insert(sql_models.Account)  # type: ignore [no-untyped-call]
        .values(id=job_run.triggering_actor.id, login=job_run.triggering_actor.login)
        .on_conflict_do_nothing()
    )
    sql = sqlalchemy.insert(sql_models.JobRun).values(
        id=job_run.id,
        workflow_run_id=job_run.workflow_run_id,
        workflow_id=job_run.workflow_id,
        name=job_run.name,
        owner_id=job_run.owner.id,
        repository=job_run.repository,
        conclusion=sql_models.JobRunConclusion(job_run.conclusion),
        triggering_event=sql_models.JobRunTriggerEvent(job_run.triggering_event),
        triggering_actor_id=job_run.triggering_actor.id,
        started_at=job_run.started_at,
        completed_at=job_run.completed_at,
        run_attempt=job_run.run_attempt,
        operating_system=sql_models.JobRunOperatingSystem(job_run.operating_system),
        cores=job_run.cores,
    )
    await db.execute(sql)
