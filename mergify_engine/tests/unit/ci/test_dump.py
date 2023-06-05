import datetime
import typing
from unittest import mock

from freezegun import freeze_time
import msgpack
import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import date
from mergify_engine import github_events
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import dump
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository


NOW = datetime.datetime(2023, 4, 20, 12, 0, tzinfo=datetime.UTC)
AN_HOUR_AGO = datetime.datetime(2023, 4, 20, 11, 0, tzinfo=datetime.UTC)
TWO_HOURS_AGO = datetime.datetime(2023, 4, 20, 10, 0, tzinfo=datetime.UTC)
LESS_THAN_AN_HOUR_AGO = datetime.datetime(2023, 4, 20, 11, 1, tzinfo=datetime.UTC)


@pytest.fixture(autouse=True)
async def insert_accounts(db: sqlalchemy.ext.asyncio.AsyncSession) -> None:
    await db.execute(
        sqlalchemy.insert(github_account.GitHubAccount),
        [{"id": 1, "login": "some_owner"}],
    )
    await db.commit()


@pytest.fixture(autouse=True)
async def mock_repositories() -> typing.AsyncGenerator[None, None]:
    repos = {11: "some_repo", 12: "another_repo"}

    with mock.patch(
        "mergify_engine.ci.dump._get_repository_name_from_id",
        side_effect=lambda _, _id: repos.get(_id),
    ):
        yield


@freeze_time(NOW)
async def test_dump_next_repository(
    redis_links: redis_utils.RedisLinks, db: sqlalchemy.ext.asyncio.AsyncSession
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": AN_HOUR_AGO,
            }
        ],
    )
    await db.commit()

    mocked_gh_client = mock.AsyncMock()

    with mock.patch("mergify_engine.ci.dump.dump") as mocked_dump, mock.patch(
        "mergify_engine.ci.dump._create_gh_client_from_login",
        return_value=mocked_gh_client,
    ):
        await dump.dump_next_repository(redis_links)

        mocked_dump.assert_called_once_with(
            redis_links,
            mocked_gh_client,
            "some_owner",
            "some_repo",
            date.DateTimeRange(AN_HOUR_AGO, NOW),
        )

    result = await db.execute(
        sqlalchemy.select(github_repository.GitHubRepository.last_dump_at)
    )
    row = result.first()
    assert row is not None
    assert row.last_dump_at == NOW.replace(tzinfo=None)


@freeze_time(NOW)
async def test_get_next_repository_normal_case(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": AN_HOUR_AGO,
            },
            {
                "id": 12,
                "owner_id": 1,
                "last_dump_at": NOW,
            },
        ],
    )
    await db.commit()

    next_sub = await dump.get_next_repository(db)

    assert next_sub.owner_id == 1
    assert next_sub.owner == "some_owner"
    assert next_sub.repository_id == 11
    assert next_sub.last_dump_at == AN_HOUR_AGO


@freeze_time(NOW)
async def test_get_next_repository_new_repository(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": None,
            },
            {
                "id": 12,
                "owner_id": 1,
                "last_dump_at": NOW,
            },
        ],
    )
    await db.commit()

    next_sub = await dump.get_next_repository(db)

    assert next_sub.owner_id == 1
    assert next_sub.owner == "some_owner"
    assert next_sub.repository_id == 11
    assert next_sub.last_dump_at == AN_HOUR_AGO


@freeze_time(NOW)
async def test_get_next_repository_nothing_to_dump(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": LESS_THAN_AN_HOUR_AGO,
            },
        ],
    )
    await db.commit()

    with pytest.raises(dump.NoDataToDump):
        await dump.get_next_repository(db)


@freeze_time(NOW)
async def test_get_next_repository_not_up_to_date(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": TWO_HOURS_AGO,
            },
        ],
    )
    await db.commit()

    next_sub = await dump.get_next_repository(db)

    assert next_sub.owner_id == 1
    assert next_sub.owner == "some_owner"
    assert next_sub.repository_id == 11
    assert next_sub.last_dump_at == TWO_HOURS_AGO


@pytest.fixture
def sample_ci_events_to_process(
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]]
) -> dict[str, github_events.CIEventToProcess]:
    ci_events = {}

    for filename, (event_type, event) in sample_events.items():
        if event_type in ("workflow_run", "workflow_job"):
            ci_events[filename] = github_events.CIEventToProcess(event_type, "", event)

    return ci_events


async def test_dump_event_stream(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    logger_checker: None,
) -> None:
    await redis_links.stream.xadd(
        "workflow_job",
        {"workflow_run_key": "some/key", "workflow_job_id": 13403743463},
    )
    await redis_links.stream.hset(
        "some/key",
        "workflow_job/13403743463",
        msgpack.packb(
            {
                "event_type": "workflow_job",
                "data": sample_ci_events_to_process[
                    "workflow_job.completed.json"
                ].slim_event,
            }
        ),
    )

    # We havn't received the completed workflow run event, it should do nothing
    await dump.dump_event_stream(redis_links)
    stream_events = await redis_links.stream.xrange("workflow_job")
    assert len(stream_events) == 1

    await redis_links.stream.hset(
        "some/key",
        "workflow_run",
        msgpack.packb(
            {
                "event_type": "workflow_run",
                "data": sample_ci_events_to_process[
                    "workflow_run.completed.json"
                ].slim_event,
            }
        ),
    )

    with mock.patch(
        "mergify_engine.ci.pull_registries.RedisPullRequestRegistry.get_from_commit",
        return_value=[],
    ), mock.patch("mergify_engine.ci.dump._create_gh_client_from_login"):
        await dump.dump_event_stream(redis_links)

    sql = sqlalchemy.select(github_actions.JobRun)
    result = await db.scalars(sql)
    job_runs = list(result)
    assert len(job_runs) == 1
    actual_job_run = job_runs[0]
    assert actual_job_run.name == "test"
    assert actual_job_run.conclusion == github_actions.JobRunConclusion.SUCCESS

    stream_events = await redis_links.stream.xrange("workflow_job")
    assert len(stream_events) == 0

    hash_keys = await redis_links.stream.hkeys("some/key")
    assert b"workflow_job/13403743463" not in hash_keys
    assert len(hash_keys) == 1

    # Try to process again the event, in case GitHub sends an event twice. It
    # shouldn't raise an error if the job is in the database.
    await redis_links.stream.xadd(
        "workflow_job",
        {"workflow_run_key": "some/key", "workflow_job_id": 13403743463},
    )

    await dump.dump_event_stream(redis_links)

    stream_events = await redis_links.stream.xrange("workflow_job")
    assert len(stream_events) == 0


async def test_dump_event_stream_without_organization(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
) -> None:
    await redis_links.stream.xadd(
        "workflow_job",
        {"workflow_run_key": "some/key", "workflow_job_id": 13897999414},
    )
    await redis_links.stream.hset(
        "some/key",
        "workflow_job/13897999414",
        msgpack.packb(
            {
                "event_type": "workflow_job",
                "data": sample_ci_events_to_process[
                    "workflow_job.no_org.json"
                ].slim_event,
            }
        ),
    )
    await redis_links.stream.hset(
        "some/key",
        "workflow_run",
        msgpack.packb(
            {
                "event_type": "workflow_run",
                "data": sample_ci_events_to_process[
                    "workflow_run.no_org.json"
                ].slim_event,
            }
        ),
    )

    with mock.patch(
        "mergify_engine.ci.pull_registries.RedisPullRequestRegistry.get_from_commit",
        return_value=[],
    ), mock.patch("mergify_engine.ci.dump._create_gh_client_from_login"):
        await dump.dump_event_stream(redis_links)

    sql = sqlalchemy.select(github_actions.JobRun)
    result = await db.scalars(sql)
    job_runs = list(result)
    assert len(job_runs) == 1
    actual_job_run = job_runs[0]
    assert actual_job_run.conclusion == github_actions.JobRunConclusion.SUCCESS
