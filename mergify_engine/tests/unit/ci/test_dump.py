import datetime
import typing
from unittest import mock

from freezegun import freeze_time
import msgpack
import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import dump
from mergify_engine.models import github_account
from mergify_engine.models import github_actions
from mergify_engine.models import github_repository


TODAY = datetime.datetime(2023, 4, 20)
YESTERDAY = datetime.datetime(2023, 4, 19)
DAY_BEFORE_YESTERDAY = datetime.datetime(2023, 4, 18)


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


@freeze_time(TODAY)
async def test_dump_next_repository(
    redis_links: redis_utils.RedisLinks, db: sqlalchemy.ext.asyncio.AsyncSession
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": YESTERDAY,
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
            YESTERDAY.date(),
        )

    result = await db.execute(
        sqlalchemy.select(github_repository.GitHubRepository.last_dump_at)
    )
    row = result.first()
    assert row is not None
    assert row.last_dump_at == TODAY


@freeze_time(TODAY)
async def test_get_next_repository_normal_case(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": YESTERDAY,
            },
            {
                "id": 12,
                "owner_id": 1,
                "last_dump_at": TODAY,
            },
        ],
    )
    await db.commit()

    next_sub = await dump.get_next_repository(db)

    assert next_sub.owner_id == 1
    assert next_sub.owner == "some_owner"
    assert next_sub.repository_id == 11
    assert next_sub.last_dump_at == YESTERDAY


@freeze_time(TODAY)
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
                "last_dump_at": TODAY,
            },
        ],
    )
    await db.commit()

    next_sub = await dump.get_next_repository(db)

    assert next_sub.owner_id == 1
    assert next_sub.owner == "some_owner"
    assert next_sub.repository_id == 11
    assert next_sub.last_dump_at == YESTERDAY


@freeze_time(TODAY)
async def test_get_next_repository_nothing_to_dump(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": TODAY,
            },
        ],
    )
    await db.commit()

    with pytest.raises(dump.NoDataToDump):
        await dump.get_next_repository(db)


@freeze_time(TODAY)
async def test_get_next_repository_not_up_to_date(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "owner_id": 1,
                "last_dump_at": DAY_BEFORE_YESTERDAY,
            },
        ],
    )
    await db.commit()

    next_sub = await dump.get_next_repository(db)

    assert next_sub.owner_id == 1
    assert next_sub.owner == "some_owner"
    assert next_sub.repository_id == 11
    assert next_sub.last_dump_at == DAY_BEFORE_YESTERDAY


async def test_dump_event_stream(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> None:
    await redis_links.stream.xadd(
        "workflow_job",
        {"workflow_run_key": "some/key", "workflow_job_id": "some-job-id"},
    )
    await redis_links.stream.hset(
        "some/key",
        "workflow_job/some-job-id",
        msgpack.packb(
            {
                "event_type": "workflow_job",
                "data": sample_events["workflow_job.completed.json"][1],
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
                "data": sample_events["workflow_run.completed.json"][1],
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
    assert b"workflow_job/some-job-id" not in hash_keys
    assert len(hash_keys) == 1


async def test_dump_event_stream_without_organization(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]],
) -> None:
    await redis_links.stream.xadd(
        "workflow_job",
        {"workflow_run_key": "some/key", "workflow_job_id": "some-job-id"},
    )
    await redis_links.stream.hset(
        "some/key",
        "workflow_job/some-job-id",
        msgpack.packb(
            {
                "event_type": "workflow_job",
                "data": sample_events["workflow_job.no_org.json"][1],
            }
        ),
    )
    await redis_links.stream.hset(
        "some/key",
        "workflow_run",
        msgpack.packb(
            {
                "event_type": "workflow_run",
                "data": sample_events["workflow_run.no_org.json"][1],
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
