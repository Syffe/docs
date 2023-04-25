import datetime
import typing
from unittest import mock

from freezegun import freeze_time
import pytest
import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import redis_utils
from mergify_engine.ci import dump
from mergify_engine.models import github_account
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
