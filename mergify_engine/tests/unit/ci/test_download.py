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
from mergify_engine.ci import download
from mergify_engine.ci import models
from mergify_engine.ci import pull_registries
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
        "mergify_engine.ci.download._get_repository_name_from_id",
        side_effect=lambda _, _id: repos.get(_id),
    ):
        yield


@freeze_time(NOW)
async def test_download_next_repositories(
    redis_links: redis_utils.RedisLinks, db: sqlalchemy.ext.asyncio.AsyncSession
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "name": "some_repo",
                "owner_id": 1,
                "last_download_at": AN_HOUR_AGO,
            }
        ],
    )
    await db.commit()

    mocked_gh_client = mock.AsyncMock()

    with mock.patch(
        "mergify_engine.ci.download.download"
    ) as mocked_download, mock.patch(
        "mergify_engine.ci.download._create_gh_client_from_login",
        return_value=mocked_gh_client,
    ):
        await download.download_next_repositories(redis_links)

        mocked_download.assert_called_once_with(
            redis_links,
            mocked_gh_client,
            "some_owner",
            "some_repo",
            date.DateTimeRange(AN_HOUR_AGO, NOW),
        )

    result = await db.execute(
        sqlalchemy.select(github_repository.GitHubRepository.last_download_at)
    )
    row = result.first()
    assert row is not None
    assert row.last_download_at == NOW.replace(tzinfo=None)


@freeze_time(NOW)
async def test_get_next_repositories_normal_case(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "name": "some_repo",
                "owner_id": 1,
                "last_download_at": AN_HOUR_AGO,
            },
            {
                "id": 12,
                "name": "some_other_repo",
                "owner_id": 1,
                "last_download_at": NOW,
            },
        ],
    )
    await db.commit()

    repos = await download.get_next_repositories(db)

    assert len(repos) == 1
    repo = repos[0]
    assert repo.owner_id == 1
    assert repo.owner == "some_owner"
    assert repo.repository_id == 11
    assert repo.last_download_at == AN_HOUR_AGO


@freeze_time(NOW)
async def test_get_next_repositories_new_repository(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "name": "some_repo",
                "owner_id": 1,
                "last_download_at": None,
            },
            {
                "id": 12,
                "name": "some_other_repo",
                "owner_id": 1,
                "last_download_at": NOW,
            },
        ],
    )
    await db.commit()

    repos = await download.get_next_repositories(db)

    assert len(repos) == 1
    repo = repos[0]
    assert repo.owner_id == 1
    assert repo.owner == "some_owner"
    assert repo.repository_id == 11
    assert repo.last_download_at == AN_HOUR_AGO


@freeze_time(NOW)
async def test_get_next_repositories_nothing_to_download(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "name": "some_repo",
                "owner_id": 1,
                "last_download_at": LESS_THAN_AN_HOUR_AGO,
            },
        ],
    )
    await db.commit()

    repos = await download.get_next_repositories(db)

    assert len(repos) == 0


@freeze_time(NOW)
async def test_get_next_repositories_not_up_to_date(
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    await db.execute(
        sqlalchemy.insert(github_repository.GitHubRepository),
        [
            {
                "id": 11,
                "name": "some_repo",
                "owner_id": 1,
                "last_download_at": TWO_HOURS_AGO,
            },
        ],
    )
    await db.commit()

    repos = await download.get_next_repositories(db)

    assert len(repos) == 1
    repo = repos[0]
    assert repo.owner_id == 1
    assert repo.owner == "some_owner"
    assert repo.repository_id == 11
    assert repo.last_download_at == TWO_HOURS_AGO


@pytest.fixture
def sample_ci_events_to_process(
    sample_events: dict[str, tuple[github_types.GitHubEventType, typing.Any]]
) -> dict[str, github_events.CIEventToProcess]:
    ci_events = {}

    for filename, (event_type, event) in sample_events.items():
        if event_type in ("workflow_run", "workflow_job"):
            ci_events[filename] = github_events.CIEventToProcess(event_type, "", event)

    return ci_events


@pytest.mark.parametrize(
    "event_filename", ["workflow_run.completed.json", "workflow_run.no_org.json"]
)
async def test_process_event_stream_workflow_run(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    event_filename: str,
    logger_checker: None,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_run",
        "data": msgpack.packb(sample_ci_events_to_process[event_filename].slim_event),
    }
    await redis_links.stream.xadd("gha_workflow_run", stream_event)
    await redis_links.stream.xadd("gha_workflow_run", stream_event)

    with mock.patch.object(
        pull_registries.RedisPullRequestRegistry,
        "get_from_commit",
        return_value=[models.PullRequest(id=1, number=1, title="hello", state="open")],
    ):
        await download.process_event_streams(redis_links)

    workflow_runs = list(
        await db.scalars(sqlalchemy.select(github_actions.WorkflowRun))
    )
    assert len(workflow_runs) == 1
    actual_workflow_run = workflow_runs[0]
    assert actual_workflow_run.event == github_actions.JobRunTriggerEvent.PULL_REQUEST

    pulls = list(await db.scalars(sqlalchemy.select(github_actions.PullRequest)))
    assert len(pulls) == 1
    actual_pull = pulls[0]
    assert actual_pull.id == 1
    assert actual_pull.number == 1
    assert actual_pull.title == "hello"
    assert actual_pull.state == "open"

    associations = list(
        await db.scalars(
            sqlalchemy.select(github_actions.PullRequestWorkflowRunAssociation)
        )
    )
    assert len(associations) == 1
    actual_association = associations[0]
    assert actual_association.pull_request_id == 1

    stream_events = await redis_links.stream.xrange("workflow_run")
    assert len(stream_events) == 0


async def test_process_event_stream_workflow_job(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
    sample_ci_events_to_process: dict[str, github_events.CIEventToProcess],
    logger_checker: None,
) -> None:
    # Create the event twice, as we should handle duplicates
    stream_event = {
        "event_type": "workflow_job",
        "data": msgpack.packb(
            sample_ci_events_to_process["workflow_job.completed.json"].slim_event
        ),
    }
    await redis_links.stream.xadd("gha_workflow_job", stream_event)
    await redis_links.stream.xadd("gha_workflow_job", stream_event)

    await download.process_event_streams(redis_links)

    sql = sqlalchemy.select(github_actions.WorkflowJob)
    result = await db.scalars(sql)
    workflow_jobs = list(result)
    assert len(workflow_jobs) == 1
    actual_workflow_job = workflow_jobs[0]
    assert actual_workflow_job.conclusion == github_actions.JobRunConclusion.SUCCESS
    assert actual_workflow_job.labels == ["ubuntu-20.04"]

    stream_events = await redis_links.stream.xrange("workflow_job")
    assert len(stream_events) == 0
