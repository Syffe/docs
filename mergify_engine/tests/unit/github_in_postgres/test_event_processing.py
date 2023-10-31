import typing

import sqlalchemy
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import worker_pusher
from mergify_engine.github_in_postgres import process_events
from mergify_engine.models import github as gh_models


async def test_event_with_missing_data(
    redis_links: redis_utils.RedisLinks,
    db: sqlalchemy.ext.asyncio.AsyncSession,
) -> None:
    invalid_event = typing.cast(
        github_types.GitHubPullRequest,
        {
            "diff_url": "https://github.com/foo/bar/pull/123.diff",
            "html_url": "https://github.com/foo/bar/pull/123",
            "id": 1234567890,
            "issue_url": "https://api.github.com/repos/foo/bar/issues/123",
            "locked": False,
            "node_id": "foobarbarfoo",
            "number": 123,
            "patch_url": "https://github.com/foo/bar/pull/123.patch",
            "state": "closed",
            "url": "https://api.github.com/repos/foo/bar/pulls/123",
        },
    )

    await worker_pusher.push_github_in_pg_event(
        redis_links.stream, "pull_request", "12345", invalid_event
    )

    await process_events.store_redis_events_in_pg(redis_links)

    pull_requests = list(await db.scalars(sqlalchemy.select(gh_models.PullRequest)))
    assert len(pull_requests) == 0
