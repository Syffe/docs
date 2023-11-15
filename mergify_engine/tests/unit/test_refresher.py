import msgpack

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import refresher


async def test_refresh_with_pull_request_number(
    redis_stream: redis_utils.RedisStream,
) -> None:
    gh_owner = github_types.GitHubAccount(
        {
            "id": github_types.GitHubAccountIdType(1),
            "login": github_types.GitHubLogin("foo"),
            "type": "User",
            "avatar_url": "",
        },
    )

    gh_repo = github_types.GitHubRepository(
        {
            "id": github_types.GitHubRepositoryIdType(0),
            "owner": gh_owner,
            "full_name": "",
            "archived": False,
            "url": "",
            "html_url": "",
            "default_branch": github_types.GitHubRefType(""),
            "name": github_types.GitHubRepositoryName("test"),
            "private": False,
        },
    )

    await refresher.send_pull_refresh(
        redis_stream,
        gh_repo,
        pull_request_number=github_types.GitHubPullRequestNumber(5),
        action="internal",
        source="test",
    )
    await refresher.send_branch_refresh(
        redis_stream,
        gh_repo,
        ref=github_types.GitHubRefType("master"),
        action="admin",
        source="test",
    )

    keys = await redis_stream.keys("*")
    assert set(keys) == {
        b"bucket~1",
        b"bucket-sources~0~0",
        b"bucket-sources~0~5",
        b"streams",
    }

    messages = await redis_stream.xrange("bucket-sources~0~5")
    assert len(messages) == 1
    event = msgpack.unpackb(messages[0][1][b"source"])["data"]
    assert event["action"] == "internal"
    assert event["ref"] is None
    assert event["pull_request_number"] == 5

    messages = await redis_stream.xrange("bucket-sources~0~0")
    assert len(messages) == 1
    event = msgpack.unpackb(messages[0][1][b"source"])["data"]
    assert event["action"] == "admin"
    assert event["ref"] == "master"
    assert event["pull_request_number"] is None
