import dataclasses

import msgpack
import pytest

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import models
from mergify_engine.ci import pull_registries


@dataclasses.dataclass
class FakePullRequestRegistry:
    pulls: list[models.PullRequest] | None = None

    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[models.PullRequest]:
        if self.pulls is None:
            return [
                models.PullRequest(
                    id=1234, number=12, title="feat: my awesome feature", state="open"
                )
            ]
        return self.pulls


@pytest.fixture
async def registry(
    redis_links: redis_utils.RedisLinks,
) -> pull_registries.RedisPullRequestRegistry:
    return pull_registries.RedisPullRequestRegistry(
        redis_links.cache, FakePullRequestRegistry()
    )


async def test_get_from_commit(
    registry: pull_registries.RedisPullRequestRegistry,
) -> None:
    owner = github_types.GitHubLogin("some-owner")
    repo = github_types.GitHubRepositoryName("some-repo")
    commit_sha = github_types.SHAType("some-sha")
    await registry.redis.hset(
        registry.cache_key(owner, repo, commit_sha),
        "1234",
        msgpack.packb(
            {
                "id": 1234,
                "number": 12,
                "title": "feat: my awesome feature",
                "state": "open",
            }
        ),
    )

    pulls = await registry.get_from_commit(owner, repo, commit_sha)

    assert len(pulls) == 1
    pull = pulls[0]
    assert pull.id == 1234
    assert pull.number == 12
    assert pull.title == "feat: my awesome feature"


async def test_get_from_commit_without_cache(
    registry: pull_registries.RedisPullRequestRegistry,
) -> None:
    owner = github_types.GitHubLogin("some-owner")
    repo = github_types.GitHubRepositoryName("some-repo")
    commit_sha = github_types.SHAType("some-sha")
    # Pre-condition: cache is empty
    redis_content = await registry.redis.hgetall(
        registry.cache_key(owner, repo, commit_sha)
    )
    assert len(redis_content) == 0

    pulls = await registry.get_from_commit(owner, repo, commit_sha)

    assert len(pulls) == 1
    pull = pulls[0]
    assert pull.id == 1234
    assert pull.number == 12
    assert pull.title == "feat: my awesome feature"

    redis_content = await registry.redis.hgetall(
        registry.cache_key(
            github_types.GitHubLogin("some-owner"),
            github_types.GitHubRepositoryName("some-repo"),
            github_types.SHAType("some-sha"),
        )
    )
    assert len(redis_content) == 1


async def test_register(registry: pull_registries.RedisPullRequestRegistry) -> None:
    owner = github_types.GitHubLogin("some-owner")
    repo = github_types.GitHubRepositoryName("some-repo")
    commit_sha = github_types.SHAType("some-sha")
    # Pre-condition: cache is empty
    redis_content = await registry.redis.hgetall(
        pull_registries.RedisPullRequestRegistry.cache_key(owner, repo, commit_sha)
    )
    assert len(redis_content) == 0

    await pull_registries.RedisPullRequestRegistry.register_commits(
        registry.redis,
        owner,
        repo,
        {commit_sha},
        models.PullRequest(
            id=1234, number=12, title="feat: my awesome feature", state="open"
        ),
    )

    redis_content = await registry.redis.hgetall(
        pull_registries.RedisPullRequestRegistry.cache_key(owner, repo, commit_sha)
    )
    assert len(redis_content) == 1


async def test_get_from_commit_empyt_list(redis_links: redis_utils.RedisLinks) -> None:
    registry = pull_registries.RedisPullRequestRegistry(
        redis_links.cache, FakePullRequestRegistry(pulls=[])
    )
    owner = github_types.GitHubLogin("some-owner")
    repo = github_types.GitHubRepositoryName("some-repo")
    commit_sha = github_types.SHAType("some-sha")

    pulls = await registry.get_from_commit(owner, repo, commit_sha)

    assert len(pulls) == 0
    # Cache is set with a fake pull request
    redis_content = await registry.redis.hgetall(
        registry.cache_key(
            github_types.GitHubLogin("some-owner"),
            github_types.GitHubRepositoryName("some-repo"),
            github_types.SHAType("some-sha"),
        )
    )
    assert len(redis_content) == 1
    assert msgpack.unpackb(redis_content[b"0"]) == {
        "id": 0,
        "number": 0,
        "title": "No pull request associated to this commit",
        "state": "closed",
    }
