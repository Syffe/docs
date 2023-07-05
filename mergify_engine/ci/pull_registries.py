from collections import abc
import dataclasses
import datetime
import typing

import msgpack

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import models as ci_models
from mergify_engine.clients import github


class PullRequestFromCommitRegistry(typing.Protocol):
    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[ci_models.PullRequest]:
        ...


@dataclasses.dataclass
class HTTPPullRequestRegistry:
    login: github_types.GitHubLogin | None = None
    client: github.AsyncGitHubClient | None = None
    _installation: github_types.GitHubInstallation | None = None

    async def get_client(self) -> github.AsyncGitHubClient:
        if self.client is not None:
            return self.client

        if self._installation is None and self.login is not None:
            self._installation = await github.get_installation_from_login(self.login)

        if self._installation is None:
            raise RuntimeError("client or login not provided")

        auth = github.GitHubAppInstallationAuth(self._installation)
        return github.AsyncGitHubInstallationClient(auth=auth)

    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[ci_models.PullRequest]:
        # https://docs.github.com/en/rest/commits/commits#list-pull-requests-associated-with-a-commit
        client = await self.get_client()
        pull_payloads = typing.cast(
            abc.AsyncIterable[github_types.GitHubPullRequest],
            client.items(
                f"/repos/{owner}/{repository}/commits/{commit_sha}/pulls",
                resource_name="pulls",
                page_limit=None,
            ),
        )
        return [
            ci_models.PullRequest(
                id=p["id"], number=p["number"], title=p["title"], state=p["state"]
            )
            async for p in pull_payloads
        ]


@dataclasses.dataclass
class RedisPullRequestRegistry:
    """Commit cache is stored in Redis as hashes:

         key                                                  field: 1234                       field: 3456
    ┌──────────────────────────────────────────┬──────────────────────────────────────┬─────────────────────────────┐
    |                                          | {                                    | {                           |
    |                                          |   "id": 1234,                        |   "id": 3456,               |
    |commits/<owner>/<repository>/<commit_sha> |   "number": 12,                      |   "number": 34,             |
    |                                          |   "title": "feat: my awesome feature"|   "title": "fix: my bad bug"|
    |                                          | }                                    | }                           |
    └──────────────────────────────────────────┴──────────────────────────────────────┴─────────────────────────────┘

    """

    redis: redis_utils.RedisCache
    source_registry: PullRequestFromCommitRegistry
    CACHE_EXPIRATION: typing.ClassVar[datetime.timedelta] = datetime.timedelta(days=30)

    @staticmethod
    def cache_key(
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> str:
        return f"commits/{owner}/{repository}/{commit_sha}"

    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[ci_models.PullRequest]:
        cache = await self.redis.hgetall(self.cache_key(owner, repository, commit_sha))
        if cache:
            return self._parse(cache)

        pulls = await self.source_registry.get_from_commit(
            owner, repository, commit_sha
        )
        await self._store(self.redis, owner, repository, {commit_sha}, pulls)

        return pulls

    def _parse(self, cache: dict[bytes, bytes]) -> list[ci_models.PullRequest]:
        pulls = []
        for raw in cache.values():
            data = msgpack.unpackb(raw)
            if data["id"]:
                pulls.append(ci_models.PullRequest(**data))
        return pulls

    @classmethod
    async def _store(
        cls,
        redis: redis_utils.RedisCache,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_shas: set[github_types.SHAType],
        pulls: list[ci_models.PullRequest],
    ) -> None:
        # NOTE(charly): no pull requests means that the commit is not associated
        # to any. We store a fake pull request to return an empty list and avoid
        # doint HTTP request later.
        if not pulls:
            pulls = [
                ci_models.PullRequest(
                    0, 0, "No pull request associated to this commit", state="closed"
                )
            ]

        for commit_sha in commit_shas:
            cache_key = cls.cache_key(owner, repository, commit_sha)
            pipe = await redis.pipeline()
            for pull in pulls:
                await pipe.hset(
                    cache_key,
                    str(pull.id),
                    msgpack.packb(dataclasses.asdict(pull)),
                )
            await pipe.expire(cache_key, cls.CACHE_EXPIRATION)
        await pipe.execute()

    @classmethod
    async def register_commits(
        cls,
        redis: redis_utils.RedisCache,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_shas: set[github_types.SHAType],
        pull_request: ci_models.PullRequest,
    ) -> None:
        await cls._store(redis, owner, repository, commit_shas, [pull_request])
