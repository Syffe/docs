from collections import abc
import dataclasses
import datetime
import typing

import msgpack
from sqlalchemy.dialects import postgresql

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine.ci import models as ci_models
from mergify_engine.clients import github
from mergify_engine.models import github_actions as sql_models


class NotFoundError(Exception):
    pass


class PostgresPullRequestRegistry:
    async def insert(self, new_pull: ci_models.PullRequest) -> None:
        async with database.create_session() as session:
            sql = (
                postgresql.insert(sql_models.PullRequest)  # type: ignore [no-untyped-call]
                .values(
                    id=new_pull.id,
                    number=new_pull.number,
                    title=new_pull.title,
                    state=new_pull.state,
                )
                .on_conflict_do_update(
                    index_elements=[sql_models.PullRequest.id],
                    set_={"number": new_pull.number, "title": new_pull.title},
                )
            )
            await session.execute(sql)
            await session.commit()

    async def register_job_run(self, pull_id: int, job_run: ci_models.JobRun) -> None:
        async with database.create_session() as session:
            sql = (
                postgresql.insert(sql_models.PullRequestJobRunAssociation)  # type: ignore [no-untyped-call]
                .values(pull_request_id=pull_id, job_run_id=job_run.id)
                .on_conflict_do_nothing(
                    index_elements=["pull_request_id", "job_run_id"]
                )
            )
            await session.execute(sql)
            await session.commit()


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
    client: github.AsyncGithubClient

    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[ci_models.PullRequest]:
        # https://docs.github.com/en/rest/commits/commits#list-pull-requests-associated-with-a-commit
        pull_payloads = typing.cast(
            abc.AsyncIterable[github_types.GitHubPullRequest],
            self.client.items(
                f"/repos/{owner}/{repository}/commits/{commit_sha}/pulls",
                resource_name="pulls",
                page_limit=None,
            ),
        )
        pulls = [
            ci_models.PullRequest(
                id=p["id"], number=p["number"], title=p["title"], state=p["state"]
            )
            async for p in pull_payloads
        ]

        return pulls


@dataclasses.dataclass
class RedisPullRequestRegistry:
    """Commit cache is stored in Redis as hashes:

         key                      field: 1234                       field: 3456
                     +--------------------------------------+-----------------------------+
                     | {                                    | {                           |
                     |   "id": 1234,                        |   "id": 3456,               |
    commits/6dcb09b  |   "number": 12,                      |   "number": 34,             |
                     |   "title": "feat: my awesome feature"|   "title": "fix: my bad bug"|
                     | }                                    | }                           |
                     +--------------------------------------+-----------------------------+

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
        result = await self.redis.hgetall(self.cache_key(owner, repository, commit_sha))
        if result:
            return [
                ci_models.PullRequest(**msgpack.unpackb(raw)) for raw in result.values()
            ]

        pulls = await self.source_registry.get_from_commit(
            owner, repository, commit_sha
        )
        await self._store(self.redis, owner, repository, {commit_sha}, pulls)

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
