import collections
import dataclasses
import datetime
import typing

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine import json
from mergify_engine import redis_utils


class CachedPullRequest(typing.TypedDict):
    number: github_types.GitHubPullRequestNumber
    base_ref: github_types.GitHubRefType
    head_sha: github_types.SHAType


OPENED_PULL_REQUEST_CACHE_EXPIRATION = datetime.timedelta(days=1)


@dataclasses.dataclass
class PullRequestFinder:
    installation: context.Installation
    opened_pulls_by_repo_and_branch: dict[
        github_types.GitHubRepositoryIdType,
        dict[github_types.GitHubRefType, set[github_types.GitHubPullRequestNumber]],
    ] = dataclasses.field(default_factory=lambda: collections.defaultdict(dict))
    sha_to_pull_numbers: dict[
        github_types.SHAType, set[github_types.GitHubPullRequestNumber]
    ] = dataclasses.field(default_factory=dict)

    @staticmethod
    def _cache_key(repo_id: github_types.GitHubRepositoryIdType) -> str:
        return f"pull-requests/{repo_id}"

    async def extract_pull_numbers_from_event(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
        event_type: github_types.GitHubEventType,
        data: github_types.GitHubEvent,
    ) -> set[github_types.GitHubPullRequestNumber]:
        if event_type == "refresh":
            data = typing.cast(github_types.GitHubEventRefresh, data)
            if (pull_request_number := data.get("pull_request_number")) is not None:
                return {pull_request_number}

            if (ref := data.get("ref")) is not None:
                branch = github_types.GitHubRefType(ref[11:])  # refs/heads/
                return await self._get_pull_numbers_from_repo(repo_id, branch)

            raise RuntimeError("unsupported refresh event format")

        if event_type == "push":
            data = typing.cast(github_types.GitHubEventPush, data)
            branch = github_types.GitHubRefType(data["ref"][11:])  # refs/heads/
            return await self._get_pull_numbers_from_repo(repo_id, branch)

        if event_type == "status":
            data = typing.cast(github_types.GitHubEventStatus, data)
            return await self._get_pull_numbers_from_sha(repo_id, data["sha"])

        if event_type in ("check_suite", "check_run"):
            info: github_types.GitHubCheckRun | github_types.GitHubCheckSuite
            if event_type == "check_run":
                data = typing.cast(github_types.GitHubEventCheckRun, data)
                info = data["check_run"]
            elif event_type == "check_suite":
                data = typing.cast(github_types.GitHubEventCheckSuite, data)
                info = data["check_suite"]

            # NOTE(sileht): This list may contains Pull Request from another org/user fork...
            pull_numbers = {
                p["number"]
                for p in info["pull_requests"]
                # NOTE(sileht): engine <= 5.0 doesn't have id set
                if p["base"]["repo"].get("id") == repo_id
            }
            if not pull_numbers:
                sha = info["head_sha"]
                pull_numbers = await self._get_pull_numbers_from_sha(repo_id, sha)
            return pull_numbers

        return set()

    async def _get_pull_numbers_from_sha(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
        sha: github_types.SHAType,
    ) -> set[github_types.GitHubPullRequestNumber]:
        if sha not in self.sha_to_pull_numbers:
            await self._fetch_open_pull_requests(repo_id)

        if sha not in self.sha_to_pull_numbers:
            self.sha_to_pull_numbers[sha] = set()

        return self.sha_to_pull_numbers[sha]

    async def _get_pull_numbers_from_repo(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
        branch: github_types.GitHubRefType,
    ) -> set[github_types.GitHubPullRequestNumber]:
        if branch not in self.opened_pulls_by_repo_and_branch[repo_id]:
            await self._fetch_open_pull_requests(repo_id)

        if branch not in self.opened_pulls_by_repo_and_branch[repo_id]:
            self.opened_pulls_by_repo_and_branch[repo_id][branch] = set()

        return self.opened_pulls_by_repo_and_branch[repo_id][branch]

    @classmethod
    async def sync(
        cls,
        redis: redis_utils.RedisCache,
        pull_request: github_types.GitHubPullRequest,
    ) -> None:
        cache_key = cls._cache_key(pull_request["base"]["repo"]["id"])
        if pull_request["state"] == "open":
            pipe = await redis.pipeline()
            await pipe.hset(
                cache_key,
                str(pull_request["number"]),
                json.dumps(
                    CachedPullRequest(
                        number=pull_request["number"],
                        base_ref=pull_request["base"]["ref"],
                        head_sha=pull_request["head"]["sha"],
                    )
                ),
            )
            await pipe.expire(cache_key, OPENED_PULL_REQUEST_CACHE_EXPIRATION)
            await pipe.execute()
        else:
            await redis.hdel(cache_key, str(pull_request["number"]))

    async def _fetch_open_pull_requests(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
    ) -> None:
        cache_key = self._cache_key(repo_id)
        result = await self.installation.redis.cache.hgetall(cache_key)

        if not result:
            cached_pulls = []
            pipe = await self.installation.redis.cache.pipeline()
            async for p in self.installation.client.items(
                f"/repositories/{repo_id}/pulls",
                resource_name="pull requests",
                page_limit=100,
            ):
                cached_pull = CachedPullRequest(
                    number=p["number"],
                    head_sha=p["head"]["sha"],
                    base_ref=p["base"]["ref"],
                )
                cached_pulls.append(cached_pull)
                await pipe.hset(cache_key, str(p["number"]), json.dumps(cached_pull))
            await pipe.expire(cache_key, OPENED_PULL_REQUEST_CACHE_EXPIRATION)
            await pipe.execute()
        else:
            cached_pulls = [
                typing.cast(CachedPullRequest, json.loads(raw))
                for raw in result.values()
            ]

        for p in cached_pulls:
            self.opened_pulls_by_repo_and_branch[repo_id].setdefault(
                p["base_ref"], set()
            ).add(p["number"])
            self.sha_to_pull_numbers.setdefault(p["head_sha"], set()).add(p["number"])
