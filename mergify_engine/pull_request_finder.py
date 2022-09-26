import collections
import dataclasses
import typing

from mergify_engine import context
from mergify_engine import github_types


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

    async def extract_pull_numbers_from_event(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
        event_type: github_types.GitHubEventType,
        data: github_types.GitHubEvent,
    ) -> typing.Set[github_types.GitHubPullRequestNumber]:
        if event_type == "refresh":
            data = typing.cast(github_types.GitHubEventRefresh, data)
            if (pull_request_number := data.get("pull_request_number")) is not None:
                return {pull_request_number}
            elif (ref := data.get("ref")) is not None:
                branch = github_types.GitHubRefType(ref[11:])  # refs/heads/
                return await self._get_pull_numbers_from_repo(repo_id, branch)
            else:
                raise RuntimeError("unsupported refresh event format")

        elif event_type == "push":
            data = typing.cast(github_types.GitHubEventPush, data)
            branch = github_types.GitHubRefType(data["ref"][11:])  # refs/heads/
            return await self._get_pull_numbers_from_repo(repo_id, branch)

        elif event_type == "status":
            data = typing.cast(github_types.GitHubEventStatus, data)
            return await self._get_pull_numbers_from_sha(repo_id, data["sha"])

        elif event_type in ("check_suite", "check_run"):
            info: github_types.GitHubCheckRun | github_types.GitHubCheckSuite
            if event_type == "check_run":
                data = typing.cast(github_types.GitHubEventCheckRun, data)
                info = data["check_run"]
            elif event_type == "check_suite":
                data = typing.cast(github_types.GitHubEventCheckSuite, data)
                info = data["check_suite"]
            else:
                raise RuntimeError(f"impossible event_type: {event_type}")

            # NOTE(sileht): This list may contains Pull Request from another org/user fork...
            pull_numbers = {
                p["number"]
                for p in info["pull_requests"]
                if p["base"]["repo"]["id"] == repo_id
            }
            if not pull_numbers:
                sha = info["head_sha"]
                pull_numbers = await self._get_pull_numbers_from_sha(repo_id, sha)
            return pull_numbers
        else:
            return set()

    async def _get_pull_numbers_from_sha(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
        sha: github_types.SHAType,
    ) -> typing.Set[github_types.GitHubPullRequestNumber]:
        pull_numbers = self.sha_to_pull_numbers.get(sha)
        if pull_numbers is None:
            pulls = typing.cast(
                list[github_types.GitHubPullRequest],
                await self.installation.client.item(
                    f"/repositories/{repo_id}/commits/{sha}/pulls?per_page=100"
                ),
            )
            pull_numbers = {p["number"] for p in pulls if p["state"] == "open"}
            self.sha_to_pull_numbers[sha] = pull_numbers
        return pull_numbers

    async def _get_pull_numbers_from_repo(
        self,
        repo_id: github_types.GitHubRepositoryIdType,
        branch: github_types.GitHubRefType,
    ) -> typing.Set[github_types.GitHubPullRequestNumber]:
        pull_numbers = self.opened_pulls_by_repo_and_branch.get(repo_id, {}).get(branch)
        if pull_numbers is None:
            pulls = [
                p
                async for p in self.installation.client.items(
                    f"/repositories/{repo_id}/pulls?base={branch}",
                    resource_name="pull requests",
                    page_limit=100,
                )
            ]
            pull_numbers = set()
            for p in pulls:
                pull_numbers.add(p["number"])
                self.sha_to_pull_numbers.setdefault(p["head"]["sha"], set()).add(
                    p["number"]
                )
            self.opened_pulls_by_repo_and_branch[repo_id][branch] = pull_numbers
        return pull_numbers
