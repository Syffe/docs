from collections import abc
import dataclasses
import typing

import sqlalchemy

from mergify_engine import github_types
from mergify_engine.clients import github
from mergify_engine.models.github import pull_request as gh_pull_request_mod


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
    ) -> list[github_types.GitHubPullRequest]:
        # https://docs.github.com/en/rest/commits/commits#list-pull-requests-associated-with-a-commit
        client = await self.get_client()

        pulls = typing.cast(
            abc.AsyncIterable[github_types.GitHubPullRequest],
            client.items(
                f"/repos/{owner}/{repository}/commits/{commit_sha}/pulls",
                resource_name="pulls",
                page_limit=None,
            ),
        )

        return [
            typing.cast(
                github_types.GitHubPullRequest,
                await client.item(
                    f"/repos/{owner}/{repository}/pulls/{pull['number']}"
                ),
            )
            async for pull in pulls
        ]


@dataclasses.dataclass
class PostgresPullRequestRegistry:
    database_session: sqlalchemy.ext.asyncio.AsyncSession
    http_pull_registry: HTTPPullRequestRegistry

    async def get_from_commit(
        self,
        owner: github_types.GitHubLogin,
        repository: github_types.GitHubRepositoryName,
        commit_sha: github_types.SHAType,
    ) -> list[gh_pull_request_mod.PullRequest]:
        pulls_from_db = (
            await self.database_session.scalars(
                sqlalchemy.select(gh_pull_request_mod.PullRequest).where(
                    gh_pull_request_mod.PullRequest.head["sha"].astext == commit_sha
                )
            )
        ).all()

        if pulls_from_db:
            return typing.cast(list[gh_pull_request_mod.PullRequest], pulls_from_db)

        pulls_from_github = await self.http_pull_registry.get_from_commit(
            owner, repository, commit_sha
        )

        pulls_validated_typing = [
            gh_pull_request_mod.PullRequest.type_adapter.validate_python(pull)
            for pull in pulls_from_github
        ]
        new_inserted_pulls = (
            await self.database_session.scalars(
                sqlalchemy.insert(gh_pull_request_mod.PullRequest).returning(
                    gh_pull_request_mod.PullRequest
                ),
                pulls_validated_typing,
            )
        ).all()

        return typing.cast(list[gh_pull_request_mod.PullRequest], new_inserted_pulls)
