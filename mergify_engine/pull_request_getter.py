import typing

import daiquiri
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine.clients import github as gh_clients
from mergify_engine.github_in_postgres import utils as ghinpg_utils
from mergify_engine.models.github import pull_request as gh_pull_model


LOG = daiquiri.getLogger(__name__)


@typing.overload
async def _find_pull_in_db(
    pull_number: github_types.GitHubPullRequestNumber,
    *,
    repo_id: github_types.GitHubRepositoryIdType,
) -> github_types.GitHubPullRequest | None:
    ...


@typing.overload
async def _find_pull_in_db(
    pull_number: github_types.GitHubPullRequestNumber,
    *,
    repo_owner: github_types.GitHubLogin,
    repo_name: github_types.GitHubRepositoryName,
) -> github_types.GitHubPullRequest | None:
    ...


async def _find_pull_in_db(
    pull_number: github_types.GitHubPullRequestNumber,
    repo_id: github_types.GitHubRepositoryIdType | None = None,
    repo_owner: github_types.GitHubLogin | None = None,
    repo_name: github_types.GitHubRepositoryName | None = None,
) -> github_types.GitHubPullRequest | None:
    async with database.create_session() as session:
        stmt = sqlalchemy.select(gh_pull_model.PullRequest).where(
            gh_pull_model.PullRequest.number == pull_number,
        )
        if repo_id:
            stmt = stmt.where(
                gh_pull_model.PullRequest.base["repo"]["id"].astext == str(repo_id),
            )
        else:
            stmt = stmt.where(
                gh_pull_model.PullRequest.base["repo"]["owner"]["login"].astext
                == repo_owner,
                gh_pull_model.PullRequest.base["repo"]["name"].astext == repo_name,
            )

        pull = await session.scalar(stmt)
        if pull is not None:
            return pull.as_github_dict()

    return None


async def get_pull_request(
    client: gh_clients.AsyncGitHubClient,
    pull_number: github_types.GitHubPullRequestNumber,
    repo_id: github_types.GitHubRepositoryIdType,
    repo_owner: github_types.GitHubLogin | None = None,
    force_new: bool = False,
    **client_kwargs: typing.Any,
) -> github_types.GitHubPullRequest:
    if not force_new and await ghinpg_utils.can_repo_use_github_in_pg_data(
        repo_id,
        repo_owner,
    ):
        pull_from_db = await _find_pull_in_db(pull_number, repo_id=repo_id)
        if pull_from_db is not None:
            return pull_from_db

    return typing.cast(
        github_types.GitHubPullRequest,
        await client.item(
            f"/repositories/{repo_id}/pulls/{pull_number}",
            **client_kwargs,
        ),
    )
