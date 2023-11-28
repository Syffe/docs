import typing

import daiquiri
import sqlalchemy

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.clients import github as gh_clients
from mergify_engine.models.github import pull_request as gh_pull_model
from mergify_engine.models.github import repository as gh_repo_model


LOG = daiquiri.getLogger(__name__)


# No typing overload for this function so we can pass both at the same time
# and let the function decide which one to use.
async def can_repo_use_pull_requests_in_pg(
    repo_id: github_types.GitHubRepositoryIdType | None = None,
    repo_owner: github_types.GitHubLogin | None = None,
) -> bool:
    if not settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS:
        return False

    if "*" in settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS:
        return True

    if repo_id is None and repo_owner is None:
        raise RuntimeError(
            "`can_repo_use_pull_requests_in_pg` needs to have either `repo_id`  or `repo_owner` specified",
        )

    if not repo_owner:
        async with database.create_session() as session:
            repo_obj = await session.scalar(
                sqlalchemy.select(gh_repo_model.GitHubRepository).where(
                    gh_repo_model.GitHubRepository.id == repo_id,
                ),
            )
            if repo_obj is None:
                LOG.warning(
                    "Could not find repository object in Postgres associated with repository id %s",
                    repo_id,
                )
                return False

            repo_owner = repo_obj.owner.login

    return repo_owner in settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS


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


@typing.overload
async def get_pull_request(
    client: gh_clients.AsyncGitHubClient,
    pull_number: github_types.GitHubPullRequestNumber,
    *,
    repo_id: github_types.GitHubRepositoryIdType,
    force_new: bool = False,
    **client_kwargs: typing.Any,
) -> github_types.GitHubPullRequest:
    ...


@typing.overload
async def get_pull_request(
    client: gh_clients.AsyncGitHubClient,
    pull_number: github_types.GitHubPullRequestNumber,
    *,
    repo_owner: github_types.GitHubLogin,
    repo_name: github_types.GitHubRepositoryName,
    force_new: bool = False,
    **client_kwargs: typing.Any,
) -> github_types.GitHubPullRequest:
    ...


async def get_pull_request(
    client: gh_clients.AsyncGitHubClient,
    pull_number: github_types.GitHubPullRequestNumber,
    repo_id: github_types.GitHubRepositoryIdType | None = None,
    repo_owner: github_types.GitHubLogin | None = None,
    repo_name: github_types.GitHubRepositoryName | None = None,
    force_new: bool = False,
    **client_kwargs: typing.Any,
) -> github_types.GitHubPullRequest:
    if not force_new and await can_repo_use_pull_requests_in_pg(
        repo_id,
        repo_owner,
    ):
        if repo_id:
            pull_from_db = await _find_pull_in_db(pull_number, repo_id=repo_id)
        elif repo_owner and repo_name:
            pull_from_db = await _find_pull_in_db(
                pull_number,
                repo_owner=repo_owner,
                repo_name=repo_name,
            )
        else:
            raise RuntimeError(
                "`get_pull_request` needs to have either `repo_id` or both `repo_owner` and `repo_name` specified",
            )

        if pull_from_db is not None:
            return pull_from_db

    if repo_id:
        return typing.cast(
            github_types.GitHubPullRequest,
            await client.item(
                f"/repositories/{repo_id}/pulls/{pull_number}",
                **client_kwargs,
            ),
        )

    return typing.cast(
        github_types.GitHubPullRequest,
        await client.item(
            f"/repos/{repo_owner}/{repo_name}/pulls/{pull_number}",
            **client_kwargs,
        ),
    )
