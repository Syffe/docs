import daiquiri

from mergify_engine import database
from mergify_engine import github_types
from mergify_engine import settings
from mergify_engine.models.github import repository as gh_repo_model


LOG = daiquiri.getLogger(__name__)


# No typing overload for this function so we can pass both at the same time
# and let the function decide which one to use.
async def can_repo_use_github_in_pg_data(
    repo_id: github_types.GitHubRepositoryIdType | None = None,
    repo_owner: github_types.GitHubLogin | None = None,
) -> bool:
    if not settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS:
        return False

    if "*" in settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS:
        return True

    if repo_id is None and repo_owner is None:
        raise RuntimeError(
            "`can_repo_use_github_in_pg_data` needs to have either `repo_id`  or `repo_owner` specified",
        )

    if not repo_owner:
        async with database.create_session() as session:
            repo_obj = await session.get(gh_repo_model.GitHubRepository, repo_id)
            if repo_obj is None:
                LOG.warning(
                    "Could not find repository object in Postgres associated with repository id %s",
                    repo_id,
                )
                return False

            repo_owner = repo_obj.owner.login

    return repo_owner in settings.GITHUB_IN_POSTGRES_USE_PR_IN_PG_FOR_ORGS
