import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.tests.db_populator import DbPopulator


class CollidingRepoName(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        colliding_account_1 = gh_models.GitHubAccount(
            id=github_types.GitHubAccountIdType(cls.next_id(gh_models.GitHubAccount)),
            login=github_types.GitHubLogin("colliding-account-1"),
            type="User",
            avatar_url="https://dummy.com",
        )

        colliding_repo_1 = gh_models.GitHubRepository(
            id=github_types.GitHubRepositoryIdType(
                cls.next_id(gh_models.GitHubRepository)
            ),
            name=github_types.GitHubRepositoryName("colliding_repo_name"),
            owner=colliding_account_1,
            private=False,
            archived=False,
        )

        DbPopulator.internal_ref["colliding_repo_1"] = colliding_repo_1.id
        session.add(colliding_repo_1)

        colliding_account_2 = gh_models.GitHubAccount(
            id=github_types.GitHubAccountIdType(cls.next_id(gh_models.GitHubAccount)),
            login=github_types.GitHubLogin("colliding-account-2"),
            type="User",
            avatar_url="https://dummy.com",
        )

        colliding_repo_2 = gh_models.GitHubRepository(
            id=github_types.GitHubRepositoryIdType(
                cls.next_id(gh_models.GitHubRepository)
            ),
            name=github_types.GitHubRepositoryName("colliding_repo_name"),
            owner=colliding_account_2,
            private=False,
            archived=False,
        )

        DbPopulator.internal_ref["colliding_repo_2"] = colliding_repo_2.id
        session.add(colliding_repo_2)
