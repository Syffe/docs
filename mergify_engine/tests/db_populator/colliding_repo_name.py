import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github_account
from mergify_engine.models import github_repository
from mergify_engine.tests.db_populator import DbPopulator


class CollidingRepoName(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        colliding_acount_1 = github_account.GitHubAccount(
            id=github_types.GitHubAccountIdType(
                cls.next_id(github_account.GitHubAccount)
            ),
            login=github_types.GitHubLogin("colliding_acount_1"),
            type="User",
        )

        session.add(
            github_repository.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(github_repository.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("colliding_repo_name"),
                owner=colliding_acount_1,
                full_name="colliding_acount_1/colliding_repo_name",
                private=False,
                archived=False,
            )
        )

        colliding_acount_2 = github_account.GitHubAccount(
            id=github_types.GitHubAccountIdType(
                cls.next_id(github_account.GitHubAccount)
            ),
            login=github_types.GitHubLogin("colliding_acount_2"),
            type="User",
        )

        session.add(
            github_repository.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(github_repository.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("colliding_repo_name"),
                owner=colliding_acount_2,
                full_name="colliding_acount_2/colliding_repo_name",
                private=False,
                archived=False,
            )
        )
