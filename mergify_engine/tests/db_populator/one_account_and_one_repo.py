import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github_account
from mergify_engine.models import github_repository
from mergify_engine.tests.db_populator import DbPopulator


class OneAccountAndOneRepo(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        oneAccount = github_account.GitHubAccount(
            id=github_types.GitHubAccountIdType(
                cls.next_id(github_account.GitHubAccount)
            ),
            login=github_types.GitHubLogin("OneAccount"),
            type="User",
        )

        session.add(
            github_repository.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(github_repository.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("OneRepo"),
                owner=oneAccount,
                full_name="OneAccount/OneRepo",
                private=False,
                archived=False,
            )
        )
