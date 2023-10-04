import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.tests.db_populator import DbPopulator


class OneAccountAndOneRepo(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        oneAccount = gh_models.GitHubAccount(
            id=github_types.GitHubAccountIdType(cls.next_id(gh_models.GitHubAccount)),
            login=github_types.GitHubLogin("OneAccount"),
            type="User",
        )

        session.add(
            gh_models.GitHubRepository(
                id=github_types.GitHubRepositoryIdType(
                    cls.next_id(gh_models.GitHubRepository)
                ),
                name=github_types.GitHubRepositoryName("OneRepo"),
                owner=oneAccount,
                private=False,
                archived=False,
            )
        )
