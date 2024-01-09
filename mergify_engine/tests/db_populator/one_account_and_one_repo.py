import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine.models import github as gh_models
from mergify_engine.tests.db_populator import DbPopulator


class OneAccountAndOneRepo(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        one_account = gh_models.GitHubAccount(
            id=github_types.GitHubAccountIdType(cls.next_id(gh_models.GitHubAccount)),
            login=github_types.GitHubLogin("OneAccount"),
            type="User",
            avatar_url="https://dummy.com",
        )
        DbPopulator.internal_ref["OneAccount"] = one_account.id

        one_repo = gh_models.GitHubRepository(
            id=github_types.GitHubRepositoryIdType(
                cls.next_id(gh_models.GitHubRepository),
            ),
            name=github_types.GitHubRepositoryName("OneRepo"),
            owner=one_account,
            private=False,
            archived=False,
        )

        session.add(one_repo)

        DbPopulator.internal_ref["OneRepo"] = one_repo.id
