import sqlalchemy.ext.asyncio

from mergify_engine.tests.db_populator import DbPopulator


class AccountAndRepo(DbPopulator):
    @classmethod
    async def _load(cls, session: sqlalchemy.ext.asyncio.AsyncSession) -> None:
        await cls.load(session, {"CollidingRepoName", "OneAccountAndOneRepo"})
