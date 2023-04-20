from __future__ import annotations

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from mergify_engine import github_types
from mergify_engine import models


class GitHubAccount(models.Base):
    __tablename__ = "github_account"

    id: orm.Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    login: orm.Mapped[github_types.GitHubLogin] = orm.mapped_column(
        sqlalchemy.Text, unique=True
    )

    @classmethod
    async def create_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        account_id: github_types.GitHubAccountIdType,
        account_login: github_types.GitHubLogin,
    ) -> None:
        sql = (
            postgresql.insert(cls)  # type: ignore [no-untyped-call]
            .values(id=account_id, login=account_login)
            .on_conflict_do_update(
                index_elements=[cls.id],
                set_={"login": account_login},
            )
        )
        await session.execute(sql)
