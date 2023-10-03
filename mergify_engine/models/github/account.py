from __future__ import annotations

import enum
import typing

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models


class GitHubAccountDict(typing.TypedDict):
    login: github_types.GitHubLogin
    id: github_types.GitHubAccountIdType
    type: github_types.GitHubAccountType


class GitHubAccountType(enum.StrEnum):
    USER = "User"
    ORGANIZATION = "Organization"
    BOT = "Bot"


class GitHubAccount(models.Base):
    __tablename__ = "github_account"
    __github_attributes__ = (
        "id",
        "login",
        "type",
    )

    id: orm.Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    login: orm.Mapped[github_types.GitHubLogin] = orm.mapped_column(
        sqlalchemy.Text,
        unique=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    type: orm.Mapped[GitHubAccountType | None] = orm.mapped_column(
        sqlalchemy.Enum(GitHubAccountType),
        nullable=True,
        anonymizer_config="anon.random_in_enum(type)",
    )

    application_keys_count: orm.Mapped[int] = orm.mapped_column(
        nullable=False, server_default="0", anonymizer_config=None
    )

    @classmethod
    async def create_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        account: github_types.GitHubAccount | GitHubAccountDict,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(id=account["id"], login=account["login"], type=account.get("type"))
            .on_conflict_do_update(
                index_elements=[cls.id],
                set_={"login": account["login"]},
            )
        )
        await session.execute(sql)

    @classmethod
    async def get_or_create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        account: github_types.GitHubAccount | GitHubAccountDict,
    ) -> GitHubAccount:
        result = await session.execute(
            sqlalchemy.select(cls).where(cls.id == account["id"])
        )
        if (account_obj := result.scalar_one_or_none()) is not None:
            # NOTE(lecrepont01): update attributes
            account_obj.login = account["login"]
            if "type" in account:
                account_obj.type = GitHubAccountType(account["type"])
            return account_obj

        return cls(
            id=account["id"],
            login=account["login"],
            type=account.get("type"),
            application_keys_count=0,
        )

    def as_github_dict(self) -> GitHubAccountDict:
        return typing.cast(GitHubAccountDict, super().as_github_dict())
