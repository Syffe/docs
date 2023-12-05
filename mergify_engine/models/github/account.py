from __future__ import annotations

import enum
import typing

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models


if typing.TYPE_CHECKING:
    # Need to import directly the PullRequest object otherwise sqlalchemy throws
    # an error saying it can't find the "<name_of_import>.PullRequest"
    from mergify_engine.models.github.pull_request import PullRequest


class GitHubAccountType(enum.StrEnum):
    USER = "User"
    ORGANIZATION = "Organization"
    BOT = "Bot"


class GitHubAccount(models.Base):
    __tablename__ = "github_account"
    __repr_attributes__ = ("id", "login", "type")
    __github_attributes__ = (
        "id",
        "login",
        "type",
        "avatar_url",
    )

    id: orm.Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    login: orm.Mapped[github_types.GitHubLogin] = orm.mapped_column(
        sqlalchemy.Text,
        index=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    type: orm.Mapped[GitHubAccountType | None] = orm.mapped_column(
        sqlalchemy.Enum(GitHubAccountType),
        nullable=True,
        anonymizer_config="anon.random_in_enum(type)",
    )
    application_keys_count: orm.Mapped[int] = orm.mapped_column(
        nullable=False,
        server_default="0",
        anonymizer_config=None,
    )
    avatar_url: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )

    # We need late evaluation of imports on these 2, otherwise we get a circular import
    _pull_request_assignees: orm.Mapped[
        list["PullRequest"]  # noqa: UP037
    ] = orm.relationship(
        secondary="at_pull_request_assignees_github_account",
        back_populates="assignees",
        viewonly=True,
    )

    _pull_request_requested_reviewers: orm.Mapped[
        list["PullRequest"]  # noqa: UP037
    ] = orm.relationship(
        secondary="at_pull_request_assignees_github_account",
        back_populates="requested_reviewers",
        viewonly=True,
    )

    @classmethod
    async def create_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        account: github_types.GitHubAccount,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(
                id=account["id"],
                login=account["login"],
                type=account.get("type"),
                avatar_url=account.get(
                    "avatar_url",
                    cls.build_avatar_url(account["id"]),
                ),
            )
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
        account: github_types.GitHubAccount,
    ) -> GitHubAccount:
        result = await session.execute(
            sqlalchemy.select(cls).where(cls.id == account["id"]),
        )
        if (account_obj := result.scalar_one_or_none()) is not None:
            # NOTE(lecrepont01): update attributes
            account_obj.login = account["login"]
            account_obj.avatar_url = account.get(
                "avatar_url",
                cls.build_avatar_url(account["id"]),
            )
            if "type" in account:
                account_obj.type = GitHubAccountType(account["type"])
            return account_obj

        return cls(
            id=account["id"],
            login=account["login"],
            type=account.get("type"),
            avatar_url=account.get("avatar_url", cls.build_avatar_url(account["id"])),
            application_keys_count=0,
        )

    def as_github_dict(self) -> github_types.GitHubAccount:
        return typing.cast(github_types.GitHubAccount, super().as_github_dict())

    @staticmethod
    def build_avatar_url(account_id: int) -> str:
        return f"https://avatars.githubusercontent.com/u/{account_id}?v=4"
