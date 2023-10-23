from __future__ import annotations

import typing

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.hybrid

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models.github import account as github_account


class GitHubRepositoryDict(models.ORMObjectAsDict):
    id: github_types.GitHubRepositoryIdType
    owner: github_types.GitHubAccount
    name: github_types.GitHubRepositoryName
    private: bool
    default_branch: github_types.GitHubRefType
    full_name: str
    archived: bool


class GitHubRepository(models.Base):
    __tablename__ = "github_repository"
    __table_args__ = (
        sqlalchemy.Index("github_repository_owner_id_name_idx", "owner_id", "name"),
    )
    __repr_attributes__ = ("id", "full_name")
    __github_attributes__ = (
        "id",
        "owner",
        "name",
        "private",
        "default_branch",
        "full_name",
        "archived",
    )

    id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    owner_id: orm.Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        anonymizer_config=None,
    )

    owner: orm.Mapped[github_account.GitHubAccount] = orm.relationship(
        lazy="joined", foreign_keys=[owner_id]
    )

    name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=False,
        index=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    private: orm.Mapped[bool | None] = orm.mapped_column(
        sqlalchemy.Boolean, nullable=True, anonymizer_config=None
    )
    default_branch: orm.Mapped[github_types.GitHubRefType | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    archived: orm.Mapped[bool | None] = orm.mapped_column(
        sqlalchemy.Boolean, nullable=True, anonymizer_config=None
    )

    @sqlalchemy.ext.hybrid.hybrid_property
    def full_name(self) -> str | None:
        if self.owner is None or self.name is None:
            return None  # type: ignore[unreachable]
        return f"{self.owner.login}/{self.name}"

    @full_name.inplace.expression
    @classmethod
    def _full_name_expression(cls) -> sqlalchemy.ColumnElement[str]:
        return sqlalchemy.type_coerce(
            sqlalchemy.func.concat(
                sqlalchemy.select(github_account.GitHubAccount.login)
                .where(cls.owner_id == github_account.GitHubAccount.id)
                .scalar_subquery(),
                "/",
                cls.name,
            ),
            sqlalchemy.Text,
        )

    @classmethod
    async def get_by_name(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        owner_id: github_types.GitHubAccountIdType,
        name: github_types.GitHubRepositoryName,
    ) -> GitHubRepository | None:
        result = await session.execute(
            sqlalchemy.select(cls).where(cls.owner_id == owner_id, cls.name == name)
        )
        return result.scalar_one_or_none()

    @classmethod
    async def get_or_create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository | GitHubRepositoryDict,
    ) -> GitHubRepository:
        owner = await github_account.GitHubAccount.get_or_create(
            session, repository["owner"]
        )

        result = await session.execute(
            sqlalchemy.select(cls).where(cls.id == repository["id"])
        )
        if (repository_obj := result.scalar_one_or_none()) is not None:
            # NOTE(lecrepont01): update attributes
            repository_obj.name = repository["name"]
            repository_obj.private = repository.get("private")
            repository_obj.default_branch = repository.get("default_branch")
            repository_obj.archived = repository.get("archived")
            return repository_obj

        return cls(
            id=repository["id"],
            name=repository["name"],
            owner=owner,
            private=repository.get("private"),
            default_branch=repository.get("default_branch"),
            archived=repository.get("archived"),
        )

    def as_github_dict(self) -> GitHubRepositoryDict:
        return typing.cast(GitHubRepositoryDict, super().as_github_dict())

    def is_complete(self) -> bool:
        return None not in self.as_github_dict().values()
