from __future__ import annotations

import sqlalchemy
from sqlalchemy import orm
import sqlalchemy.ext.asyncio

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models import github_account


class GitHubRepository(models.Base):
    __tablename__ = "github_repository"

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
    full_name: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    archived: orm.Mapped[bool | None] = orm.mapped_column(
        sqlalchemy.Boolean, nullable=True, anonymizer_config=None
    )

    @classmethod
    async def get_or_create(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository: github_types.GitHubRepository,
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
            return repository_obj

        return cls(id=repository["id"], name=repository["name"], owner=owner)
