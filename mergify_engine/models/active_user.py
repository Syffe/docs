from __future__ import annotations

import datetime
import typing

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import models


# NOTE(sileht): keep more than needed to be able to answer invoice questions on support
ACTIVE_USERS_EVENTS_EXPIRATION = datetime.timedelta(days=90)


class ActiveUser(models.Base):
    __tablename__ = "active_user"
    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = (
        "repository_id",
        "user_account_id",
        "last_seen_at",
    )

    repository_id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.BigInteger,
        sqlalchemy.ForeignKey("github_repository.id"),
        primary_key=True,
        nullable=False,
        anonymizer_config=None,
    )
    # Account of the user, not the repository owner
    user_github_account_id: orm.Mapped[
        github_types.GitHubAccountIdType
    ] = orm.mapped_column(
        sqlalchemy.BigInteger,
        sqlalchemy.ForeignKey("github_account.id"),
        primary_key=True,
        nullable=False,
        anonymizer_config=None,
    )

    last_seen_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        anonymizer_config="anon.dnoise(last_seen_at, ''2 days'')",
    )

    last_event: orm.Mapped[dict[str, typing.Any]] = orm.mapped_column(
        postgresql.JSONB,
        nullable=False,
        anonymizer_config="custom_masks.jsonb_obj(2, 2, ARRAY[''text''])",
    )

    @classmethod
    async def track(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        repository_id: github_types.GitHubRepositoryIdType,
        user_github_account_id: github_types.GitHubAccountIdType,
        last_event: typing.Any,
    ) -> None:
        now = date.utcnow()
        sql = (
            postgresql.insert(cls)
            .values(
                repository_id=repository_id,
                user_github_account_id=user_github_account_id,
                last_seen_at=now,
                last_event=last_event,
            )
            .on_conflict_do_update(
                index_elements=[cls.user_github_account_id, cls.repository_id],
                set_={"last_seen_at": now, "last_event": last_event},
            )
        )
        await session.execute(sql)

    @classmethod
    async def delete_outdated(cls) -> None:
        async with database.create_session() as session:
            await session.execute(
                sqlalchemy.delete(cls).where(
                    cls.last_seen_at < (date.utcnow() - ACTIVE_USERS_EVENTS_EXPIRATION),
                ),
            )
