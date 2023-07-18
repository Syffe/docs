from __future__ import annotations

import datetime

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.orm import Mapped
import sqlalchemy_utils

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models import github_account as github_account_mod
from mergify_engine.models import github_user


class ApplicationKey(models.Base):
    __tablename__ = "application"

    id: Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        primary_key=True,
        nullable=False,
        autoincrement=True,
        anonymizer_config=None,
    )

    name: Mapped[str] = orm.mapped_column(
        sqlalchemy.String(255),
        nullable=False,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    api_access_key: Mapped[str] = orm.mapped_column(
        sqlalchemy.String(255), nullable=False, anonymizer_config="''CONFIDENTIAL''"
    )

    api_secret_key: Mapped[str] = orm.mapped_column(
        sqlalchemy_utils.PasswordType(
            schemes=["pbkdf2_sha512"],
        ),
        nullable=False,
        anonymizer_config="''CONFIDENTIAL''",
    )
    github_account_id: Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.BigInteger,
        sqlalchemy.ForeignKey("github_account.id"),
        index=True,
        nullable=False,
        anonymizer_config=None,
    )
    created_by_github_user_id: Mapped[
        github_types.GitHubAccountIdType | None
    ] = orm.mapped_column(
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("github_user.id"),
        anonymizer_config=None,
    )

    github_account: Mapped[github_account_mod.GitHubAccount] = orm.relationship(
        "GitHubAccount",
        foreign_keys=[github_account_id],
        lazy="immediate",
    )

    created_by: Mapped[github_user.GitHubUser | None] = orm.relationship(
        "GitHubUser",
        uselist=False,
        foreign_keys=[created_by_github_user_id],
        lazy="immediate",
    )

    created_at: Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime,
        server_default=sqlalchemy.func.now(),
        nullable=False,
        anonymizer_config="anon.dnoise(created_at, ''2 days'')",
    )

    @staticmethod
    async def get_by_key(
        session: sqlalchemy.ext.asyncio.AsyncSession,
        api_access_key: str,
        api_secret_key: str,
    ) -> ApplicationKey | None:
        result_application = await session.execute(
            sqlalchemy.select(ApplicationKey).where(
                ApplicationKey.api_access_key == api_access_key
            )
        )
        try:
            application = result_application.unique().scalar_one()
        except sqlalchemy.exc.NoResultFound:
            return None

        if application.api_secret_key != api_secret_key:
            return None

        return application
