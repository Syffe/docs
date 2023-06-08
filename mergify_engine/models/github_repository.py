import datetime

import sqlalchemy
from sqlalchemy import orm

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models import github_account


class GitHubRepository(models.Base):
    __tablename__ = "github_repository"

    id: orm.Mapped[github_types.GitHubRepositoryIdType] = orm.mapped_column(
        sqlalchemy.BigInteger, primary_key=True, autoincrement=False
    )
    owner_id: orm.Mapped[github_types.GitHubAccountIdType] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id")
    )
    last_download_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sqlalchemy.DateTime, nullable=True
    )

    owner: orm.Mapped[github_account.GitHubAccount] = orm.relationship(
        lazy="joined", foreign_keys=[owner_id]
    )
