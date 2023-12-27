from __future__ import annotations

import datetime  # noqa: TCH003
import typing

import pydantic
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models.github import repository as gh_repository_model


class Status(models.Base):
    __tablename__ = "commit_status"

    type_adapter: typing.ClassVar[
        pydantic.TypeAdapter[github_types.GitHubEventStatusBase]
    ] = pydantic.TypeAdapter(github_types.GitHubEventStatusBase)

    __repr_attributes__ = (
        "id",
        "context",
        "state",
        "sha",
    )

    __github_attributes__ = (
        "id",
        "context",
        "state",
        "description",
        "target_url",
        "avatar_url",
        "created_at",
        "updated_at",
    )

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    context: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        index=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    state: orm.Mapped[github_types.GitHubStatusState] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    description: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( words := 7 )",
    )
    target_url: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
    )
    avatar_url: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(created_at, ''1 hour'')",
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(updated_at, ''1 hour'')",
    )
    sha: orm.Mapped[github_types.SHAType] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
        index=True,
    )
    name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
    )
    repo_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
    )

    repository: orm.Mapped[gh_repository_model.GitHubRepository] = orm.relationship(
        lazy="raise_on_sql",
    )

    def as_github_dict(self) -> github_types.GitHubStatus:
        return typing.cast(github_types.GitHubStatus, super().as_github_dict())

    @classmethod
    async def insert_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        data: github_types.GitHubEventStatusBase,
    ) -> None:
        validated_data = cls.type_adapter.validate_python(data)
        # Copy the validated_data in a more generic dict in order to manipulate
        # more easily.
        data_for_obj: dict[str, typing.Any] = validated_data.copy()  # type: ignore[assignment]
        repository_dict = data_for_obj.pop("repository")
        repository = await gh_repository_model.GitHubRepository.get_or_create(
            session,
            repository_dict,
        )
        data_for_obj["repo_id"] = repository.id
        data_for_obj["repository"] = repository

        await session.merge(cls(**data_for_obj))

    @classmethod
    async def get_pull_request_statuses(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull: github_types.GitHubPullRequest,
        repo_id: github_types.GitHubRepositoryIdType,
    ) -> list[github_types.GitHubStatus]:
        statuses = (
            await session.scalars(
                sqlalchemy.select(cls).where(
                    cls.sha == pull["head"]["sha"],
                    cls.repo_id == repo_id,
                ),
            )
        ).all()

        sorted_statuses = sorted(
            statuses,
            key=lambda status: status.created_at,
            reverse=True,
        )
        status_to_return = []
        status_added = set()
        for status in sorted_statuses:
            if status.context in status_added:
                continue

            status_added.add(status.context)
            status_to_return.append(status)

        return [s.as_github_dict() for s in status_to_return]
