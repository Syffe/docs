from __future__ import annotations

import datetime  # noqa: TCH003
import typing

import pydantic
import sqlalchemy
from sqlalchemy import func
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from mergify_engine import database
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models.github import repository as gh_repository_model


class CheckRun(models.Base):
    __tablename__ = "check_run"
    __table_args__ = (
        sqlalchemy.Index(
            "check_run_app_id_gin_idx",
            sqlalchemy.text("(app-> 'id')"),
            postgresql_using="gin",
        ),
    )

    __repr_attributes__ = ("id", "name", "head_sha", "status", "conclusion", "repo_id")
    __github_attributes__ = (
        "id",
        "name",
        "app",
        "external_id",
        "head_sha",
        "status",
        "output",
        "conclusion",
        "started_at",
        "completed_at",
        "html_url",
        "details_url",
        "check_suite",
    )

    type_adapter: typing.ClassVar[
        pydantic.TypeAdapter[github_types.GitHubCheckRunWithRepository]
    ] = pydantic.TypeAdapter(github_types.GitHubCheckRunWithRepository)

    github_type_adapter: typing.ClassVar[
        pydantic.TypeAdapter[github_types.GitHubCheckRun]
    ] = pydantic.TypeAdapter(github_types.GitHubCheckRun)

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config="anon.random_int_between(1, 1000000)",
    )
    head_sha: orm.Mapped[github_types.SHAType] = orm.mapped_column(
        sqlalchemy.Text,
        index=True,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
    )
    name: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        index=True,
        anonymizer_config="anon.lorem_ipsum( words := 3 )",
    )
    app: orm.Mapped[github_types.GitHubApp] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 4, ARRAY[''text''])",
    )
    external_id: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 10 )",
    )
    status: orm.Mapped[github_types.GitHubCheckRunStatus] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 10 )",
    )
    output: orm.Mapped[github_types.GitHubCheckRunOutput] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(3, 6, ARRAY[''text''])",
    )
    conclusion: orm.Mapped[
        github_types.GitHubCheckRunConclusion | None
    ] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 10 )",
    )
    started_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        server_default=func.now(),
        default=date.utcnow,
        anonymizer_config="anon.dnoise(started_at, ''2 days'')",
        index=True,
    )
    completed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        server_default=func.now(),
        default=date.utcnow,
        anonymizer_config="anon.dnoise(completed_at, ''2 days'')",
    )
    html_url: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 10 )",
    )
    details_url: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 10 )",
    )
    check_suite: orm.Mapped[github_types.GitHubCheckRunCheckSuite] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(2, 2, ARRAY[''text''])",
    )
    repo_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_repository.id"),
        anonymizer_config=None,
    )

    repository: orm.Mapped[gh_repository_model.GitHubRepository] = orm.relationship(
        lazy="raise_on_sql",
    )

    def as_github_dict(self) -> github_types.GitHubCheckRun:
        _dict = super().as_github_dict()
        # NOTE: We do not store the `pull_requests` of a check run because we never use it.
        # In order to keep the GitHubCheckRun typing without changing everything, we
        # just return an empty `pull_requests` list inside the dict.
        _dict.update({"pull_requests": []})  # type: ignore[misc]
        return typing.cast(github_types.GitHubCheckRun, _dict)

    @classmethod
    async def delete_outdated_check_runs(cls) -> None:
        async with database.create_session() as session:
            to_delete = orm.aliased(cls, name="to_delete")
            reference = orm.aliased(cls, name="reference")

            await session.execute(
                sqlalchemy.delete(to_delete).where(
                    to_delete.name == reference.name,
                    to_delete.head_sha == reference.head_sha,
                    to_delete.repo_id == reference.repo_id,
                    to_delete.app["id"] == reference.app["id"],
                    to_delete.started_at < reference.started_at,
                ),
            )

            await session.commit()

    @classmethod
    async def insert_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        data: github_types.GitHubCheckRunWithRepository,
    ) -> None:
        repo = data.pop("repository")  # type: ignore[misc]

        validated_data = cls.github_type_adapter.validate_python(data)
        # Copy the validated_data in a more generic dict in order to manipulate
        # more easily.
        data_for_obj: dict[str, typing.Any] = validated_data.copy()  # type: ignore[assignment]
        data_for_obj.pop("pull_requests")

        repository = await gh_repository_model.GitHubRepository.get_or_create(
            session,
            repo,
        )
        data_for_obj["repo_id"] = repository.id
        data_for_obj["repository"] = repository

        check_run_obj = cls(**data_for_obj)
        await session.merge(check_run_obj)

    @classmethod
    async def get_checks_as_github_dict(
        cls,
        repo_id: int,
        sha: github_types.SHAType,
        check_name: str | None = None,
        app_id: int | None = None,
    ) -> list[github_types.GitHubCheckRun]:
        stmt = sqlalchemy.select(cls).where(
            cls.head_sha == sha,
            cls.repo_id == repo_id,
        )
        if check_name:
            stmt = stmt.where(
                cls.name == check_name,
            )

        if app_id:
            stmt = stmt.where(
                cls.app["id"].astext == str(app_id),
            )

        async with database.create_session() as session:
            checks = (await session.scalars(stmt)).all()

        checks_sorted = sorted(
            checks,
            key=lambda check: check.started_at,
            reverse=True,
        )
        checks_set = []
        checks_added = set()
        for check in checks_sorted:
            if (check.name, check.app["id"]) not in checks_added:
                checks_added.add((check.name, check.app["id"]))
                checks_set.append(check)

        return [check.as_github_dict() for check in checks_set]
