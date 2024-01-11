import typing

import pydantic
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import github_types
from mergify_engine import models


class PullRequestFile(models.Base):
    __tablename__ = "pull_request_file"

    type_adapter: pydantic.TypeAdapter[github_types.GitHubFile] = pydantic.TypeAdapter(
        github_types.GitHubFile,
    )

    __repr_attributes = ("sha", "filename", "changes", "status")

    __github_attributes__ = (
        "sha",
        "filename",
        "status",
        "additions",
        "deletions",
        "changes",
        "contents_url",
        "blob_url",
        "raw_url",
        "patch",
        "previous_filename",
    )

    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"),
        primary_key=True,
        anonymizer_config=None,
    )
    pull_request_head_sha: orm.Mapped[github_types.SHAType] = orm.mapped_column(
        sqlalchemy.Text,
        primary_key=True,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
    )
    sha: orm.Mapped[github_types.SHAType | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 15 )",
    )
    filename: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        primary_key=True,
        anonymizer_config="anon.lorem_ipsum( characters := 15 )",
    )
    status: orm.Mapped[github_types.GitHubFileStatus] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 15 )",
    )
    additions: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(0, 50)",
    )
    deletions: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(0, 50)",
    )
    changes: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(0, 50)",
    )
    blob_url: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 30 )",
    )
    raw_url: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 30 )",
    )
    contents_url: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 30 )",
    )
    patch: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 30 )",
    )
    previous_filename: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 15 )",
    )

    def as_github_dict(self) -> github_types.GitHubFile:
        return typing.cast(github_types.GitHubFile, super().as_github_dict())

    @classmethod
    async def insert_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        data: github_types.GitHubFile,
        pull_request_id: int,
        pull_request_head_sha: github_types.SHAType,
    ) -> None:
        validated_data = cls.type_adapter.validate_python(data)
        # Copy the validated_data in a more generic dict in order to manipulate
        # more easily.
        data_for_obj: dict[str, typing.Any] = validated_data.copy()  # type: ignore[assignment]
        data_for_obj["pull_request_id"] = pull_request_id
        data_for_obj["pull_request_head_sha"] = pull_request_head_sha

        await session.merge(cls(**data_for_obj))

    @classmethod
    async def get_pull_request_files(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull_request_id: int,
        pull_request_head_sha: github_types.SHAType,
    ) -> list[github_types.GitHubFile]:
        stmt = sqlalchemy.select(cls).where(
            cls.pull_request_id == pull_request_id,
            cls.pull_request_head_sha == pull_request_head_sha,
        )

        return [file.as_github_dict() for file in (await session.scalars(stmt)).all()]
