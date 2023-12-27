import typing

import pydantic
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models.github import account as gh_account_model


if typing.TYPE_CHECKING:
    from mergify_engine.models.github.pull_request import PullRequest


class PullRequestCommit(models.Base):
    __tablename__ = "pull_request_commit"

    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = (
        "id",
        "sha",
    )

    __github_attributes__ = (
        "sha",
        "parents",
        "commit",
        "committer",
        "author",
    )

    type_adapter: typing.ClassVar[
        pydantic.TypeAdapter[github_types.GitHubBranchCommit]
    ] = pydantic.TypeAdapter(github_types.GitHubBranchCommit)

    # used only for ordering commits
    # commit index in the pull request, first commit (after base branch) = index 1
    index: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        nullable=False,
        anonymizer_config=None,
    )

    sha: orm.Mapped[github_types.SHAType] = orm.mapped_column(
        sqlalchemy.Text,
        primary_key=True,
        index=True,
        anonymizer_config="anon.lorem_ipsum( characters := 15 )",
    )
    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"),
        primary_key=True,
        index=True,
        anonymizer_config=None,
    )
    parents: orm.Mapped[
        list[github_types.GitHubBranchCommitParent]
    ] = orm.mapped_column(
        postgresql.ARRAY(postgresql.JSONB, dimensions=1),
        anonymizer_config="custom_masks.lorem_ipsum_array(0, 5, 20)",
    )
    commit: orm.Mapped[github_types.GitHubBranchCommitCommit] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(2, 2, ARRAY[''text''])",
    )

    committer_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        nullable=True,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    committer: orm.Mapped[gh_account_model.GitHubAccount | None] = orm.relationship(
        lazy="joined",
        foreign_keys=[committer_id],
    )
    author_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        nullable=True,
        anonymizer_config="anon.random_int_between(1,100000)",
    )

    author: orm.Mapped[gh_account_model.GitHubAccount | None] = orm.relationship(
        lazy="joined",
        foreign_keys=[author_id],
    )
    pull_request: orm.Mapped["PullRequest"] = orm.relationship(
        back_populates="head_commits",
    )

    @classmethod
    async def insert_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        data: github_types.GitHubBranchCommit,
        pull_request_id: int,
        index: int,
    ) -> None:
        validated_data = cls.type_adapter.validate_python(data)
        # Copy the validated_data in a more generic dict in order to manipulate
        # more easily.
        data_for_obj: dict[str, typing.Any] = validated_data.copy()  # type: ignore[assignment]

        committer = data_for_obj.pop("committer")
        if committer is not None:
            data_for_obj["committer_id"] = committer["id"]
            await gh_account_model.GitHubAccount.create_or_update(session, committer)
        else:
            data_for_obj["committer_id"] = None

        author = data_for_obj.pop("author", None)
        data_for_obj["author_id"] = author["id"] if author else None

        if author:
            await gh_account_model.GitHubAccount.create_or_update(session, author)

        commit_obj = cls(
            **data_for_obj,
            pull_request_id=pull_request_id,
            index=index,
        )
        await session.merge(commit_obj)

    def as_github_dict(self) -> github_types.GitHubBranchCommit:
        return typing.cast(github_types.GitHubBranchCommit, super().as_github_dict())

    @classmethod
    async def get_pull_request_commits(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull_request_id: int,
        pull_head_sha: github_types.SHAType,
    ) -> list[github_types.GitHubBranchCommit]:
        # Circular import
        from mergify_engine.models.github.pull_request import PullRequest

        stmt = (
            sqlalchemy.select(cls)
            .join(PullRequest, PullRequest.id == cls.pull_request_id)
            .where(
                PullRequest.id == pull_request_id,
                PullRequest.head["sha"].astext == pull_head_sha,
            )
            # last commit first
            .order_by(cls.index.desc())
        )

        return [
            commit.as_github_dict() for commit in (await session.scalars(stmt)).all()
        ]
