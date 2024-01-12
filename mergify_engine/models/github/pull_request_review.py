import datetime
import typing

import pydantic
import sqlalchemy
from sqlalchemy import orm

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.models.github import account as gh_account_model


if typing.TYPE_CHECKING:
    from mergify_engine.models.github.pull_request import PullRequest


class PullRequestReview(models.Base):
    __tablename__ = "pull_request_review"

    __repr_attributes__ = ("id", "state", "submitted_at", "commit_id")
    __github_attributes__ = (
        "id",
        "user",
        "body",
        "state",
        "author_association",
        "submitted_at",
        "commit_id",
    )
    type_adapter: typing.ClassVar[
        pydantic.TypeAdapter[github_types.GitHubEventPullRequestReviewForPostgres]
    ] = pydantic.TypeAdapter(github_types.GitHubEventPullRequestReviewForPostgres)

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config="anon.random_int_between(1, 1000000)",
    )
    user_id: orm.Mapped[int | None] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        nullable=True,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    user: orm.Mapped[gh_account_model.GitHubAccount | None] = orm.relationship(
        lazy="joined",
        foreign_keys=[user_id],
    )
    body: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( words := 30 )",
    )
    state: orm.Mapped[github_types.GitHubReviewStateType] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    author_association: orm.Mapped[
        github_types.GitHubCommentAuthorAssociation
    ] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    submitted_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        index=True,
        anonymizer_config="anon.dnoise(submitted_at, ''1 hour'')",
    )
    commit_id: orm.Mapped[github_types.SHAType | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"),
        index=True,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    pull_request: orm.Mapped["PullRequest"] = orm.relationship(
        lazy="raise_on_sql",
    )

    def as_github_dict(self) -> github_types.GitHubReview:
        return typing.cast(github_types.GitHubReview, super().as_github_dict())

    @classmethod
    async def insert_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        data: github_types.GitHubEventPullRequestReviewForPostgres,
    ) -> None:
        validated_data = cls.type_adapter.validate_python(data)
        # Copy the validated_data in a more generic dict in order to manipulate
        # more easily.
        data_for_obj: dict[str, typing.Any] = validated_data.copy()  # type: ignore[assignment]

        data_for_obj.pop("user", None)
        # Use the one from validated_data to keep typing
        user_dict = validated_data.get("user")
        if user_dict is not None:
            data_for_obj["user"] = await gh_account_model.GitHubAccount.get_or_create(
                session,
                user_dict,
            )
            data_for_obj["user_id"] = data_for_obj["user"].id
        else:
            data_for_obj["user_id"] = None

        await session.merge(cls(**data_for_obj))

    @classmethod
    async def get_pull_request_reviews(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull_request_id: github_types.GitHubPullRequestId,
    ) -> list[github_types.GitHubReview]:
        stmt = (
            sqlalchemy.select(cls)
            .where(cls.pull_request_id == pull_request_id)
            .order_by(cls.submitted_at.desc())
        )
        reviews_objs = (await session.scalars(stmt)).all()

        # Reviews are ordered by latest `submitted_at` first.
        # We still need to filter out outdated reviews based on user_id.
        reviews: list[github_types.GitHubReview] = []
        existing_user_ids = set()
        for review in reviews_objs:
            if review.user_id in existing_user_ids:
                continue

            existing_user_ids.add(review.user_id)
            reviews.append(review.as_github_dict())

        return reviews
