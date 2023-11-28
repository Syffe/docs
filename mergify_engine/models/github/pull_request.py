from collections import abc
import datetime
import typing

import daiquiri
import pydantic
import pydantic_core
import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.hybrid

from mergify_engine import github_types
from mergify_engine import models
from mergify_engine.clients import github
from mergify_engine.clients import http
from mergify_engine.models.github import account as gh_account_model
from mergify_engine.models.github import pull_request_commit as pr_commit_model


LOG = daiquiri.getLogger(__name__)


class PullRequest(models.Base):
    __tablename__ = "pull_request"

    __repr_attributes__: typing.ClassVar[tuple[str, ...]] = (
        "id",
        "number",
        "state",
    )
    __github_attributes__ = (
        "id",
        "number",
        "base",
        "head",
        "state",
        "user",
        "labels",
        "merged_at",
        "draft",
        "merge_commit_sha",
        "html_url",
        "issue_url",
        "title",
        "body",
        "locked",
        "assignees",
        "requested_reviewers",
        "requested_teams",
        "milestone",
        "updated_at",
        "created_at",
        "closed_at",
        "node_id",
        "maintainer_can_modify",
        "merged",
        "merged_by",
        "rebaseable",
        "mergeable",
        "mergeable_state",
        "changed_files",
        "commits",
    )

    type_adapter: typing.ClassVar[
        pydantic.TypeAdapter[github_types.GitHubPullRequest]
    ] = pydantic.TypeAdapter(github_types.GitHubPullRequest)

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        index=True,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    title: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 5 )",
    )
    state: orm.Mapped[github_types.GitHubPullRequestState] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
    # ##########
    body: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( words := 30 )",
    )

    base: orm.Mapped[github_types.GitHubBaseBranchRef] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(2, 2, ARRAY[''text''])",
    )

    head: orm.Mapped[github_types.GitHubHeadBranchRef] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(2, 2, ARRAY[''text''])",
    )
    user_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    user: orm.Mapped[gh_account_model.GitHubAccount] = orm.relationship(
        lazy="joined",
        foreign_keys=[user_id],
    )
    labels: orm.Mapped[list[github_types.GitHubLabel]] = orm.mapped_column(
        sqlalchemy.ARRAY(postgresql.JSONB, dimensions=1),
        anonymizer_config="custom_masks.json_obj_array(0, 3, ARRAY['id', 'name', 'color', 'default']",
    )
    draft: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    html_url: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
    )
    issue_url: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 20 )",
    )

    merged_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        anonymizer_config="anon.dnoise(merged_at, ''1 hour'')",
    )
    updated_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(updated_at, ''1 hour'')",
    )
    created_at: orm.Mapped[datetime.datetime] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        anonymizer_config="anon.dnoise(created_at, ''1 hour'')",
    )
    closed_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        anonymizer_config="anon.dnoise(closed_at, ''1 hour'')",
    )
    node_id: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 10 )",
    )

    locked: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    assignees: orm.Mapped[list[gh_account_model.GitHubAccount]] = orm.relationship(
        lazy="immediate",
        secondary="at_pull_request_assignees_github_account",
        back_populates="_pull_request_assignees",
        viewonly=True,
    )
    requested_reviewers: orm.Mapped[
        list[gh_account_model.GitHubAccount]
    ] = orm.relationship(
        lazy="immediate",
        secondary="at_pull_request_requested_reviewers_github_account",
        back_populates="_pull_request_requested_reviewers",
        viewonly=True,
    )
    requested_teams: orm.Mapped[list[github_types.GitHubTeam]] = orm.mapped_column(
        sqlalchemy.ARRAY(postgresql.JSONB, dimensions=1),
        anonymizer_config="custom_masks.json_obj_array(0, 2, ARRAY['slug'])",
    )
    milestone: orm.Mapped[github_types.GitHubMilestone | None] = orm.mapped_column(
        postgresql.JSONB,
        nullable=True,
        anonymizer_config="custom_masks.jsonb_obj(3, 3, ARRAY[''text''])",
    )

    merge_commit_sha: orm.Mapped[github_types.SHAType | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 10 )",
    )

    maintainer_can_modify: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    merged: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    merged_by: orm.Mapped[github_types.GitHubAccount | None] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config="custom_masks.jsonb_obj(4, 4, ARRAY[''text''])",
    )
    rebaseable: orm.Mapped[bool | None] = orm.mapped_column(
        sqlalchemy.Boolean,
        nullable=True,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    mergeable: orm.Mapped[bool | None] = orm.mapped_column(
        sqlalchemy.Boolean,
        nullable=True,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    mergeable_state: orm.Mapped[str | None] = orm.mapped_column(
        sqlalchemy.Text,
        nullable=True,
        anonymizer_config="anon.lorem_ipsum( characters := 10 )",
    )
    changed_files: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    commits: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    head_commits: orm.Mapped[
        list[pr_commit_model.PullRequestCommit]
    ] = orm.relationship(lazy="raise_on_sql", back_populates="pull_request")

    @classmethod
    async def _update_commits(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull_request_id: int,
        pull_number: int,
        repo_owner: github_types.GitHubLogin,
        repo_name: github_types.GitHubRepositoryName,
    ) -> None:
        installation = await github.get_installation_from_login(repo_owner)
        auth = github.GitHubAppInstallationAuth(installation)
        client = github.AsyncGitHubInstallationClient(auth=auth)

        try:
            new_commits = [
                commit
                async for commit in typing.cast(
                    abc.AsyncIterable[github_types.GitHubBranchCommit],
                    client.items(
                        f"/repos/{repo_owner}/{repo_name}/pulls/{pull_number}/commits",
                        resource_name="commits",
                        page_limit=10,
                    ),
                )
            ]
        except http.HTTPNotFound as e:
            LOG.warning(
                "Skipping commit update for pull request %i because we can't query it's commits",
                pull_request_id,
                exc=str(e),
            )
            return

        for commit in new_commits:
            try:
                pr_commit_model.PullRequestCommit.type_adapter.validate_python(commit)
            except pydantic_core.ValidationError:
                LOG.warning(
                    "Skipping commit insert/update for pull_request %i because one of its commit can't be validated by pydantic",
                    pull_request_id,
                    commit=commit,
                )
                return

        new_commits_head_shas = [commit["sha"] for commit in new_commits]

        await session.execute(
            sqlalchemy.delete(pr_commit_model.PullRequestCommit).where(
                PullRequest.id == pr_commit_model.PullRequestCommit.pull_request_id,
                pr_commit_model.PullRequestCommit.sha.notin_(new_commits_head_shas),
            ),
        )

        for commit_index, commit in enumerate(new_commits):
            await pr_commit_model.PullRequestCommit.insert_or_update(
                session,
                commit,
                pull_request_id,
                commit_index,
            )

    @classmethod
    async def insert_or_update(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        data: github_types.GitHubPullRequest,
    ) -> None:
        fetch_new_commits = not (
            await session.scalar(
                sqlalchemy.select(cls).where(
                    cls.id == data["id"],
                    cls.head["sha"].astext == data["head"]["sha"],
                ),
            )
        )

        validated_data = cls.type_adapter.validate_python(data)
        # Copy the validated_data in a more generic dict in order to manipulate
        # more easily.
        data_for_obj: dict[str, typing.Any] = validated_data.copy()  # type: ignore[assignment]

        user = data_for_obj.pop("user")
        data_for_obj["user_id"] = user["id"]
        # Postgres Text fields do not allow nul bytes and if the body contains some, then
        # GitHub sends them raw in the body. So we need to remove them.
        if data_for_obj["body"] is not None:
            data_for_obj["body"] = data_for_obj["body"].replace("\x00", "")

        await gh_account_model.GitHubAccount.create_or_update(session, user)

        assignees = data_for_obj.pop("assignees")
        requested_reviewers = data_for_obj.pop("requested_reviewers")
        pull_obj = cls(**data_for_obj)
        await session.merge(pull_obj)

        for assignee in assignees:
            await gh_account_model.GitHubAccount.create_or_update(session, assignee)
            await PullRequestAssigneesGitHubAccountAssociationTable.insert(
                session,
                validated_data["id"],
                assignee["id"],
            )

        assignees_ids = [assignee["id"] for assignee in assignees]
        # Delete github account assignees not in the list anymore
        await session.execute(
            sqlalchemy.delete(PullRequestAssigneesGitHubAccountAssociationTable).where(
                PullRequestAssigneesGitHubAccountAssociationTable.pull_request_id
                == data_for_obj["id"],
                PullRequestAssigneesGitHubAccountAssociationTable.github_account_id.notin_(
                    assignees_ids,
                ),
            ),
        )

        for requested_reviewer in requested_reviewers:
            await gh_account_model.GitHubAccount.create_or_update(
                session,
                requested_reviewer,
            )
            await PullRequestRequestedReviewersGitHubAccountAssociationTable.insert(
                session,
                validated_data["id"],
                requested_reviewer["id"],
            )

        requested_reviewers_ids = [
            requested_reviewer["id"] for requested_reviewer in requested_reviewers
        ]
        # Delete github account requested reviewers not in the list anymore
        await session.execute(
            sqlalchemy.delete(
                PullRequestRequestedReviewersGitHubAccountAssociationTable,
            ).where(
                PullRequestRequestedReviewersGitHubAccountAssociationTable.pull_request_id
                == data_for_obj["id"],
                PullRequestRequestedReviewersGitHubAccountAssociationTable.github_account_id.notin_(
                    requested_reviewers_ids,
                ),
            ),
        )

        if fetch_new_commits:
            await cls._update_commits(
                session,
                pull_obj.id,
                pull_obj.number,
                pull_obj.base["repo"]["owner"]["login"],
                pull_obj.base["repo"]["name"],
            )

    def as_github_dict(self) -> github_types.GitHubPullRequest:
        return typing.cast(github_types.GitHubPullRequest, super().as_github_dict())


class PullRequestAssigneesGitHubAccountAssociationTable(models.Base):
    __tablename__ = "at_pull_request_assignees_github_account"

    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"),
        primary_key=True,
        anonymizer_config=None,
    )
    github_account_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        primary_key=True,
        anonymizer_config=None,
    )

    pull_request: orm.Mapped[PullRequest] = orm.relationship()
    github_account: orm.Mapped[gh_account_model.GitHubAccount] = orm.relationship()

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull_request_id: int,
        github_account_id: int,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(
                pull_request_id=pull_request_id,
                github_account_id=github_account_id,
            )
            .on_conflict_do_nothing(
                index_elements=["pull_request_id", "github_account_id"],
            )
        )
        await session.execute(sql)


class PullRequestRequestedReviewersGitHubAccountAssociationTable(models.Base):
    __tablename__ = "at_pull_request_requested_reviewers_github_account"

    pull_request_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("pull_request.id"),
        primary_key=True,
        anonymizer_config=None,
    )
    github_account_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.ForeignKey("github_account.id"),
        primary_key=True,
        anonymizer_config=None,
    )

    pull_request: orm.Mapped[PullRequest] = orm.relationship()
    github_account: orm.Mapped[gh_account_model.GitHubAccount] = orm.relationship()

    @classmethod
    async def insert(
        cls,
        session: sqlalchemy.ext.asyncio.AsyncSession,
        pull_request_id: int,
        github_account_id: int,
    ) -> None:
        sql = (
            postgresql.insert(cls)
            .values(
                pull_request_id=pull_request_id,
                github_account_id=github_account_id,
            )
            .on_conflict_do_nothing(
                index_elements=["pull_request_id", "github_account_id"],
            )
        )
        await session.execute(sql)


# This table is kept just in case we still need the data from it for the moment
class PullRequestForCiEventProcessing(models.Base):
    __tablename__ = "old_pull_request_for_ci_event_processing"

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=False,
        anonymizer_config=None,
    )
    number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        anonymizer_config="anon.random_int_between(1,100000)",
    )
    title: orm.Mapped[str] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( words := 5 )",
    )
    state: orm.Mapped[github_types.GitHubPullRequestState] = orm.mapped_column(
        sqlalchemy.Text,
        anonymizer_config="anon.lorem_ipsum( characters := 7 )",
    )
