"""update base_repository_id on pull_request

Revision ID: c3263ca8c1c0
Revises: e03fc10bcaf2
Create Date: 2023-12-06 11:20:04.176211

"""
from __future__ import annotations

import asyncio
import enum

import alembic
import alembic.operations
import sqlalchemy
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio

from mergify_engine import settings


revision = "c3263ca8c1c0"
down_revision = "56e12b9a1fb5"
branch_labels = None
depends_on = None

BATCH_SIZE = 100

pull_request = sqlalchemy.sql.table(
    "pull_request",
    sqlalchemy.sql.column("id", sqlalchemy.Integer),
    sqlalchemy.sql.column("base_repository_id", sqlalchemy.Integer),
    sqlalchemy.sql.column("base", postgresql.JSONB),
)


class GitHubAccountType(enum.StrEnum):
    USER = "User"
    ORGANIZATION = "Organization"
    BOT = "Bot"


github_account = sqlalchemy.sql.table(
    "github_account",
    sqlalchemy.sql.column("id", sqlalchemy.Integer),
    sqlalchemy.sql.column("login", sqlalchemy.Text),
    sqlalchemy.sql.column("type", sqlalchemy.Enum(GitHubAccountType)),
    sqlalchemy.sql.column("avatar_url", sqlalchemy.Text),
)

github_repository = sqlalchemy.sql.table(
    "github_repository",
    sqlalchemy.sql.column("id", sqlalchemy.BigInteger),
    sqlalchemy.sql.column("owner_id", sqlalchemy.Integer),
    sqlalchemy.sql.column("name", sqlalchemy.Text),
    sqlalchemy.sql.column("private", sqlalchemy.Boolean),
    sqlalchemy.sql.column("default_branch", sqlalchemy.Text),
    sqlalchemy.sql.column("archived", sqlalchemy.Boolean),
)


async def migrate_data(
    conn: sqlalchemy.ext.asyncio.AsyncConnection | sqlalchemy.ext.asyncio.AsyncSession,
    manual: bool = False,
) -> None:
    if not manual and settings.SAAS_MODE:
        print(  # noqa: T201
            "This must be a manual data migration for SaaS. "
            "Please run it directly on server with cmd `mergify-database-update`.",
        )
        # NOTE(leo): `alembic_version` will be incremented as if the migration had been
        # performed
        return

    if isinstance(conn, sqlalchemy.ext.asyncio.AsyncConnection):
        session = sqlalchemy.ext.asyncio.AsyncSession(bind=conn)
    else:
        session = conn

    batch_select = (
        sqlalchemy.select(pull_request.c.id)
        .where(pull_request.c.base_repository_id.is_(None))
        .limit(BATCH_SIZE)
    )

    pulls_to_update = (await session.execute(batch_select)).scalars().all()

    # Some alias to try to make sqlachemy statement more readable
    cast = sqlalchemy.func.cast
    pr_repo = pull_request.c.base["repo"]
    pr_owner = pr_repo["owner"]

    total = 0
    while pulls_to_update:
        # Insert missing account
        select_missing_account = (
            sqlalchemy.select(
                cast(pr_owner["id"], sqlalchemy.Integer).label("id"),
                pr_owner.op("->>")("login").label("login"),
                cast(
                    sqlalchemy.func.upper(pr_owner.op("->>")("type")),
                    sqlalchemy.Enum(GitHubAccountType),
                ).label("type"),
                pr_owner.op("->>")("avatar_url").label("avatar_url"),
            )
            .distinct()
            .select_from(
                pull_request.outerjoin(
                    github_repository,
                    github_repository.c.id == cast(pr_repo["id"], sqlalchemy.Integer),
                ).outerjoin(
                    github_account,
                    github_account.c.id == cast(pr_owner["id"], sqlalchemy.Integer),
                ),
            )
            .where(
                github_repository.c.id.is_(None),
                github_account.c.id.is_(None),
                pull_request.c.id.in_(pulls_to_update),
            )
        )
        await session.execute(
            sqlalchemy.insert(github_account).from_select(
                ["id", "login", "type", "avatar_url"],
                select_missing_account,
            ),
        )

        # Insert missing repo
        select_missing_repo = (
            sqlalchemy.select(
                cast(pr_repo["id"], sqlalchemy.Integer).label("id"),
                pr_repo.op("->>")("name").label("name"),
                cast(pr_owner["id"], sqlalchemy.Integer).label("owner_id"),
                cast(pr_repo["private"], sqlalchemy.Boolean).label("private"),
                pr_repo.op("->>")("default_branch").label(
                    "default_branch",
                ),
                cast(pr_repo["archived"], sqlalchemy.Boolean).label("archived"),
            )
            .distinct()
            .select_from(
                pull_request.outerjoin(
                    github_repository,
                    github_repository.c.id == cast(pr_repo["id"], sqlalchemy.Integer),
                ),
            )
            .where(
                github_repository.c.id.is_(None),
                pull_request.c.id.in_(pulls_to_update),
            )
        )
        await session.execute(
            sqlalchemy.insert(github_repository).from_select(
                ["id", "name", "owner_id", "private", "default_branch", "archived"],
                select_missing_repo,
            ),
        )

        # Update pull request base_repository_id
        await session.execute(
            sqlalchemy.update(pull_request)
            .values(base_repository_id=cast(pr_repo["id"], sqlalchemy.Integer))
            .where(pull_request.c.id.in_(pulls_to_update)),
        )

        if manual:
            await session.commit()

        total += len(pulls_to_update)
        pulls_to_update = (await session.execute(batch_select)).scalars().all()

    print(f"total rows updated: {total}")  # noqa: T201


def upgrade() -> None:
    abstract_operations = alembic.operations.AbstractOperations(
        alembic.op.get_context(),
    )
    abstract_operations.run_async(migrate_data)


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass


async def manual_run() -> None:
    from mergify_engine import database

    database.init_sqlalchemy("migration_script")
    async with database.create_session() as session:
        await migrate_data(session, manual=True)

    # test the expected result
    async with database.create_session() as session:
        count = await session.execute(
            sqlalchemy.select(sqlalchemy.func.count()).where(
                pull_request.c.base_repository_id.is_(None),
            ),
        )

    if count.scalar() != 0:
        raise RuntimeError("Failed to migrate all data")


if __name__ == "__main__":
    print("* Data migration started...")  # noqa: T201
    asyncio.run(manual_run())
    print("* Migration successful")  # noqa: T201
