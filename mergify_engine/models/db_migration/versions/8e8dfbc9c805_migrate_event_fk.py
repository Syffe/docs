"""Migrate existing events checks SpecChecks FK

Revision ID: 8e8dfbc9c805
Revises: 57a7534b63e9
Create Date: 2023-11-20 15:14:45.599908

"""
import asyncio
import os

import alembic.operations
import sqlalchemy
import sqlalchemy.ext.asyncio


revision = "8e8dfbc9c805"
down_revision = "57a7534b63e9"
branch_labels = None
depends_on = None

BATCH_UPDATE_SIZE = 100


# NOTE(leo): we do not use SQLAlchemy models from code but rather minimal
# SQLAlchemy models defined locally because migrations depending on the
# source code could break after some changes


event_check_start = sqlalchemy.sql.table(
    "event_action_queue_checks_start",
    sqlalchemy.sql.column("id", sqlalchemy.Integer),
    sqlalchemy.sql.column("speculative_check_pull_request_id", sqlalchemy.Integer),
)

event_check_end = sqlalchemy.sql.table(
    "event_action_queue_checks_end",
    sqlalchemy.sql.column("id", sqlalchemy.Integer),
    sqlalchemy.sql.column("speculative_check_pull_request_id", sqlalchemy.Integer),
)

spec_check_pull_request = sqlalchemy.sql.table(
    "speculative_check_pull_request",
    sqlalchemy.sql.column("id", sqlalchemy.Integer),
    sqlalchemy.sql.column(
        "event_id",
        sqlalchemy.Integer,
    ),
)


async def migrate_data(
    conn: sqlalchemy.ext.asyncio.AsyncConnection | sqlalchemy.ext.asyncio.AsyncSession,
    force: bool = False,
) -> None:
    if not force and os.getenv("MERGIFYENGINE_SAAS_MODE"):
        print(
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

    for checks_evt in (
        event_check_start,
        event_check_end,
    ):
        spec_check_pull_request_alias = spec_check_pull_request.alias()

        subquery = (
            sqlalchemy.select(spec_check_pull_request_alias.c.id)
            .where(spec_check_pull_request_alias.c.event_id.is_(None))
            .limit(BATCH_UPDATE_SIZE)
        )

        update_stmt = (
            sqlalchemy.update(spec_check_pull_request)
            .where(
                spec_check_pull_request.c.id.in_(subquery),
                spec_check_pull_request.c.id
                == checks_evt.c.speculative_check_pull_request_id,
            )
            .values(event_id=checks_evt.c.id)
            .returning(spec_check_pull_request.c.id)
        )

        returned = BATCH_UPDATE_SIZE
        while returned == BATCH_UPDATE_SIZE:
            results = await session.execute(update_stmt)
            returned = results.rowcount  # type: ignore[attr-defined]


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
        await migrate_data(session, force=True)


if __name__ == "__main__":
    print("Data migration started...")
    asyncio.run(manual_run())
    print("Migration done")
