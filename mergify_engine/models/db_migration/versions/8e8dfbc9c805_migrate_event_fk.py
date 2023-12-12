# ruff: noqa: T201
"""Migrate existing events checks SpecChecks FK

Revision ID: 8e8dfbc9c805
Revises: 57a7534b63e9
Create Date: 2023-11-20 15:14:45.599908

"""
import asyncio

import alembic.operations
import sqlalchemy
from sqlalchemy import func
import sqlalchemy.ext.asyncio

from mergify_engine import settings


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
    manual: bool = False,
) -> None:
    if not manual and settings.SAAS_MODE:
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

    total = 0
    for checks_evt in (
        event_check_start,
        event_check_end,
    ):
        # subquery to get the batches of objects that need to be updated
        subquery = (
            sqlalchemy.select(spec_check_pull_request.c.id)
            .join(
                checks_evt,
                spec_check_pull_request.c.id
                == checks_evt.c.speculative_check_pull_request_id,
            )
            .where(spec_check_pull_request.c.event_id.is_(None))
            .limit(BATCH_UPDATE_SIZE)
        )
        to_update_spec_checks_ids = (await session.execute(subquery)).scalars().all()

        while len(to_update_spec_checks_ids) != 0:
            # update the rows by batches
            update_stmt = (
                sqlalchemy.update(spec_check_pull_request)
                .where(
                    spec_check_pull_request.c.id.in_(to_update_spec_checks_ids),
                    spec_check_pull_request.c.id
                    == checks_evt.c.speculative_check_pull_request_id,
                )
                .values(event_id=checks_evt.c.id)
            )

            await session.execute(update_stmt)

            if manual:
                await session.commit()

            total += len(to_update_spec_checks_ids)
            to_update_spec_checks_ids = (
                (await session.execute(subquery)).scalars().all()
            )

    print(f"total rows updated: {total}")


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
            sqlalchemy.select(func.count())
            .select_from(spec_check_pull_request)
            .where(spec_check_pull_request.c.event_id.is_(None)),
        )
    if count.scalar() != 0:
        raise RuntimeError("Failed to migrate all data")


if __name__ == "__main__":
    print("* Data migration started...")
    asyncio.run(manual_run())
    print("* Migration successful")
