"""Event actor as JSON

Revision ID: e680f42377e6
Revises: 1a31a8da24c5
Create Date: 2023-11-28 10:06:46.354782

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "e680f42377e6"
down_revision = "1a31a8da24c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "event_queue_freeze_create",
        sqlalchemy.Column(
            "created_by",
            postgresql.JSONB(),  # type: ignore [no-untyped-call]
            nullable=True,
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_freeze_create e "
            "SET created_by = TO_JSONB(a) "
            "FROM github_authenticated_actor a "
            "WHERE e.created_by_id = a.id",
        ),
    )

    alembic.op.add_column(
        "event_queue_freeze_update",
        sqlalchemy.Column(
            "updated_by",
            postgresql.JSONB(),  # type: ignore [no-untyped-call]
            nullable=True,
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_freeze_update e "
            "SET updated_by = TO_JSONB(a) "
            "FROM github_authenticated_actor a "
            "WHERE e.updated_by_id = a.id",
        ),
    )

    alembic.op.add_column(
        "event_queue_freeze_delete",
        sqlalchemy.Column(
            "deleted_by",
            postgresql.JSONB(),  # type: ignore [no-untyped-call]
            nullable=True,
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_freeze_delete e "
            "SET deleted_by = TO_JSONB(a) "
            "FROM github_authenticated_actor a "
            "WHERE e.deleted_by_id = a.id",
        ),
    )

    alembic.op.add_column(
        "event_queue_pause_create",
        sqlalchemy.Column(
            "created_by",
            postgresql.JSONB(),  # type: ignore [no-untyped-call]
            nullable=True,
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_pause_create e "
            "SET created_by = TO_JSONB(a) "
            "FROM github_authenticated_actor a "
            "WHERE e.created_by_id = a.id",
        ),
    )

    alembic.op.add_column(
        "event_queue_pause_update",
        sqlalchemy.Column(
            "updated_by",
            postgresql.JSONB(),  # type: ignore [no-untyped-call]
            nullable=True,
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_pause_update e "
            "SET updated_by = TO_JSONB(a) "
            "FROM github_authenticated_actor a "
            "WHERE e.updated_by_id = a.id",
        ),
    )

    alembic.op.add_column(
        "event_queue_pause_delete",
        sqlalchemy.Column(
            "deleted_by",
            postgresql.JSONB(),  # type: ignore [no-untyped-call]
            nullable=True,
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_pause_delete e "
            "SET deleted_by = TO_JSONB(a) "
            "FROM github_authenticated_actor a "
            "WHERE e.deleted_by_id = a.id",
        ),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
