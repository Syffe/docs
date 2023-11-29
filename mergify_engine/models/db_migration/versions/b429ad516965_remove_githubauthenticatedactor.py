"""Remove GithubAuthenticatedActor

Revision ID: b429ad516965
Revises: 12b69c37eddf
Create Date: 2023-11-28 16:32:31.297497

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "b429ad516965"
down_revision = "12b69c37eddf"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.alter_column(
        "event_queue_freeze_create",
        "created_by",
        existing_type=postgresql.JSONB(astext_type=sqlalchemy.Text()),  # type: ignore [no-untyped-call]
        nullable=False,
    )
    alembic.op.drop_constraint(
        "event_queue_freeze_create_created_by_id_fkey",
        "event_queue_freeze_create",
        type_="foreignkey",
    )
    alembic.op.drop_column("event_queue_freeze_create", "created_by_id")

    alembic.op.alter_column(
        "event_queue_freeze_delete",
        "deleted_by",
        existing_type=postgresql.JSONB(astext_type=sqlalchemy.Text()),  # type: ignore [no-untyped-call]
        nullable=False,
    )
    alembic.op.drop_constraint(
        "event_queue_freeze_delete_deleted_by_id_fkey",
        "event_queue_freeze_delete",
        type_="foreignkey",
    )
    alembic.op.drop_column("event_queue_freeze_delete", "deleted_by_id")

    alembic.op.alter_column(
        "event_queue_freeze_update",
        "updated_by",
        existing_type=postgresql.JSONB(astext_type=sqlalchemy.Text()),  # type: ignore [no-untyped-call]
        nullable=False,
    )
    alembic.op.drop_constraint(
        "event_queue_freeze_update_updated_by_id_fkey",
        "event_queue_freeze_update",
        type_="foreignkey",
    )
    alembic.op.drop_column("event_queue_freeze_update", "updated_by_id")

    alembic.op.alter_column(
        "event_queue_pause_create",
        "created_by",
        existing_type=postgresql.JSONB(astext_type=sqlalchemy.Text()),  # type: ignore [no-untyped-call]
        nullable=False,
    )
    alembic.op.drop_constraint(
        "event_queue_pause_create_created_by_id_fkey",
        "event_queue_pause_create",
        type_="foreignkey",
    )
    alembic.op.drop_column("event_queue_pause_create", "created_by_id")

    alembic.op.alter_column(
        "event_queue_pause_delete",
        "deleted_by",
        existing_type=postgresql.JSONB(astext_type=sqlalchemy.Text()),  # type: ignore [no-untyped-call]
        nullable=False,
    )
    alembic.op.drop_constraint(
        "event_queue_pause_delete_deleted_by_id_fkey",
        "event_queue_pause_delete",
        type_="foreignkey",
    )
    alembic.op.drop_column("event_queue_pause_delete", "deleted_by_id")

    alembic.op.alter_column(
        "event_queue_pause_update",
        "updated_by",
        existing_type=postgresql.JSONB(astext_type=sqlalchemy.Text()),  # type: ignore [no-untyped-call]
        nullable=False,
    )
    alembic.op.drop_constraint(
        "event_queue_pause_update_updated_by_id_fkey",
        "event_queue_pause_update",
        type_="foreignkey",
    )
    alembic.op.drop_column("event_queue_pause_update", "updated_by_id")

    alembic.op.drop_table("github_authenticated_actor")
    alembic.op.execute(sqlalchemy.text("drop type githubauthenticatedactortype"))


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
