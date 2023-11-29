"""Fix event JSON metadata type property case

Revision ID: 12b69c37eddf
Revises: e680f42377e6
Create Date: 2023-11-29 11:20:44.840892

"""

import alembic
import sqlalchemy


revision = "12b69c37eddf"
down_revision = "e680f42377e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_freeze_create e "
            "SET created_by = JSON_BUILD_OBJECT('id', a.id, 'type', LOWER(a.type::TEXT), 'name', a.name) "
            "FROM github_authenticated_actor a "
            "WHERE e.created_by_id = a.id",
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_freeze_update e "
            "SET updated_by = JSON_BUILD_OBJECT('id', a.id, 'type', LOWER(a.type::TEXT), 'name', a.name) "
            "FROM github_authenticated_actor a "
            "WHERE e.updated_by_id = a.id",
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_freeze_delete e "
            "SET deleted_by = JSON_BUILD_OBJECT('id', a.id, 'type', LOWER(a.type::TEXT), 'name', a.name) "
            "FROM github_authenticated_actor a "
            "WHERE e.deleted_by_id = a.id",
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_pause_create e "
            "SET created_by = JSON_BUILD_OBJECT('id', a.id, 'type', LOWER(a.type::TEXT), 'name', a.name) "
            "FROM github_authenticated_actor a "
            "WHERE e.created_by_id = a.id",
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_pause_update e "
            "SET updated_by = JSON_BUILD_OBJECT('id', a.id, 'type', LOWER(a.type::TEXT), 'name', a.name) "
            "FROM github_authenticated_actor a "
            "WHERE e.updated_by_id = a.id",
        ),
    )
    alembic.op.execute(
        sqlalchemy.text(
            "UPDATE event_queue_pause_delete e "
            "SET deleted_by = JSON_BUILD_OBJECT('id', a.id, 'type', LOWER(a.type::TEXT), 'name', a.name) "
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
