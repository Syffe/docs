"""Spec check add fk constraint

Revision ID: 1a31a8da24c5
Revises: 1d40ddeacc3b
Create Date: 2023-11-21 14:48:44.828049

"""
import alembic
import sqlalchemy


revision = "1a31a8da24c5"
down_revision = "1d40ddeacc3b"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop old constraints
    alembic.op.drop_constraint(
        "event_action_queue_checks_end_speculative_check_pull_re_bcae",
        "event_action_queue_checks_end",
        type_="foreignkey",
    )
    alembic.op.drop_constraint(
        "event_action_queue_checks_start_speculative_check_pull__7f8e",
        "event_action_queue_checks_start",
        type_="foreignkey",
    )

    # Create new constraint on prexisting, set non nullable
    alembic.op.alter_column(
        "speculative_check_pull_request",
        "event_id",
        existing_type=sqlalchemy.INTEGER(),
        nullable=False,
    )
    alembic.op.create_foreign_key(
        alembic.op.f("speculative_check_pull_request_event_id_fkey"),
        "speculative_check_pull_request",
        "event",
        ["event_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # Drop old FK constraints
    alembic.op.drop_column(
        "event_action_queue_checks_end",
        "speculative_check_pull_request_id",
    )
    alembic.op.drop_column(
        "event_action_queue_checks_start",
        "speculative_check_pull_request_id",
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
