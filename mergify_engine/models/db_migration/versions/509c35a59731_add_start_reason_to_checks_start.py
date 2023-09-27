"""add-start-reason-to-checks-start

Revision ID: 509c35a59731
Revises: cdde18b1072d
Create Date: 2023-09-15 09:52:09.670729

"""
import alembic
import sqlalchemy


revision = "509c35a59731"
down_revision = "cdde18b1072d"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "event_action_queue_checks_start",
        sqlalchemy.Column(
            "start_reason",
            sqlalchemy.Text(),
            nullable=False,
            anonymizer_config="anon.lorem_ipsum( words := 7)",
        ),
    )
    alembic.op.execute(
        "UPDATE event_action_queue_checks_start SET start_reason = '' WHERE start_reason IS NULL"
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
