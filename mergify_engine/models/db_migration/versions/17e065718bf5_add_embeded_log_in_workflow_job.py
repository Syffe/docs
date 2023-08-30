"""Add embeded_log in workflow_job

Revision ID: 17e065718bf5
Revises: 8091f931f1c1
Create Date: 2023-08-25 11:14:15.716462

"""
import alembic
import sqlalchemy


revision = "17e065718bf5"
down_revision = "8091f931f1c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "embedded_log",
            sqlalchemy.String(),
            nullable=True,
            anonymizer_config="anon.lorem_ipsum( words := 20 )",
        ),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
