"""add head SHA on workflow_job

Revision ID: 2df335ca04f5
Revises: e4894ddcace0
Create Date: 2023-10-06 14:02:09.464860

"""
import alembic
import sqlalchemy


revision = "2df335ca04f5"
down_revision = "e4894ddcace0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column("head_sha", sqlalchemy.String(), anonymizer_config=None),
    )

    # NOTE(Kontrolix): Set head_sha to '' for old data to be able to set the
    # not null constraint, it's not a problem since it was not use before.
    alembic.op.execute(
        "UPDATE gha_workflow_job SET head_sha = '' WHERE head_sha IS NULL"
    )

    alembic.op.alter_column("gha_workflow_job", "head_sha", nullable=False)


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
