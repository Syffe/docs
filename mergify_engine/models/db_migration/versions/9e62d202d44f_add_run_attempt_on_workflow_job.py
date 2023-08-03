"""Add run_attempt on workflow_job

Revision ID: 9e62d202d44f
Revises: cbe997ab7ada
Create Date: 2023-08-01 12:07:22.591313

"""
import alembic
import sqlalchemy


revision = "9e62d202d44f"
down_revision = "cbe997ab7ada"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "run_attempt",
            sqlalchemy.BigInteger(),
            nullable=True,
            anonymizer_config=None,
        ),
    )

    alembic.op.execute(
        """
        WITH compute_run_attempt as
            (
                SELECT
                    id,
                    ROW_NUMBER() OVER(PARTITION BY workflow_run_id, name ORDER BY started_at) run_attempt
                FROM
                    gha_workflow_job
            )
        UPDATE
            gha_workflow_job
        SET
            run_attempt=compute_run_attempt.run_attempt
        FROM
            compute_run_attempt
        WHERE
            compute_run_attempt.id=gha_workflow_job.id
        """
    )

    alembic.op.alter_column("gha_workflow_job", "run_attempt", nullable=False)


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
