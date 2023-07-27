"""add_wj_job_embedding_distance

Revision ID: 7539200eded8
Revises: cac437b249cb
Create Date: 2023-07-26 16:50:16.837735

"""
import alembic
import sqlalchemy


revision = "7539200eded8"
down_revision = "cac437b249cb"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_table(
        "gha_workflow_job_log_neighbours",
        sqlalchemy.Column(
            "job_id", sqlalchemy.BigInteger(), nullable=False, anonymizer_config=None
        ),
        sqlalchemy.Column(
            "neighbour_job_id",
            sqlalchemy.BigInteger(),
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.Column(
            "cosine_similarity",
            sqlalchemy.Float(),
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["job_id"],
            ["gha_workflow_job.id"],
            name=alembic.op.f("gha_workflow_job_log_neighbours_job_id_fkey"),
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["neighbour_job_id"],
            ["gha_workflow_job.id"],
            name=alembic.op.f("gha_workflow_job_log_neighbours_neighbour_job_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "job_id",
            "neighbour_job_id",
            name=alembic.op.f("gha_workflow_job_log_neighbours_pkey"),
        ),
    )
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "neighbours_computed_at",
            sqlalchemy.DateTime(),
            nullable=True,
            anonymizer_config=None,
        ),
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
