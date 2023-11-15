"""Add model CiIssue

Revision ID: 49425c551193
Revises: a72a0edc7d19
Create Date: 2023-10-23 12:13:50.934296

"""
import alembic
import sqlalchemy


revision = "49425c551193"
down_revision = "a72a0edc7d19"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_table(
        "ci_issue",
        sqlalchemy.Column(
            "id",
            sqlalchemy.Integer(),
            autoincrement=True,
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.Column("short_id_suffix", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.Column("name", sqlalchemy.String(), nullable=True),
        sqlalchemy.Column(
            "repository_id",
            sqlalchemy.BigInteger(),
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["repository_id"],
            ["github_repository.id"],
            name=alembic.op.f("ci_issue_repository_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint("id", name=alembic.op.f("ci_issue_pkey")),
        sqlalchemy.UniqueConstraint(
            "repository_id",
            "short_id_suffix",
            name=alembic.op.f("ci_issue_repository_id_short_id_suffix_key"),
        ),
    )
    alembic.op.create_table(
        "ci_issue_counter",
        sqlalchemy.Column(
            "repository_id",
            sqlalchemy.BigInteger(),
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.Column("current_value", sqlalchemy.Integer(), nullable=False),
        sqlalchemy.ForeignKeyConstraint(
            ["repository_id"],
            ["github_repository.id"],
            name=alembic.op.f("ci_issue_counter_repository_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "repository_id",
            name=alembic.op.f("ci_issue_counter_pkey"),
        ),
    )
    alembic.op.add_column(
        "gha_workflow_job",
        sqlalchemy.Column(
            "ci_issue_id",
            sqlalchemy.Integer(),
            nullable=True,
            anonymizer_config=None,
        ),
    )
    alembic.op.create_foreign_key(
        alembic.op.f("gha_workflow_job_ci_issue_id_fkey"),
        "gha_workflow_job",
        "ci_issue",
        ["ci_issue_id"],
        ["id"],
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
