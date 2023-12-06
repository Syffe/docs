"""Add ci issue GPT

Revision ID: da41815ebbbe
Revises: 65e800f05671
Create Date: 2023-11-30 14:26:38.334411

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "da41815ebbbe"
down_revision = "65e800f05671"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_table(
        "ci_issue_gpt",
        sqlalchemy.Column(
            "id",
            sqlalchemy.Integer(),
            autoincrement=True,
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.Column(
            "short_id_suffix",
            sqlalchemy.Integer(),
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.Column(
            "name",
            sqlalchemy.String(),
            nullable=True,
            anonymizer_config=None,
        ),
        sqlalchemy.Column(
            "repository_id",
            sqlalchemy.BigInteger(),
            nullable=False,
            anonymizer_config=None,
        ),
        sqlalchemy.Column(
            "status",
            postgresql.ENUM(
                "UNRESOLVED",
                "RESOLVED",
                name="ciissuestatus",
                create_type=False,
            ),
            server_default="UNRESOLVED",
            nullable=False,
            anonymizer_config="anon.random_in_enum(status)",
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["repository_id"],
            ["github_repository.id"],
            name=alembic.op.f("ci_issue_gpt_repository_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint("id", name=alembic.op.f("ci_issue_gpt_pkey")),
        sqlalchemy.UniqueConstraint(
            "repository_id",
            "short_id_suffix",
            name="ci_issue_gpt_repository_id_short_id_suffix_key",
        ),
    )
    alembic.op.add_column(
        "gha_workflow_job_log_metadata",
        sqlalchemy.Column(
            "ci_issue_id",
            sqlalchemy.Integer(),
            nullable=True,
            anonymizer_config=None,
        ),
    )
    alembic.op.create_foreign_key(
        alembic.op.f("gha_workflow_job_log_metadata_ci_issue_id_fkey"),
        "gha_workflow_job_log_metadata",
        "ci_issue_gpt",
        ["ci_issue_id"],
        ["id"],
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
