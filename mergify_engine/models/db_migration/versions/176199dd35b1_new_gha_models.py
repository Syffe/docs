"""New GHA models

Revision ID: 176199dd35b1
Revises: a875052e527c
Create Date: 2023-06-21 08:36:12.979855

"""
import alembic
import sqlalchemy
from sqlalchemy.dialects import postgresql


revision = "176199dd35b1"
down_revision = "a875052e527c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.create_table(
        "gha_workflow_job",
        sqlalchemy.Column(
            "id",
            sqlalchemy.BigInteger(),
            autoincrement=False,
            nullable=False,
        ),
        sqlalchemy.Column("workflow_run_id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column("name", sqlalchemy.String(), nullable=False),
        sqlalchemy.Column(
            "started_at",
            sqlalchemy.DateTime(timezone=True),
            nullable=False,
        ),
        sqlalchemy.Column(
            "completed_at",
            sqlalchemy.DateTime(timezone=True),
            nullable=False,
        ),
        sqlalchemy.Column(
            "conclusion",
            postgresql.ENUM(
                "SUCCESS",
                "FAILURE",
                "SKIPPED",
                "CANCELLED",
                name="jobrunconclusion",
                create_type=False,
            ),
            nullable=False,
        ),
        sqlalchemy.Column(
            "labels",
            sqlalchemy.ARRAY(sqlalchemy.TEXT, dimensions=1),
            nullable=False,
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "id",
            name=alembic.op.f("gha_workflow_job_pkey"),
        ),
    )
    alembic.op.create_table(
        "gha_workflow_run",
        sqlalchemy.Column(
            "id",
            sqlalchemy.BigInteger(),
            autoincrement=False,
            nullable=False,
        ),
        sqlalchemy.Column("workflow_id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column("owner_id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column("repository", sqlalchemy.Text(), nullable=False),
        sqlalchemy.Column(
            "event",
            postgresql.ENUM(
                "PULL_REQUEST",
                "PULL_REQUEST_TARGET",
                "PUSH",
                "SCHEDULE",
                name="jobruntriggerevent",
                create_type=False,
            ),
            nullable=False,
        ),
        sqlalchemy.Column(
            "triggering_actor_id",
            sqlalchemy.BigInteger(),
            nullable=False,
        ),
        sqlalchemy.Column("run_attempt", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.ForeignKeyConstraint(
            ["owner_id"],
            ["github_account.id"],
            name=alembic.op.f("gha_workflow_run_owner_id_fkey"),
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["triggering_actor_id"],
            ["github_account.id"],
            name=alembic.op.f("gha_workflow_run_triggering_actor_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "id",
            name=alembic.op.f("gha_workflow_run_pkey"),
        ),
    )
    alembic.op.create_index(
        "gha_job_run_owner_id_repository_idx",
        "gha_workflow_run",
        ["owner_id", "repository"],
        unique=False,
    )
    alembic.op.create_table(
        "jt_gha_workflow_run_pull_request",
        sqlalchemy.Column("pull_request_id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.Column("workflow_run_id", sqlalchemy.BigInteger(), nullable=False),
        sqlalchemy.ForeignKeyConstraint(
            ["pull_request_id"],
            ["pull_request.id"],
            name=alembic.op.f("jt_gha_workflow_run_pull_request_pull_request_id_fkey"),
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["workflow_run_id"],
            ["gha_workflow_run.id"],
            name=alembic.op.f("jt_gha_workflow_run_pull_request_workflow_run_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "pull_request_id",
            "workflow_run_id",
            name=alembic.op.f("jt_gha_workflow_run_pull_request_pkey"),
        ),
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    pass
