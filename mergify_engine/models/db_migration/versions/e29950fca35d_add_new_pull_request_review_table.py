"""add new pull_request_review table

Revision ID: e29950fca35d
Revises: a5ad9285b4bd
Create Date: 2024-01-10 17:35:17.035895

"""
import alembic
import sqlalchemy


revision = "e29950fca35d"
down_revision = "a5ad9285b4bd"
branch_labels = None
depends_on = None


def upgrade() -> None:
    alembic.op.create_table(
        "pull_request_review",
        sqlalchemy.Column(
            "id",
            sqlalchemy.BigInteger(),
            autoincrement=False,
            nullable=False,
            anonymizer_config="anon.random_int_between(1, 1000000)",
        ),
        sqlalchemy.Column(
            "user_id",
            sqlalchemy.BigInteger(),
            nullable=True,
            anonymizer_config="anon.random_int_between(1,100000)",
        ),
        sqlalchemy.Column(
            "body",
            sqlalchemy.Text(),
            nullable=True,
            anonymizer_config="anon.lorem_ipsum( words := 30 )",
        ),
        sqlalchemy.Column(
            "state",
            sqlalchemy.Text(),
            nullable=False,
            anonymizer_config="anon.lorem_ipsum( characters := 7 )",
        ),
        sqlalchemy.Column(
            "author_association",
            sqlalchemy.Text(),
            nullable=False,
            anonymizer_config="anon.lorem_ipsum( characters := 7 )",
        ),
        sqlalchemy.Column(
            "submitted_at",
            sqlalchemy.DateTime(timezone=True),
            nullable=True,
            anonymizer_config="anon.dnoise(submitted_at, ''1 hour'')",
        ),
        sqlalchemy.Column(
            "commit_id",
            sqlalchemy.Text(),
            nullable=True,
            anonymizer_config="anon.lorem_ipsum( characters := 7 )",
        ),
        sqlalchemy.Column(
            "pull_request_id",
            sqlalchemy.BigInteger(),
            nullable=False,
            anonymizer_config="anon.random_int_between(1,100000)",
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["pull_request_id"],
            ["pull_request.id"],
            name=alembic.op.f("pull_request_review_pull_request_id_fkey"),
        ),
        sqlalchemy.ForeignKeyConstraint(
            ["user_id"],
            ["github_account.id"],
            name=alembic.op.f("pull_request_review_user_id_fkey"),
        ),
        sqlalchemy.PrimaryKeyConstraint(
            "id",
            name=alembic.op.f("pull_request_review_pkey"),
        ),
    )
    alembic.op.create_index(
        alembic.op.f("pull_request_review_pull_request_id_idx"),
        "pull_request_review",
        ["pull_request_id"],
        unique=False,
    )
    alembic.op.create_index(
        alembic.op.f("pull_request_review_submitted_at_idx"),
        "pull_request_review",
        ["submitted_at"],
        unique=False,
    )


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
