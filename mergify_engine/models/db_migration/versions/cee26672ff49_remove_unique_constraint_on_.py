"""remove unique constraint on GitHubAccount.login

Revision ID: cee26672ff49
Revises: d4a7ed6380c9
Create Date: 2023-11-02 17:01:18.138695

"""
import alembic


revision = "cee26672ff49"
down_revision = "d4a7ed6380c9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    alembic.op.drop_constraint(
        "github_account_login_key",
        "github_account",
        type_="unique",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # NOTE(sileht): We don't want to support downgrades as it means we will
    # drop columns. And we don't want to provide tooling that may drop data.
    # For restoring old version of the database, the only supported process is
    # to use a backup.
    pass
