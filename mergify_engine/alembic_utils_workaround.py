import typing

from alembic_utils import reversible_op


# FIXME(sileht): remove me on next alembic_utils release
# Monkeypatch from: https://github.com/olirice/alembic_utils/pull/115


def drop_op_to_diff_tuple(self: typing.Any) -> typing.Any:
    return "drop_entity", self.target.identity


def create_op_to_diff_tuple(self: typing.Any) -> typing.Any:
    return (
        "create_entity",
        self.target.identity,
        str(self.target.to_sql_statement_create()),
    )


def replace_op_to_diff_tuple(self: typing.Any) -> typing.Any:
    return (
        "replace_or_revert_entity",
        self.target.identity,
        str(self.target.to_sql_statement_create_or_replace()),
    )


reversible_op.CreateOp.to_diff_tuple = create_op_to_diff_tuple
reversible_op.DropOp.to_diff_tuple = drop_op_to_diff_tuple
reversible_op.ReplaceOp.to_diff_tuple = replace_op_to_diff_tuple
