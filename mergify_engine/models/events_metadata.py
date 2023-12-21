from __future__ import annotations

import datetime  # noqa: TCH003
import typing

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql
import sqlalchemy.ext.asyncio
import sqlalchemy.ext.hybrid

from mergify_engine import models
from mergify_engine.models import enumerations


if typing.TYPE_CHECKING:
    from mergify_engine.queue.merge_train import checks


class SpeculativeCheckPullRequest(models.Base):
    __tablename__ = "speculative_check_pull_request"

    id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.BigInteger,
        primary_key=True,
        autoincrement=True,
        anonymizer_config=None,
    )
    number: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        anonymizer_config="anon.random_int_between(0, 50)",
    )
    in_place: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    checks_timed_out: orm.Mapped[bool] = orm.mapped_column(
        sqlalchemy.Boolean,
        anonymizer_config="anon.random_int_between(0,1)",
    )
    checks_started_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        anonymizer_config="anon.dnoise(checks_started_at, ''2 days'')",
    )
    checks_ended_at: orm.Mapped[datetime.datetime | None] = orm.mapped_column(
        sqlalchemy.DateTime(timezone=True),
        nullable=True,
        anonymizer_config="anon.dnoise(checks_ended_at, ''2 days'')",
    )
    checks_conclusion: orm.Mapped[
        enumerations.CheckConclusionWithStatuses | None
    ] = orm.mapped_column(
        sqlalchemy.Enum(enumerations.CheckConclusionWithStatuses),
        nullable=True,
        anonymizer_config="anon.random_in_enum(checks_conclusion)",
    )
    unsuccessful_checks: orm.Mapped[
        list[checks.QueueCheck.Serialized]
    ] = orm.mapped_column(
        postgresql.JSONB,
        anonymizer_config=(
            "custom_masks.json_obj_array(0, 5, ARRAY[''name'', ''description'', ''state'', "
            "''url'', ''avatar_url''])"
        ),
    )
    event_id: orm.Mapped[int] = orm.mapped_column(
        sqlalchemy.Integer,
        sqlalchemy.ForeignKey("event.id", ondelete="CASCADE"),
        index=True,
        anonymizer_config=None,
    )

    @sqlalchemy.ext.hybrid.hybrid_property
    def ci_runtime(self) -> float | None:
        if self.checks_ended_at is not None and self.checks_started_at is not None:
            return (self.checks_ended_at - self.checks_started_at).total_seconds()
        return None

    @ci_runtime.inplace.expression
    @classmethod
    def _ci_runtime_expression(cls) -> sqlalchemy.ColumnElement[float]:
        return sqlalchemy.type_coerce(
            sqlalchemy.extract(
                "epoch",
                cls.checks_ended_at - cls.checks_started_at,
            ),
            sqlalchemy.Float,
        )
