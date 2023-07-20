from __future__ import annotations

import datetime

import sqlalchemy
from sqlalchemy import orm
from sqlalchemy.dialects import postgresql

from mergify_engine import models
from mergify_engine.models import enumerations
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
        sqlalchemy.Integer, anonymizer_config="anon.random_int_between(0, 50)"
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
            "custom_masks.json_obj_array(0, 5, ARRAY['name', 'description', 'state', "
            "'url', 'avatar_url'])"
        ),
    )
