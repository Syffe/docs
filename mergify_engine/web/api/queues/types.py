from __future__ import annotations

import dataclasses
import typing

import pydantic

from mergify_engine.rules.config import queue_rules as qr_config


@pydantic.dataclasses.dataclass
class QueueRule:
    name: qr_config.QueueName = dataclasses.field(
        metadata={"description": "The name of the queue rule"}
    )

    # FIXME(sileht): internal data representation leaks, not a big deal for now, but not perfect neither.
    config: qr_config.QueueConfig = dataclasses.field(
        metadata={"description": "The configuration of the queue rule"}
    )

    @pydantic.field_serializer("config")
    def serialize_config(
        self, config: qr_config.QueueConfig, _info: typing.Any
    ) -> dict[str, typing.Any]:
        d = typing.cast(dict[str, typing.Any], config.copy())
        d.update(
            {
                "batch_max_wait_time": config["batch_max_wait_time"].total_seconds(),
                "checks_timeout": config["checks_timeout"].total_seconds()
                if config["checks_timeout"] is not None
                else None,
            }
        )
        return d
