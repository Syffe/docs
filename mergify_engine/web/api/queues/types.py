from __future__ import annotations

import dataclasses

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
