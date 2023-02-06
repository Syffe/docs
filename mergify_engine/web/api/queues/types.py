from __future__ import annotations

import dataclasses

import pydantic

from mergify_engine import rules


@pydantic.dataclasses.dataclass
class QueueRule:
    name: rules.QueueName = dataclasses.field(
        metadata={"description": "The name of the queue rule"}
    )

    # FIXME(sileht): internal data representation leaks, not a big deal for now, but not perfect neither.
    config: rules.QueueConfig = dataclasses.field(
        metadata={"description": "The configuration of the queue rule"}
    )
