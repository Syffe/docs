import datetime
from unittest import mock

import pytest

from mergify_engine import json
from mergify_engine.queue import freeze


@pytest.mark.parametrize(
    "name,reason,cascading",
    (
        (
            "default",
            "test default freeze",
            True,
        ),
        (
            "urgent",
            "test urgent freeze",
            False,
        ),
        (
            "low",
            "test low freeze",
            True,
        ),
    ),
)
def test_queue_freeze_deserialize(
    name: str,
    reason: str,
    cascading: bool,
) -> None:
    freeze_date = datetime.datetime(2022, 1, 23, tzinfo=datetime.UTC)
    serialized_payload = {
        "name": name,
        "reason": reason,
        "freeze_date": freeze_date,
        "cascading": cascading,
    }

    untyped_serialized_payload = json.loads(json.dumps(serialized_payload))
    repository = mock.Mock()
    queue_rule = mock.Mock(name=name)
    queue_freeze = freeze.QueueFreeze.deserialize(
        repository, queue_rule, untyped_serialized_payload
    )

    assert queue_freeze.name == name
    assert queue_freeze.reason == reason
    assert queue_freeze.freeze_date == freeze_date
    assert queue_freeze.cascading == cascading


@pytest.mark.parametrize(
    "name,reason,cascading",
    (
        (
            "default",
            "test default freeze",
            True,
        ),
        (
            "urgent",
            "test urgent freeze",
            False,
        ),
        (
            "low",
            "test low freeze",
            True,
        ),
    ),
)
def test_queue_freeze_serialized(
    name: str,
    reason: str,
    cascading: bool,
) -> None:
    freeze_date = datetime.datetime(2022, 1, 23, tzinfo=datetime.UTC)
    serialized_payload = {
        "name": name,
        "reason": reason,
        "freeze_date": freeze_date,
        "cascading": cascading,
    }

    repository = mock.Mock()
    queue_rule = mock.Mock(name=name)
    queue_freeze = freeze.QueueFreeze(
        repository=repository,
        queue_rule=queue_rule,
        name=name,
        reason=reason,
        freeze_date=freeze_date,
        cascading=cascading,
    )
    serialized_queue_freeze = queue_freeze.serialized()
    assert serialized_queue_freeze == serialized_payload
