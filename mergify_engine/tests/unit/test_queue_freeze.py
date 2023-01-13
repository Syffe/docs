import datetime
from unittest import mock

import pytest

from mergify_engine import json
from mergify_engine.queue import freeze


@pytest.mark.parametrize(
    "name,reason,application_name,application_id,freeze_date,cascading",
    (
        (
            "default",
            "test default freeze",
            "application toto",
            123,
            str(datetime.datetime.utcnow()),
            True,
        ),
        (
            "urgent",
            "test urgent freeze",
            "application tata",
            666,
            str(datetime.datetime.utcnow()),
            False,
        ),
        (
            "low",
            "test low freeze",
            "application tutu",
            777,
            str(datetime.datetime.utcnow()),
            True,
        ),
    ),
)
def test_queue_freeze_deserialize(
    name: str,
    reason: str,
    application_name: str,
    application_id: int,
    freeze_date: datetime.datetime,
    cascading: bool,
) -> None:

    serialized_payload = {
        "name": name,
        "reason": reason,
        "application_name": application_name,
        "application_id": application_id,
        "freeze_date": freeze_date,
        "cascading": cascading,
    }

    untyped_serialized_payload = json.loads(json.dumps(serialized_payload))
    repository = mock.Mock()
    queue_freeze = freeze.QueueFreeze.deserialize(
        repository, untyped_serialized_payload
    )

    assert queue_freeze.name == name
    assert queue_freeze.reason == reason
    assert queue_freeze.application_name == application_name
    assert queue_freeze.application_id == application_id
    assert queue_freeze.freeze_date == freeze_date
    assert queue_freeze.cascading == cascading


@pytest.mark.parametrize(
    "name,reason,application_name,application_id,freeze_date,cascading",
    (
        (
            "default",
            "test default freeze",
            "application toto",
            123,
            str(datetime.datetime.utcnow()),
            True,
        ),
        (
            "urgent",
            "test urgent freeze",
            "application tata",
            666,
            str(datetime.datetime.utcnow()),
            False,
        ),
        (
            "low",
            "test low freeze",
            "application tutu",
            777,
            str(datetime.datetime.utcnow()),
            True,
        ),
    ),
)
def test_queue_freeze_serialized(
    name: str,
    reason: str,
    application_name: str,
    application_id: int,
    freeze_date: datetime.datetime,
    cascading: bool,
) -> None:

    serialized_payload = {
        "name": name,
        "reason": reason,
        "application_name": application_name,
        "application_id": application_id,
        "freeze_date": freeze_date,
        "cascading": cascading,
    }

    repository = mock.Mock()
    queue_freeze = freeze.QueueFreeze(
        repository=repository,
        name=name,
        reason=reason,
        application_name=application_name,
        application_id=application_id,
        freeze_date=freeze_date,
        cascading=cascading,
    )
    serialized_queue_freeze = queue_freeze.serialized()
    assert serialized_queue_freeze == serialized_payload
