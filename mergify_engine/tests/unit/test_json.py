import datetime
import enum
import json
import uuid

import pytest

from mergify_engine import json as mergify_json


@mergify_json.register_enum_type
class Color(enum.Enum):
    RED = 1
    GREEN = 2
    BLUE = 3


uuid_str = "dd0df591c1bb4a43a3c0a333bf41778e"

payload_decoded = {
    "name": "hello",
    "conditions": [],
    "actions": {"merge": {"strict": Color.BLUE}},
    "timedelta": datetime.timedelta(hours=2, minutes=5, seconds=5),
    "datetime_naive": datetime.datetime(2021, 5, 15, 8, 35, 36, 442306),  # noqa: DTZ001
    "datetime_aware": datetime.datetime(
        2021,
        5,
        15,
        8,
        41,
        36,
        796485,
        tzinfo=datetime.UTC,
    ),
    "a set": {1, 2, 3},
    "uuid": uuid.UUID(uuid_str),
}

payload_encoded = {
    "name": "hello",
    "conditions": [],
    "actions": {
        "merge": {"strict": {"__pytype__": "enum", "class": "Color", "name": "BLUE"}},
    },
    "datetime_naive": {
        "__pytype__": "datetime.datetime",
        "value": "2021-05-15T08:35:36.442306",
    },
    "datetime_aware": {
        "__pytype__": "datetime.datetime",
        "value": "2021-05-15T08:41:36.796485+00:00",
    },
    "timedelta": {
        "__pytype__": "datetime.timedelta",
        "value": "7505.0",
    },
    "a set": {
        "__pytype__": "set",
        "value": [1, 2, 3],
    },
    "uuid": {
        "__pytype__": "uuid.UUID",
        "value": uuid_str,
    },
}


def test_register_type_fail() -> None:
    with pytest.raises(RuntimeError):
        mergify_json.register_enum_type(Color)


def test_encode() -> None:
    assert json.loads(mergify_json.dumps(payload_decoded)) == payload_encoded


def test_decode() -> None:
    json_file = json.dumps(payload_encoded)
    assert mergify_json.loads(json_file) == payload_decoded
