import base64
import typing

import msgpack
import pytest

from mergify_engine import pagination


@pytest.mark.parametrize(
    ("value", "valid_type"),
    [
        (20, pagination.CursorType[int]),
        ("abcd", pagination.CursorType[str]),
        ("with smiley: 😁", pagination.CursorType[str]),
        ("/ : \\ - * { []}", pagination.CursorType[str]),
        (
            [1, 2, 3, "a", "b", "c"],
            pagination.CursorType[list[int | str]],
        ),
        ({"abc": 1, "def": "ghi"}, pagination.CursorType[dict[str, int | str]]),
    ],
)
def test_cursor_value(
    value: object,
    valid_type: type[pagination.CursorType[typing.Any]],
) -> None:
    cursor = pagination.Cursor(value, forward=True)

    assert cursor.value(valid_type) == value

    assert pagination.Cursor.from_string(cursor.to_string()).value(valid_type) == value


@pytest.mark.parametrize(
    ("value", "invalid_type"),
    [
        (20, pagination.CursorType[str]),
        ("abcd", pagination.CursorType[int]),
        (
            [1, 2, 3, "a", "b", "c"],
            pagination.CursorType[list[int]],
        ),
        ({"abc": 1, "def": "ghi"}, pagination.CursorType[dict[str, int]]),
    ],
)
def test_cursor_value_invalid_type(
    value: object,
    invalid_type: type[pagination.CursorType[typing.Any]],
) -> None:
    cursor = pagination.Cursor(value, forward=True)

    with pytest.raises(pagination.InvalidCursorError):
        cursor.value(invalid_type)


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("abcd", id="random string"),
        pytest.param(base64.urlsafe_b64encode(b"abcd").decode(), id="random base64"),
        pytest.param(
            (base64.urlsafe_b64encode(b"abcd") + b"random").decode(),
            id="invalid base64",
        ),
        pytest.param(
            base64.urlsafe_b64encode(msgpack.dumps("abcd") + b"ramdom").decode(),
            id="invalid msgpack",
        ),
        pytest.param(
            base64.urlsafe_b64encode(msgpack.dumps({"value": "abcd"})).decode(),
            id="Missing value",
        ),
    ],
)
def test_cursor_invalid_string(value: str) -> None:
    with pytest.raises(pagination.InvalidCursorError) as exc:
        pagination.Cursor.from_string(value)
    assert str(exc.value) == value
