import base64
import typing

import msgpack
import pytest

from mergify_engine import pagination


@pytest.mark.parametrize(
    ("value", "valid_type", "cursor_txt"),
    [
        (20, pagination.CursorType[int], "gqV2YWx1ZRSnZm9yd2FyZMM="),
        (
            "abcd",
            pagination.CursorType[str],
            "gqV2YWx1ZaRhYmNkp2ZvcndhcmTD",
        ),
        (
            "with smiley: 😁",
            pagination.CursorType[str],
            "gqV2YWx1ZbF3aXRoIHNtaWxleTog8J-Ygadmb3J3YXJkww==",
        ),
        (
            "with url forbidden characters: / : \\ - * { []}",
            pagination.CursorType[str],
            "gqV2YWx1Zdkud2l0aCB1cmwgZm9yYmlkZGVuIGNoYXJhY3RlcnM6IC8gOiBcIC0gKiB7IFtdfadmb3J3YXJkww==",
        ),
        (
            [1, 2, 3, "a", "b", "c"],
            pagination.CursorType[list[int | str]],
            "gqV2YWx1ZZYBAgOhYaFioWOnZm9yd2FyZMM=",
        ),
        (
            {"abc": 1, "def": "ghi"},
            pagination.CursorType[dict[str, int | str]],
            "gqV2YWx1ZYKjYWJjAaNkZWajZ2hpp2ZvcndhcmTD",
        ),
    ],
)
def test_cursor_value(
    value: object,
    valid_type: type[pagination.CursorType[typing.Any]],
    cursor_txt: str,
) -> None:
    cursor = pagination.Cursor(value, forward=True)

    assert cursor.value(valid_type) == value

    assert cursor.to_string() == cursor_txt

    assert pagination.Cursor.from_string(cursor_txt).value(valid_type) == value

    assert pagination.Cursor.from_string(cursor_txt).to_string() == cursor_txt


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


@pytest.mark.parametrize(
    ("forward", "first_id", "last_id", "expected_next", "expected_previous"),
    [
        (
            True,
            20,
            30,
            pagination.Cursor(30, forward=True),
            pagination.Cursor(20, forward=False),
        ),
        (
            True,
            20,
            None,
            pagination.Cursor(None, forward=True),
            pagination.Cursor(20, forward=False),
        ),
        (
            True,
            None,
            30,
            pagination.Cursor(30, forward=True),
            pagination.Cursor(None, forward=False),
        ),
        (
            True,
            None,
            None,
            pagination.Cursor(None, forward=True),
            pagination.Cursor(None, forward=False),
        ),
        (
            False,
            20,
            30,
            pagination.Cursor(20, forward=False),
            pagination.Cursor(30, forward=True),
        ),
        (
            False,
            20,
            None,
            pagination.Cursor(20, forward=False),
            pagination.Cursor(None, forward=False),
        ),
        (
            False,
            None,
            30,
            pagination.Cursor(None, forward=True),
            pagination.Cursor(30, forward=True),
        ),
        (
            False,
            None,
            None,
            pagination.Cursor(None, forward=True),
            pagination.Cursor(None, forward=False),
        ),
    ],
)
def test_cursor_next_previous(
    forward: bool,
    first_id: int | None,
    last_id: int | None,
    expected_next: pagination.Cursor,
    expected_previous: pagination.Cursor,
) -> None:
    cursor = pagination.Cursor(20, forward=forward)

    next_cursor = cursor.next(first_id, last_id)
    assert next_cursor == expected_next
    assert next_cursor.from_string(next_cursor.to_string()) == expected_next

    prev_cursor = cursor.previous(first_id, last_id)
    assert prev_cursor == expected_previous
    assert prev_cursor.from_string(prev_cursor.to_string()) == expected_previous
