import typing

import pytest

from mergify_engine import check_api


def test_conclusion_str() -> None:
    assert str(check_api.Conclusion(None)) == "🟠 pending"
    assert str(check_api.Conclusion("success")) == "✅ success"


@pytest.mark.parametrize(
    ("left", "right", "keys", "expected"),
    (
        ({}, {}, ("a",), True),
        ({"a": 1}, {"a": 1}, ("a",), True),
        ({"a": 1}, {"a": 2}, ("a",), False),
        ({"a": 1}, {"a": 1}, ("a", "b"), True),
        ({"a": 1, "b": 1}, {"a": 1, "b": 1}, ("a", "b"), True),
        ({"a": 1, "b": 1}, {"a": 1, "b": 3}, ("a", "b"), False),
        ({"a": 1}, {"a": 1}, (), True),
    ),
)
def test_compare_dict(
    left: dict[str, typing.Any],
    right: dict[str, typing.Any],
    keys: tuple[str, ...],
    expected: bool,
) -> None:
    assert check_api.compare_dict(left, right, keys) == expected
