import pytest
import voluptuous

from mergify_engine.rules.config import queue_rules


def test_has_only_one_of_min_value() -> None:
    with pytest.raises(
        ValueError, match=r"^Need at least 2 keys to check for exclusivity$"
    ):
        queue_rules._has_only_one_of("foo")


def test_has_only_one() -> None:
    queue_rules._has_only_one_of("foo", "bar")({"foo": 1, "baz": 2})


def test_has_only_one_invalid() -> None:
    with pytest.raises(voluptuous.Invalid, match=r"^Must contain only one of foo,bar$"):
        queue_rules._has_only_one_of("foo", "bar")({"foo": 1, "bar": 2})
