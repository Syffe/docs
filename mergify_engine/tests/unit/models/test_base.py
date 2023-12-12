import datetime
import typing

import pytest

from mergify_engine import models
from mergify_engine.models.github.account import GitHubAccountType


@pytest.mark.parametrize(
    ("value", "expected_value"),
    [
        (
            datetime.datetime(2023, 11, 10, 10, 5, 23, tzinfo=datetime.UTC),
            "2023-11-10T10:05:23Z",
        ),
        (GitHubAccountType.USER, "User"),
        (GitHubAccountType.ORGANIZATION, "Organization"),
    ],
)
def test_as_github_dict_value_transformer(
    value: typing.Any,
    expected_value: typing.Any,
) -> None:
    assert models.Base._as_github_dict_value_transformer(value) == expected_value
