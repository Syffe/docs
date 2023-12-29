import typing

import pytest

from mergify_engine import github_types
from mergify_engine.tests.functional import utils as tests_utils


@pytest.mark.parametrize(
    ("data", "expected_data", "should_match"),
    (
        (
            {
                "check_run": {
                    "name": "test",
                    "pull_requests": [
                        {"number": 123, "id": 456, "head": {"ref": "blabla"}},
                    ],
                },
            },
            tests_utils.get_check_run_event_payload(
                pr_number=github_types.GitHubPullRequestNumber(123),
            ),
            True,
        ),
        (
            {
                "check_run": {
                    "name": "test",
                    "pull_requests": [
                        {"number": 123, "id": 456, "head": {"ref": "blabla"}},
                        {"number": 456, "id": 456, "head": {"ref": "blabla"}},
                    ],
                },
            },
            tests_utils.get_check_run_event_payload(
                pr_number=github_types.GitHubPullRequestNumber(123),
            ),
            True,
        ),
        (
            {
                "check_run": {
                    "name": "test",
                    "pull_requests": [
                        {"number": 456, "id": 456, "head": {"ref": "blabla"}},
                    ],
                },
            },
            tests_utils.get_check_run_event_payload(
                pr_number=github_types.GitHubPullRequestNumber(123),
            ),
            False,
        ),
    ),
)
def test_match_expected_data(
    data: typing.Any,
    expected_data: typing.Any,
    should_match: bool,
) -> None:
    assert tests_utils.match_expected_data(data, expected_data) == should_match
