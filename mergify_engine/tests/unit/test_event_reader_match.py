import typing

import pytest

from mergify_engine import github_types
from mergify_engine.tests.functional import event_reader
from mergify_engine.tests.functional import utils as tests_utils


@pytest.mark.parametrize(
    ("data", "expected_data", "should_match"),
    (
        (
            {
                "name": "test",
                "pull_requests": [
                    {"number": 123, "id": 456, "head": {"ref": "blabla"}},
                ],
            },
            tests_utils.get_check_run_event_payload(
                pr_number=github_types.GitHubPullRequestNumber(123),
            ),
            True,
        ),
        (
            {
                "name": "test",
                "pull_requests": [
                    {"number": 123, "id": 456, "head": {"ref": "blabla"}},
                    {"number": 456, "id": 456, "head": {"ref": "blabla"}},
                ],
            },
            tests_utils.get_check_run_event_payload(
                pr_number=github_types.GitHubPullRequestNumber(123),
            ),
            True,
        ),
        (
            {
                "name": "test",
                "pull_requests": [
                    {"number": 456, "id": 456, "head": {"ref": "blabla"}},
                ],
            },
            tests_utils.get_check_run_event_payload(
                pr_number=github_types.GitHubPullRequestNumber(123),
            ),
            False,
        ),
    ),
)
def test_event_reader_match(
    data: typing.Any,
    expected_data: typing.Any,
    should_match: bool,
) -> None:
    assert event_reader._match(data, expected_data) == should_match
