import datetime
import typing

import deepdiff
import pytest

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.rules import conditions as rule_conditions


@pytest.mark.parametrize(
    "condition,expected_markdown",
    (
        (
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="author=me",
                is_label_user_input=True,
            ),
            "- [X] `author=me`",
        ),
        (
            rule_conditions.QueueConditionEvaluationResult(
                match=False,
                label="author=me",
                is_label_user_input=True,
            ),
            "- [ ] `author=me`",
        ),
        (
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="author=me",
                is_label_user_input=False,
            ),
            "- [X] author=me",
        ),
        (
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="author=me",
                is_label_user_input=True,
                description="Some description",
            ),
            "- [X] `author=me` [Some description]",
        ),
        (
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="author=me",
                attribute_name="author",
                is_label_user_input=True,
                evaluations=[
                    rule_conditions.QueueConditionEvaluationResult.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=True,
                    )
                ],
            ),
            """\
- `author=me`
  - [X] #1""",
        ),
        (
            rule_conditions.QueueConditionEvaluationResult(
                match=False,
                label="author=me",
                attribute_name="author",
                is_label_user_input=True,
                evaluations=[
                    rule_conditions.QueueConditionEvaluationResult.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                    )
                ],
            ),
            """\
- `author=me`
  - [ ] #1 ⚠️ Some error""",
        ),
    ),
)
async def test_markdown(
    condition: rule_conditions.QueueConditionEvaluationResult, expected_markdown: str
) -> None:
    assert condition._as_markdown_element() == expected_markdown


async def test_group_condition_as_markdown() -> None:
    condition = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
            )
        ],
    )
    assert condition.as_markdown() == "- [X] `base=main`"


async def test_condition_tree_as_markdown() -> None:
    condition = rule_conditions.QueueConditionEvaluationResult(
        match=False,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=False,
                label="all of",
                is_label_user_input=False,
                subconditions=[
                    rule_conditions.QueueConditionEvaluationResult(
                        match=False,
                        label="base=main",
                        is_label_user_input=True,
                        evaluations=[
                            rule_conditions.QueueConditionEvaluationResult.Evaluation(
                                pull_request=github_types.GitHubPullRequestNumber(1),
                                match=False,
                            )
                        ],
                    )
                ],
            )
        ],
    )
    expected_markdown = """\
- [ ] all of:
  - `base=main`
    - [ ] #1"""

    assert condition.as_markdown() == expected_markdown


async def test_condition_dict_serialization() -> None:
    condition = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
                description="Some description",
                attribute_name="base",
                operator="=",
                evaluations=[
                    rule_conditions.QueueConditionEvaluationResult.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                        related_checks=["ci"],
                        next_evaluation_at=None,
                    )
                ],
            ),
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="schedule=MON-FRI",
                is_label_user_input=True,
                description="Some description",
                attribute_name="schedule",
                operator="=",
                schedule=date.Schedule.from_string("MON-FRI"),
                evaluations=[
                    rule_conditions.QueueConditionEvaluationResult.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                        related_checks=[],
                        next_evaluation_at=datetime.datetime(
                            2023, 1, 10, 14, 30, tzinfo=date.UTC
                        ),
                    )
                ],
            ),
        ],
    )

    assert condition.serialized() == {
        "match": True,
        "label": "all of",
        "is_label_user_input": False,
        "description": None,
        "attribute_name": None,
        "operator": None,
        "schedule": None,
        "subconditions": [
            {
                "match": True,
                "label": "base=main",
                "is_label_user_input": True,
                "description": "Some description",
                "attribute_name": "base",
                "operator": "=",
                "schedule": None,
                "subconditions": [],
                "evaluations": [
                    {
                        "pull_request": 1,
                        "match": False,
                        "evaluation_error": "Some error",
                        "related_checks": ["ci"],
                        "next_evaluation_at": None,
                    }
                ],
            },
            {
                "match": True,
                "label": "schedule=MON-FRI",
                "is_label_user_input": True,
                "description": "Some description",
                "attribute_name": "schedule",
                "operator": "=",
                "schedule": {
                    "start_weekday": 1,
                    "end_weekday": 5,
                    "start_hour": 0,
                    "end_hour": 23,
                    "start_minute": 0,
                    "end_minute": 59,
                    "tzinfo": "UTC",
                    "is_only_days": True,
                    "is_only_times": False,
                },
                "subconditions": [],
                "evaluations": [
                    {
                        "pull_request": 1,
                        "match": False,
                        "evaluation_error": "Some error",
                        "related_checks": [],
                        "next_evaluation_at": datetime.datetime(
                            2023, 1, 10, 14, 30, tzinfo=date.UTC
                        ),
                    }
                ],
            },
        ],
        "evaluations": [],
    }
    assert (
        rule_conditions.QueueConditionEvaluationResult.deserialize(
            condition.serialized()
        )
        == condition
    )


async def test_condition_dict_serialization_with_default_values() -> None:
    condition = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
                description="Some description",
                attribute_name="base",
                evaluations=[
                    rule_conditions.QueueConditionEvaluationResult.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                    )
                ],
            )
        ],
    )
    condition_dict = typing.cast(
        rule_conditions.QueueConditionEvaluationResult.Serialized,
        {
            "match": True,
            "label": "all of",
            "is_label_user_input": False,
            "description": None,
            "attribute_name": None,
            "subconditions": [
                {
                    "match": True,
                    "label": "base=main",
                    "is_label_user_input": True,
                    "description": "Some description",
                    "attribute_name": "base",
                    "subconditions": [],
                    "evaluations": [
                        {
                            "pull_request": 1,
                            "match": False,
                            "evaluation_error": "Some error",
                        }
                    ],
                }
            ],
            "evaluations": [],
        },
    )

    assert (
        rule_conditions.QueueConditionEvaluationResult.deserialize(condition_dict)
        == condition
    )
    assert condition.subconditions[0].evaluations[0].related_checks == []
    assert condition.subconditions[0].evaluations[0].next_evaluation_at is None


async def test_condition_copy() -> None:
    condition = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
                description="Some description",
                attribute_name="base",
                operator="=",
                schedule=date.Schedule.from_string("MON-FRI"),
                evaluations=[
                    rule_conditions.QueueConditionEvaluationResult.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                        related_checks=["ci"],
                    )
                ],
            )
        ],
    )
    condition_copy = condition.copy()

    assert condition == condition_copy
    assert condition is not condition_copy

    subcondition = condition.subconditions[0]
    subcondition_copy = condition_copy.subconditions[0]
    assert subcondition is not subcondition_copy
    assert subcondition.evaluations[0] is not subcondition_copy.evaluations[0]


async def test_condition_json_serialization() -> None:
    condition = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
                description="Some description",
                attribute_name="base",
                schedule=date.Schedule.from_string("MON-FRI"),
                evaluations=[
                    rule_conditions.QueueConditionEvaluationResult.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                        related_checks=["ci"],
                        next_evaluation_at=datetime.datetime(
                            2023, 1, 10, 14, 30, tzinfo=date.UTC
                        ),
                    )
                ],
            )
        ],
    )

    expected = rule_conditions.QueueConditionEvaluationJsonSerialized(
        match=True,
        label="all of",
        description=None,
        schedule=None,
        subconditions=[
            rule_conditions.QueueConditionEvaluationJsonSerialized(
                match=True,
                label="base=main",
                description="Some description",
                schedule={
                    "timezone": "UTC",
                    "days": {
                        "monday": date.FULL_DAY,
                        "tuesday": date.FULL_DAY,
                        "wednesday": date.FULL_DAY,
                        "thursday": date.FULL_DAY,
                        "friday": date.FULL_DAY,
                        "saturday": date.EMPTY_DAY,
                        "sunday": date.EMPTY_DAY,
                    },
                },
                subconditions=[],
                evaluations=[
                    rule_conditions.QueueConditionEvaluationJsonSerialized.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                        related_checks=["ci"],
                        next_evaluation_at=datetime.datetime(
                            2023, 1, 10, 14, 30, tzinfo=date.UTC
                        ),
                    )
                ],
            )
        ],
        evaluations=[],
    )

    assert condition.as_json_dict() == expected


async def test_condition_json_serialization_reversed_schedule() -> None:
    condition = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="schedule!=MON-FRI 11:00-15:30",
        is_label_user_input=True,
        description="Some description",
        attribute_name="schedule",
        operator="!=",
        schedule=date.Schedule.from_string("MON-FRI 11:00-15:30"),
    )

    expected_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 0, "minute": 0},
                "end_at": {"hour": 11, "minute": 0},
            },
            {
                "start_at": {"hour": 15, "minute": 30},
                "end_at": {"hour": 23, "minute": 59},
            },
        ]
    }
    expected = rule_conditions.QueueConditionEvaluationJsonSerialized(
        match=True,
        label="schedule!=MON-FRI 11:00-15:30",
        description="Some description",
        schedule={
            "timezone": "UTC",
            "days": {
                "monday": expected_day,
                "tuesday": expected_day,
                "wednesday": expected_day,
                "thursday": expected_day,
                "friday": expected_day,
                "saturday": date.FULL_DAY,
                "sunday": date.FULL_DAY,
            },
        },
        subconditions=[],
        evaluations=[],
    )

    assert condition.as_json_dict() == expected


def test_deepdiff() -> None:
    condition1 = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=True,
                label="schedule=MON-FRI",
                is_label_user_input=True,
                schedule=date.Schedule.from_string("MON-FRI"),
            )
        ],
    )

    condition2 = rule_conditions.QueueConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.QueueConditionEvaluationResult(
                match=False,
                label="schedule=MON-FRI",
                is_label_user_input=True,
                schedule=date.Schedule.from_string("MON-FRI"),
            )
        ],
    )

    # NOTE(charly): deepdiff is used by TrainCar to compare two
    # QueueConditionEvaluationResult, triggering a summary update if necessary
    diff_result = deepdiff.DeepDiff(
        condition1, condition2, ignore_order=True, exclude_types=[date.Schedule]
    )
    assert diff_result
    assert diff_result.affected_paths
