import typing

import pytest

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

    assert condition.as_dict() == {
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
                        "related_checks": ["ci"],
                    }
                ],
            }
        ],
        "evaluations": [],
    }
    assert (
        rule_conditions.QueueConditionEvaluationResult.from_dict(condition.as_dict())
        == condition
    )


async def test_condition_dict_serialization_missing_related_checks() -> None:
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
        rule_conditions.QueueConditionEvaluationResult.from_dict(condition_dict)
        == condition
    )


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

    expected = rule_conditions.QueueConditionEvaluationJsonSerialized(
        match=True,
        label="all of",
        description=None,
        subconditions=[
            rule_conditions.QueueConditionEvaluationJsonSerialized(
                match=True,
                label="base=main",
                description="Some description",
                subconditions=[],
                evaluations=[
                    rule_conditions.QueueConditionEvaluationJsonSerialized.Evaluation(
                        pull_request=github_types.GitHubPullRequestNumber(1),
                        match=False,
                        evaluation_error="Some error",
                        related_checks=["ci"],
                    )
                ],
            )
        ],
        evaluations=[],
    )

    assert condition.as_json_dict() == expected
