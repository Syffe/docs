import datetime
import typing

import pytest
import voluptuous

from mergify_engine import condition_value_querier
from mergify_engine import rules
from mergify_engine.rules import conditions as rule_conditions
from mergify_engine.rules.config import pull_request_rules


@pytest.mark.parametrize(
    "condition,expected_markdown",
    (
        (
            rule_conditions.ConditionEvaluationResult(
                match=True,
                label="author=me",
                is_label_user_input=True,
            ),
            "- [X] `author=me`",
        ),
        (
            rule_conditions.ConditionEvaluationResult(
                match=False,
                label="author=me",
                is_label_user_input=True,
            ),
            "- [ ] `author=me`",
        ),
        (
            rule_conditions.ConditionEvaluationResult(
                match=True,
                label="author=me",
                is_label_user_input=False,
            ),
            "- [X] author=me",
        ),
        (
            rule_conditions.ConditionEvaluationResult(
                match=True,
                label="author=me",
                is_label_user_input=True,
                description="Some description",
            ),
            "- [X] `author=me` [Some description]",
        ),
        (
            rule_conditions.ConditionEvaluationResult(
                match=True,
                label="author=me",
                is_label_user_input=True,
                evaluation_error="Some error",
            ),
            "- [X] `author=me` ⚠️ Some error",
        ),
    ),
)
async def test_markdown(
    condition: rule_conditions.ConditionEvaluationResult, expected_markdown: str
) -> None:
    assert condition._as_markdown_element() == expected_markdown


async def test_group_condition_as_markdown() -> None:
    condition = rule_conditions.ConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.ConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
            )
        ],
    )
    assert condition.as_markdown() == "- [X] `base=main`"


async def test_condition_tree_as_markdown() -> None:
    condition = rule_conditions.ConditionEvaluationResult(
        match=False,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.ConditionEvaluationResult(
                match=False,
                label="all of",
                is_label_user_input=False,
                subconditions=[
                    rule_conditions.ConditionEvaluationResult(
                        match=False,
                        label="base=main",
                        is_label_user_input=True,
                    )
                ],
            )
        ],
    )
    expected_markdown = """\
- [ ] all of:
  - [ ] `base=main`"""

    assert condition.as_markdown() == expected_markdown


async def test_condition_dict_serialization() -> None:
    condition = rule_conditions.ConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.ConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
                description="Some description",
                evaluation_error="Some error",
                related_checks=["ci"],
                next_evaluation_at=datetime.datetime(2022, 1, 10, 14, 30),
            )
        ],
    )

    assert condition.serialized() == {
        "match": True,
        "label": "all of",
        "is_label_user_input": False,
        "description": None,
        "evaluation_error": None,
        "related_checks": [],
        "next_evaluation_at": None,
        "subconditions": [
            {
                "match": True,
                "label": "base=main",
                "is_label_user_input": True,
                "description": "Some description",
                "evaluation_error": "Some error",
                "related_checks": ["ci"],
                "next_evaluation_at": datetime.datetime(2022, 1, 10, 14, 30),
                "subconditions": [],
            }
        ],
    }
    assert (
        rule_conditions.ConditionEvaluationResult.deserialize(condition.serialized())
        == condition
    )


async def test_condition_dict_serialization_with_default_values() -> None:
    condition = rule_conditions.ConditionEvaluationResult(
        match=True,
        label="all of",
        is_label_user_input=False,
        subconditions=[
            rule_conditions.ConditionEvaluationResult(
                match=True,
                label="base=main",
                is_label_user_input=True,
                description="Some description",
                evaluation_error="Some error",
            )
        ],
    )
    condition_dict = typing.cast(
        rule_conditions.ConditionEvaluationResult.Serialized,
        {
            "match": True,
            "label": "all of",
            "is_label_user_input": False,
            "description": None,
            "evaluation_error": None,
            "subconditions": [
                {
                    "match": True,
                    "label": "base=main",
                    "is_label_user_input": True,
                    "description": "Some description",
                    "evaluation_error": "Some error",
                    "subconditions": [],
                }
            ],
        },
    )

    assert (
        rule_conditions.ConditionEvaluationResult.deserialize(condition_dict)
        == condition
    )
    assert condition.related_checks == []
    assert condition.next_evaluation_at is None


async def test_condition_evaluation_result_serialization_with_related_checks() -> None:
    class MockedContext:
        @property
        async def checks(self) -> dict[str, typing.Any]:
            return {}

        @property
        def pull(self) -> dict[str, typing.Any]:
            return {"labels": []}

    pr = condition_value_querier.PullRequest(MockedContext())  # type: ignore[arg-type]

    config = """
pull_request_rules:
  - name: blablawhocares
    conditions:
      - label=hotfix
      - "#check-failure>0"
    actions: {}
"""

    prrules = voluptuous.Schema(pull_request_rules.get_pull_request_rules_schema())(
        rules.YamlSchema(config)["pull_request_rules"]
    )

    for prr in prrules:
        evaluated_rules = await prr.evaluate([pr])
        break

    assert (
        evaluated_rules.conditions.condition._conditions[1].label == "#check-failure>0"
    )
    assert evaluated_rules.conditions.condition._conditions[1].related_checks == []

    # Just to validate that we can safely transform the conditions
    # to the pydantic dataclass
    evaluated_rules.conditions.get_summary()
