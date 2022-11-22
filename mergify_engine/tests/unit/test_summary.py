import anys
from freezegun import freeze_time
import voluptuous

from mergify_engine import context
from mergify_engine import date
from mergify_engine import rules
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.tests.unit import conftest


SCHEMA = voluptuous.Schema(
    voluptuous.All(
        [voluptuous.Coerce(rules.RuleConditionSchema)],
        voluptuous.Coerce(conditions_mod.QueueRuleConditions),
    )
)


async def test_queue_rules_order_0_depth() -> None:
    pulls = [
        conftest.FakePullRequest(
            {
                "number": 1,
                "base": "main",
                "label": [],  # type: ignore
            }
        ),
        conftest.FakePullRequest(
            {
                "number": 2,
                "base": "main",
                "label": [],  # type: ignore
            }
        ),
    ]

    async def gen_summary() -> str:
        c: conditions_mod.QueueRuleConditions = SCHEMA(
            [
                "base=fail",
                "base=main",
                "label=test",
            ]
        )
        await c(pulls)  # type: ignore[arg-type]
        return c.get_summary()

    assert (
        """- [ ] `base=fail`
- `label=test`
  - [ ] #1
  - [ ] #2
- [X] `base=main`"""
        in await gen_summary()
    )

    pulls[0].attrs["label"] = ["test"]
    assert (
        """- [ ] `base=fail`
- `label=test`
  - [X] #1
  - [ ] #2
- [X] `base=main`"""
        in await gen_summary()
    )

    pulls[1].attrs["label"] = ["test"]
    assert (
        """- [ ] `base=fail`
- [X] `base=main`
- `label=test`
  - [X] #1
  - [X] #2"""
        in await gen_summary()
    )


async def test_queue_rules_order_operator_and() -> None:
    pulls = [
        conftest.FakePullRequest(
            {
                "author": "anybody",
                "number": 1,
                "base": "main",
                "label": [],  # type: ignore
            }
        ),
        conftest.FakePullRequest(
            {
                "author": "anybody",
                "number": 2,
                "base": "main",
                "label": [],  # type: ignore
            }
        ),
    ]

    async def gen_summary() -> str:
        c: conditions_mod.QueueRuleConditions = SCHEMA(
            [
                "author=somebody",
                "base=main",
                "label=test",
                {
                    "and": [
                        "label=test",
                        "label=test2",
                    ]
                },
            ]
        )
        await c(pulls)  # type: ignore[arg-type]
        return c.get_summary()

    assert (
        """- `author=somebody`
  - [ ] #1
  - [ ] #2
- `label=test`
  - [ ] #1
  - [ ] #2
- [ ] all of:
  - `label=test`
    - [ ] #1
    - [ ] #2
  - `label=test2`
    - [ ] #1
    - [ ] #2
- [X] `base=main`"""
        in await gen_summary()
    )

    pulls[0].attrs["label"] = ["test"]
    assert (
        """- `author=somebody`
  - [ ] #1
  - [ ] #2
- `label=test`
  - [X] #1
  - [ ] #2
- [ ] all of:
  - `label=test`
    - [X] #1
    - [ ] #2
  - `label=test2`
    - [ ] #1
    - [ ] #2
- [X] `base=main`"""
        in await gen_summary()
    )

    pulls[1].attrs["label"] = ["test"]
    assert (
        """- `author=somebody`
  - [ ] #1
  - [ ] #2
- [ ] all of:
  - `label=test2`
    - [ ] #1
    - [ ] #2
  - `label=test`
    - [X] #1
    - [X] #2
- [X] `base=main`
- `label=test`
  - [X] #1
  - [X] #2"""
        in await gen_summary()
    )


async def test_queue_rules_order_operator_or() -> None:
    pulls = [
        conftest.FakePullRequest(
            {
                "author": "anybody",
                "number": 1,
                "base": "main",
                "label": [],  # type: ignore
            }
        ),
        conftest.FakePullRequest(
            {
                "author": "anybody",
                "number": 2,
                "base": "main",
                "label": [],  # type: ignore
            }
        ),
    ]

    async def gen_summary() -> str:
        c: conditions_mod.QueueRuleConditions = SCHEMA(
            [
                "author=somebody",
                "base=main",
                "label=test",
                {
                    "or": [
                        "label=test",
                        "label=test2",
                    ]
                },
            ]
        )
        await c(pulls)  # type: ignore[arg-type]
        return c.get_summary()

    assert (
        """- `author=somebody`
  - [ ] #1
  - [ ] #2
- `label=test`
  - [ ] #1
  - [ ] #2
- [ ] any of:
  - `label=test`
    - [ ] #1
    - [ ] #2
  - `label=test2`
    - [ ] #1
    - [ ] #2
- [X] `base=main`"""
        in await gen_summary()
    )

    pulls[0].attrs["label"] = ["test"]
    assert (
        """- `author=somebody`
  - [ ] #1
  - [ ] #2
- `label=test`
  - [X] #1
  - [ ] #2
- [ ] any of:
  - `label=test`
    - [X] #1
    - [ ] #2
  - `label=test2`
    - [ ] #1
    - [ ] #2
- [X] `base=main`"""
        in await gen_summary()
    )

    pulls[1].attrs["label"] = ["test"]
    assert (
        """- `author=somebody`
  - [ ] #1
  - [ ] #2
- [X] `base=main`
- `label=test`
  - [X] #1
  - [X] #2
- [X] any of:
  - `label=test`
    - [X] #1
    - [X] #2
  - `label=test2`
    - [ ] #1
    - [ ] #2"""
        in await gen_summary()
    )


async def test_condition_summary_simple() -> None:
    single_condition_checked = conditions_mod.RuleCondition(
        "base=main", description="Description"
    )
    single_condition_checked.match = True
    single_condition_checked.evaluation_error = "Error"
    pr_conditions = conditions_mod.PullRequestRuleConditions([single_condition_checked])

    expected_summary = "- [X] `base=main` [Description] ⚠️ Error"
    assert pr_conditions.get_summary() == expected_summary

    expected_summary = ""
    assert pr_conditions.get_unmatched_summary() == expected_summary

    evaluation_result = pr_conditions.get_evaluation_result()
    assert evaluation_result.as_dict() == {
        "match": False,
        "label": "all of",
        "description": None,
        "evaluation_error": None,
        "subconditions": [
            {
                "match": True,
                "label": "`base=main`",
                "description": "Description",
                "evaluation_error": "Error",
                "subconditions": [],
            }
        ],
    }

    assert evaluation_result == conditions_mod.ConditionEvaluationResult.from_dict(
        evaluation_result.as_dict()
    )


async def test_condition_summary_complex() -> None:
    schema = voluptuous.Schema(
        voluptuous.All(
            [voluptuous.Coerce(rules.RuleConditionSchema)],
            voluptuous.Coerce(conditions_mod.PullRequestRuleConditions),
        )
    )
    pr_conditions: conditions_mod.PullRequestRuleConditions = schema(
        [
            "base=main",
            {"or": ["label=foo", "label=bar"]},
            {"and": ["label=foo", "label=baz"]},
        ]
    )
    pr_conditions.condition.conditions[0].match = True
    pr_conditions.condition.conditions[1].description = "GitHub branch protection"
    pr_conditions.condition.conditions[2].conditions[  # type:ignore [union-attr]
        1
    ].match = True

    expected_summary = """\
- [ ] all of:
  - [ ] `label=foo`
  - [X] `label=baz`
- [ ] any of: [GitHub branch protection]
  - [ ] `label=bar`
  - [ ] `label=foo`
- [X] `base=main`"""
    assert pr_conditions.get_summary() == expected_summary

    expected_summary = """\
- [ ] all of:
  - [ ] `label=foo`
- [ ] any of: [GitHub branch protection]
  - [ ] `label=bar`
  - [ ] `label=foo`"""
    assert pr_conditions.get_unmatched_summary() == expected_summary

    evaluation_result = pr_conditions.get_evaluation_result()
    assert evaluation_result.as_dict() == {
        "match": False,
        "label": "all of",
        "description": None,
        "evaluation_error": None,
        "subconditions": [
            {
                "match": False,
                "label": "all of",
                "description": None,
                "evaluation_error": None,
                "subconditions": [
                    {
                        "match": False,
                        "label": "`label=foo`",
                        "description": None,
                        "evaluation_error": None,
                        "subconditions": [],
                    },
                    {
                        "match": True,
                        "label": "`label=baz`",
                        "description": None,
                        "evaluation_error": None,
                        "subconditions": [],
                    },
                ],
            },
            {
                "match": False,
                "label": "any of",
                "description": "GitHub branch protection",
                "evaluation_error": None,
                "subconditions": [
                    {
                        "match": False,
                        "label": "`label=bar`",
                        "description": None,
                        "evaluation_error": None,
                        "subconditions": [],
                    },
                    {
                        "match": False,
                        "label": "`label=foo`",
                        "description": None,
                        "evaluation_error": None,
                        "subconditions": [],
                    },
                ],
            },
            {
                "match": True,
                "label": "`base=main`",
                "description": None,
                "evaluation_error": None,
                "subconditions": [],
            },
        ],
    }

    assert evaluation_result == conditions_mod.ConditionEvaluationResult.from_dict(
        evaluation_result.as_dict()
    )


async def test_rule_condition_negation_summary() -> None:
    rule_condition_negation = rules.RuleConditionSchema(
        {"not": {"or": ["base=main", "label=foo"]}}
    )
    pr_conditions = conditions_mod.PullRequestRuleConditions([rule_condition_negation])
    pr_conditions.condition.conditions[0].match = True

    expected_summary = """\
- [X] not:
  - [ ] any of:
    - [ ] `base=main`
    - [ ] `label=foo`"""
    assert pr_conditions.get_summary() == expected_summary

    assert pr_conditions.get_unmatched_summary() == ""

    evaluation_result = pr_conditions.get_evaluation_result()
    assert evaluation_result.as_dict() == {
        "match": False,
        "label": "all of",
        "description": None,
        "evaluation_error": None,
        "subconditions": [
            {
                "match": True,
                "label": "not",
                "description": None,
                "evaluation_error": None,
                "subconditions": [
                    {
                        "match": False,
                        "label": "any of",
                        "description": None,
                        "evaluation_error": None,
                        "subconditions": [
                            {
                                "match": False,
                                "label": "`base=main`",
                                "description": None,
                                "evaluation_error": None,
                                "subconditions": [],
                            },
                            {
                                "match": False,
                                "label": "`label=foo`",
                                "description": None,
                                "evaluation_error": None,
                                "subconditions": [],
                            },
                        ],
                    }
                ],
            }
        ],
    }

    assert evaluation_result == conditions_mod.ConditionEvaluationResult.from_dict(
        evaluation_result.as_dict()
    )


def create_queue_rule_conditions(
    pull_requests: list[str | conditions_mod.FakeTreeT],
) -> conditions_mod.QueueRuleConditions:
    schema = voluptuous.Schema(
        voluptuous.All(
            [voluptuous.Coerce(rules.RuleConditionSchema)],
            voluptuous.Coerce(conditions_mod.QueueRuleConditions),
        )
    )
    conditions: conditions_mod.QueueRuleConditions = schema(pull_requests)
    return conditions


async def test_queue_rules_summary() -> None:
    conditions = create_queue_rule_conditions(
        [
            "base=main",
            {"or": ["head=feature-1", "head=feature-2", "head=feature-3"]},
            {"or": ["label=urgent", "status-failure!=noway"]},
            {"or": ["label=bar", "check-success=first-ci"]},
            {"or": ["label=foo", "check-success!=first-ci"]},
            {"and": ["label=foo", "check-success=first-ci"]},
            {"and": ["label=foo", "check-success!=first-ci"]},
            {"not": {"and": ["label=fizz", "label=buzz"]}},
            "current-year=2018",
        ]
    )
    conditions.condition.conditions.extend(
        [
            conditions_mod.RuleCondition(
                "#approved-reviews-by>=2",
                description="🛡 GitHub branch protection",
            ),
            conditions_mod.RuleConditionCombination(
                {
                    "or": [
                        conditions_mod.RuleCondition("check-success=my-awesome-ci"),
                        conditions_mod.RuleCondition("check-neutral=my-awesome-ci"),
                        conditions_mod.RuleCondition("check-skipped=my-awesome-ci"),
                    ]
                },
                description="🛡 GitHub branch protection",
            ),
            conditions_mod.RuleCondition(
                "author=me",
                description="Another mechanism to get condtions",
            ),
        ]
    )

    pulls: list[context.BasePullRequest] = [
        conftest.FakePullRequest(
            {
                "number": 1,
                "current-year": date.Year(2018),
                "author": "me",
                "base": "main",
                "head": "feature-1",
                "label": ["foo", "bar"],
                "check-success": ["first-ci", "my-awesome-ci"],
                "check-neutral": None,
                "check-skipped": None,
                "status-failure": ["noway"],
                "approved-reviews-by": ["jd", "sileht"],
            }
        ),
        conftest.FakePullRequest(
            {
                "number": 2,
                "current-year": date.Year(2018),
                "author": "me",
                "base": "main",
                "head": "feature-2",
                "label": ["foo", "urgent"],
                "check-success": ["first-ci", "my-awesome-ci"],
                "check-neutral": None,
                "check-skipped": None,
                "status-failure": ["noway"],
                "approved-reviews-by": ["jd", "sileht"],
            }
        ),
        conftest.FakePullRequest(
            {
                "number": 3,
                "current-year": date.Year(2018),
                "author": "not-me",
                "base": "main",
                "head": "feature-3",
                "label": ["foo", "urgent"],
                "check-success": ["first-ci", "my-awesome-ci"],
                "check-neutral": None,
                "check-skipped": None,
                "status-failure": ["noway"],
                "approved-reviews-by": ["jd", "sileht"],
            }
        ),
    ]
    await conditions(pulls)

    # Create a fake evaluation error
    last_condition = conditions._evaluated_conditions[1].conditions[-1]  # type:ignore
    last_condition.evaluation_error = "Error"  # type:ignore

    expected_summary = """\
- `author=me` [Another mechanism to get condtions]
  - [X] #1 ⚠️ Error
  - [X] #2
  - [ ] #3
- [ ] all of:
  - [ ] `check-success!=first-ci`
  - `label=foo`
    - [X] #1
    - [X] #2
    - [X] #3
- [ ] any of:
  - `label=urgent`
    - [ ] #1
    - [X] #2
    - [X] #3
  - [ ] `status-failure!=noway`
- `#approved-reviews-by>=2` [🛡 GitHub branch protection]
  - [X] #1
  - [X] #2
  - [X] #3
- [X] `base=main`
- [X] `current-year=2018`
- [X] all of:
  - [X] `check-success=first-ci`
  - `label=foo`
    - [X] #1
    - [X] #2
    - [X] #3
- [X] any of:
  - `head=feature-1`
    - [X] #1
    - [ ] #2
    - [ ] #3
  - `head=feature-2`
    - [ ] #1
    - [X] #2
    - [ ] #3
  - `head=feature-3`
    - [ ] #1
    - [ ] #2
    - [X] #3
- [X] any of:
  - [X] `check-success=first-ci`
  - `label=bar`
    - [X] #1
    - [ ] #2
    - [ ] #3
- [X] any of:
  - `label=foo`
    - [X] #1
    - [X] #2
    - [X] #3
  - [ ] `check-success!=first-ci`
- [X] any of [🛡 GitHub branch protection]:
  - [X] `check-success=my-awesome-ci`
  - [ ] `check-neutral=my-awesome-ci`
  - [ ] `check-skipped=my-awesome-ci`
- [X] not:
  - [ ] all of:
    - `label=buzz`
      - [ ] #1
      - [ ] #2
      - [ ] #3
    - `label=fizz`
      - [ ] #1
      - [ ] #2
      - [ ] #3"""

    assert conditions.get_evaluation_result().as_markdown() == expected_summary

    evaluation_result = conditions.get_evaluation_result()
    assert evaluation_result.as_dict() == {
        "match": False,
        "label": "all of",
        "description": None,
        "attribute_name": None,
        "subconditions": anys.AnyContains(
            {
                "match": True,
                "label": "`author=me`",
                "description": "Another mechanism to get condtions",
                "attribute_name": "author",
                "subconditions": [],
                "pull_request_evaluations": [
                    {"pull_request": 1, "match": True, "evaluation_error": "Error"},
                    {"pull_request": 2, "match": True, "evaluation_error": None},
                    {"pull_request": 3, "match": False, "evaluation_error": None},
                ],
            }
        ),
        "pull_request_evaluations": [],
    }

    assert evaluation_result == conditions_mod.QueueConditionEvaluationResult.from_dict(
        evaluation_result.as_dict()
    )


@freeze_time("2021-09-22T08:00:05", tz_offset=0)
async def test_rules_conditions_schedule() -> None:
    pulls: list[context.BasePullRequest] = [
        conftest.FakePullRequest(
            {
                "number": 1,
                "author": "me",
                "base": "main",
                "current-timestamp": date.utcnow(),
                "current-time": date.utcnow(),
                "current-day": date.Day(22),
                "current-month": date.Month(9),
                "current-year": date.Year(2021),
                "current-day-of-week": date.DayOfWeek(3),
            }
        ),
    ]

    conditions = create_queue_rule_conditions(
        [
            "base=main",
            "schedule=MON-FRI 08:00-17:00",
            "schedule=MONDAY-FRIDAY 10:00-12:00",
            "schedule=SAT-SUN 07:00-12:00",
        ]
    )

    await conditions(pulls)

    excpected_summary = """\
- [ ] `schedule=MONDAY-FRIDAY 10:00-12:00`
- [ ] `schedule=SAT-SUN 07:00-12:00`
- [X] `base=main`
- [X] `schedule=MON-FRI 08:00-17:00`"""

    assert conditions.get_summary() == excpected_summary


async def test_render_big_nested_summary() -> None:
    conditions = create_queue_rule_conditions(
        [
            {
                "or": [
                    "base=main",
                    {
                        "or": [
                            "base=main",
                            {
                                "or": [
                                    "base=main",
                                    {
                                        "or": [
                                            "base=main",
                                            {
                                                "or": [
                                                    "base=main",
                                                    {
                                                        "or": [
                                                            "base=main",
                                                            "base=main",
                                                        ]
                                                    },
                                                ]
                                            },
                                        ]
                                    },
                                ]
                            },
                        ]
                    },
                ]
            }
        ]
    )

    summary = conditions.get_summary()
    summary_split = summary.strip().split("\n")
    assert summary_split[-1] == "            - [ ] `base=main`"
