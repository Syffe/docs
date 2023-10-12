import pytest

from mergify_engine import condition_value_querier
from mergify_engine import github_types
from mergify_engine import rules as rules_mod
from mergify_engine.rules.config import mergify as mergify_conf
from mergify_engine.rules.config import pull_request_rules
from mergify_engine.tests.unit import conftest


@pytest.mark.parametrize(
    "config, ignored",
    (
        (
            """
pull_request_rules:
  - name: "head do not match and label do not match"
    conditions:
    - head=do-not-match
    - label=do-not-match
    actions: {}
""",
            True,
        ),
        (
            """
pull_request_rules:
  - name: "head do not match and label match"
    conditions:
    - head=do-not-match
    - label=match
    actions: {}
""",
            True,
        ),
        (
            """
pull_request_rules:
  - name: "head match and label match"
    conditions:
    - head=match
    - label=match
    actions: {}
""",
            False,
        ),
        (
            """
pull_request_rules:
  - name: "head match and label do not match"
    conditions:
    - head=match
    - label=do-not-match
    actions: {}
""",
            False,
        ),
        (
            """
pull_request_rules:
  - name: "no conditions"
    conditions: []
    actions: {}
""",
            False,
        ),
        (
            """
pull_request_rules:
  - name: not with combination that match
    conditions:
    - not:
        or:
          - label=do-not-match
    actions: {}
""",
            False,
        ),
        (
            """
pull_request_rules:
  - name: not with combination that match
    conditions:
    - not:
        or:
          - label=do-not-match
          - label=match
    actions: {}
""",
            False,
        ),
        (
            """
pull_request_rules:
  - name: not with combination that do not match
    conditions:
    - not:
        and:
          - label=do-not-match
          - label=match
    actions: {}
""",
            False,
        ),
        (
            """
pull_request_rules:
  - name: not with head match and label match
    conditions:
    - not:
        and:
          - head=match
          - label=match
    actions: {}
""",
            True,
        ),
        (
            """
pull_request_rules:
  - name: not with head do not match and label match
    conditions:
    - not:
        and:
          - head=do-not-match
          - label=match
    actions: {}
""",
            False,
        ),
        (
            """
pull_request_rules:
  - name: not with head do not match and label do not match
    conditions:
    - not:
        and:
          - head=do-not-match
          - label=do-not-match
    actions: {}
pull_request_rules:
  - name: no head
    conditions:
      - or:
        - label=do-not-match
        - label=other-do-no-match
    actions: {}
""",
            False,
        ),
    ),
)
async def test_pull_request_rules_evaluator(
    config: str,
    ignored: bool,
    context_getter: conftest.ContextGetterFixture,
) -> None:
    ctxt = await context_getter(1)
    ctxt.pull["head"]["ref"] = github_types.GitHubRefType("match")
    ctxt.pull["labels"] = [
        {
            "id": 4876718741,
            "name": "match",
            "color": "000000",
            "default": False,
        },
    ]

    parsed_config = await mergify_conf.get_mergify_config_from_dict(
        ctxt.repository, rules_mod.YamlSchema(config), "", False
    )

    evaluated_rules = await pull_request_rules.PullRequestRulesEvaluator.create(
        parsed_config["pull_request_rules"].rules,
        ctxt.repository,
        [condition_value_querier.PullRequest(ctxt)],
        True,
    )
    if ignored:
        assert len(evaluated_rules.ignored_rules) == 1
        assert len(evaluated_rules.matching_rules) == 0
    else:
        assert len(evaluated_rules.ignored_rules) == 0
        assert len(evaluated_rules.matching_rules) == 1
