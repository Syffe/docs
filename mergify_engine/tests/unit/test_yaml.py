import pytest
import yaml as base_yaml

from mergify_engine import rules
from mergify_engine.yaml import yaml


@pytest.mark.parametrize(
    ("config", "expected_anchors"),
    (
        (
            """
shared:
  conditions: &cond1
    - status-success=continuous-integration/fake-ci
  conditions: &cond2
    - base=main

queue_rules:
  - name: default
    conditions: *cond1
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions: *cond2
    actions:
      queue:
        name: default
""",
            ["cond1", "cond2"],
        ),
        (
            """
shared:
  conditions: &toto
    - status-success=continuous-integration/fake-ci

queue_rules:
  - name: default
    conditions: *toto
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions: *toto
    actions:
      queue:
        name: default
""",
            ["toto"],
        ),
        (
            """
shared:
  conditions: &toto
    - status-success=continuous-integration/fake-ci

queue_rules:
  - name: default
    conditions:
      - base=main
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions:
      - base=main
    actions:
      queue:
        name: default
""",
            ["toto"],
        ),
        (
            """
queue_rules:
  - name: default
    conditions:
      - base=main
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions:
      - base=main
    actions:
      queue:
        name: default
""",
            [],
        ),
    ),
)
async def test_yaml_anchor_extractor_load(
    config: str,
    expected_anchors: list[str],
) -> None:
    result = yaml.anchor_extractor_load(config)
    assert result is not None
    anchors = result[1]
    assert len(anchors) == len(expected_anchors)
    for anchor in expected_anchors:
        assert anchor in anchors


@pytest.mark.parametrize(
    ("config", "expected_exception", "expected_error_value"),
    (
        (
            """
shared:
  conditions: &toto
    - status-success=continuous-integration/fake-ci

queue_rules:
  - name: default
    conditions: *tata
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions: *tata
    actions:
      queue:
        name: default
""",
            base_yaml.composer.ComposerError,
            "found undefined alias 'tata'",
        ),
        (
            """
shared:
  conditions: &toto
    - status-success=continuous-integration/fake-ci

queue_rules
  - name: default
    conditions: *toto
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions: *toto
    actions:
      queue:
        name: default
""",
            base_yaml.scanner.ScannerError,
            "could not find expected ':'",
        ),
        (
            """
shared:
  conditions: &toto
    - status-success=continuous-integration/fake-ci

queue_rules:
 name: default
    conditions: *toto
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions: *toto
    actions:
      queue:
        name: default
""",
            base_yaml.scanner.ScannerError,
            "mapping values are not allowed here",
        ),
        (
            """
queue_rules:
  - name: default
    conditions: *toto
    speculative_checks: 5
    allow_inplace_checks: true

pull_request_rules:
  - name: Merge me
    conditions: *toto
    actions:
      queue:
        name: default
""",
            base_yaml.composer.ComposerError,
            "found undefined alias 'toto'",
        ),
    ),
)
async def test_yaml_anchor_extractor_load_error(
    config: str,
    expected_exception: type[Exception],
    expected_error_value: str,
) -> None:
    with pytest.raises(expected_exception, match=expected_error_value):
        yaml.anchor_extractor_load(config)


async def test_yaml_with_anchor_extractor_schema() -> None:
    config = """
shared:
  merge_ci: &common_checks
    - "check-success=ci/circleci: check_if_tests_done"
    - "#approved-reviews-by>=1"

queue_rules:
  - name: parent_queue
    merge_conditions:
      - label=parent
      - and: *common_checks
    checks_timeout: 25m

pull_request_rules:
  - name: parent
    conditions:
      - base=toto
      - label=to-be-merged
      - and: *common_checks
    actions:
      queue:
        name: parent_queue
        method: squash
"""

    parsed_config = rules.YamlAnchorsExtractorSchema(config)
    assert parsed_config is not None
    assert parsed_config[0] == {
        "shared": {
            "merge_ci": [
                "check-success=ci/circleci: check_if_tests_done",
                "#approved-reviews-by>=1",
            ],
        },
        "queue_rules": [
            {
                "name": "parent_queue",
                "merge_conditions": [
                    "label=parent",
                    {
                        "and": [
                            "check-success=ci/circleci: check_if_tests_done",
                            "#approved-reviews-by>=1",
                        ],
                    },
                ],
                "checks_timeout": "25m",
            },
        ],
        "pull_request_rules": [
            {
                "name": "parent",
                "conditions": [
                    "base=toto",
                    "label=to-be-merged",
                    {
                        "and": [
                            "check-success=ci/circleci: check_if_tests_done",
                            "#approved-reviews-by>=1",
                        ],
                    },
                ],
                "actions": {"queue": {"name": "parent_queue", "method": "squash"}},
            },
        ],
    }

    anchors = parsed_config[1]
    assert len(anchors) == 1
    assert "common_checks" in anchors

    nodes = anchors["common_checks"].value
    assert len(nodes) == 2
    assert nodes[0].value == "check-success=ci/circleci: check_if_tests_done"
    assert nodes[1].value == "#approved-reviews-by>=1"
