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
""",
            False,
        ),
        (
            """
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
        (
            """
pull_request_rules:
  - name: only-checks
    conditions:
      - check-success=test
    actions: {}
""",
            False,
        ),
        (
            r"""
pull_request_rules:
  - name: customer case MRGY-2780
    conditions:
      - 'label!=multiple-reviewers'
      - '#approved-reviews-by>=1'
      - 'label=match'
      - 'label!=blocked'
      - 'check-success=test'
      - 'check-failure!=percy/Rider-Web'
      - 'check-failure!=percy/Admin'
      - -draft
      - or:
          # Order here matter to reproduce the bug
          - body-raw~=(https:\/\/yo\.atlassian\.net\/).*([a-zA-Z]{2,}(-|\s)[0-9]+)
          - body-raw~=(https:\/\/yo\.pagerduty\.com\/incidents\/)
          - body-raw~=(https:\/\/yo\.slack\.com)
          - author=dependabot[bot]
      - or:
        - and:
          - "#approved-reviews-by>=1"
          - "#changes-requested-reviews-by=0"
          - branch-protection-review-decision=APPROVED
          - or:
            - check-success=Summary
            - check-neutral=Summary
            - check-skipped=Summary
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
    ctxt.repository._caches.branch_protections[github_types.GitHubRefType("main")] = {}
    ctxt._caches.pull_check_runs.set(
        [
            {
                "id": 123456,
                "app_id": 1234,
                "app_name": "Mergify",
                "output": {
                    "title": "",
                    "summary": "",
                    "text": "",
                    "annotations": [],
                    "annotations_count": 0,
                    "annotations_url": "",
                },
                "head_sha": github_types.SHAType("sha"),
                "app_slug": "github-actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/in/15368?s=40&v=4",
                "name": "Summary",
                "conclusion": "success",
                "completed_at": github_types.ISODateTimeType("2021-06-02T10:00:00Z"),
                "html_url": "",
                "external_id": "123456",
                "status": "completed",
            },
            {
                "id": 123456,
                "app_id": 1234,
                "app_name": "github-actions",
                "output": {
                    "title": "",
                    "summary": "",
                    "text": "",
                    "annotations": [],
                    "annotations_count": 0,
                    "annotations_url": "",
                },
                "head_sha": github_types.SHAType("sha"),
                "app_slug": "github-actions",
                "app_avatar_url": "https://avatars.githubusercontent.com/in/15368?s=40&v=4",
                "name": "test",
                "conclusion": None,
                "completed_at": github_types.ISODateTimeType("2021-06-02T10:00:00Z"),
                "html_url": "",
                "external_id": "123456",
                "status": "in_progress",
            },
        ]
    )
    ctxt._caches.pull_statuses.set([])
    ctxt._caches.consolidated_reviews.set(
        ([], [{"state": "APPROVED", "user": {"login": "foobar"}}])  # type: ignore[typeddict-item]
    )
    ctxt._caches.review_threads.set([])
    ctxt._caches.review_decision.set("APPROVED")
    ctxt.pull["body"] = "This is the body\nhttps://yo.atlassian.net/browse/LOCO-1202\n"
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
