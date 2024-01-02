import datetime
import typing
from unittest import mock

import voluptuous

from mergify_engine import condition_value_querier
from mergify_engine.queue.merge_train import TrainCarOutcome
from mergify_engine.rules import checks_status
from mergify_engine.rules import conditions
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers
from mergify_engine.rules.config import conditions as cond_config
from mergify_engine.tests.unit import conftest


FAKE_REPO = mock.Mock(repo={"owner": {"login": "org"}})

empty: list[str] = []


async def test_rules_conditions_update() -> None:
    pulls = [
        conftest.FakePullRequest(
            {
                "number": 1,
                "author": "me",
                "base": "main",
                "head": "feature-1",
                "label": ["foo", "bar"],
                "check-success": ["tests"],
                "check-pending": [],
                "check-failure": ["jenkins/fake-tests"],
                "check-skipped": [],
                "check-stale": [],
            },
        ),
    ]
    pulls[0].sync_checks()
    schema = voluptuous.Schema(
        voluptuous.All(
            [voluptuous.Coerce(cond_config.RuleConditionSchema)],
            voluptuous.Coerce(conditions.QueueRuleMergeConditions),
        ),
    )

    c = schema(
        [
            "label=foo",
            "check-success=tests",
            "check-success=jenkins/fake-tests",
        ],
    )

    await c(pulls)

    assert (
        c.get_summary()
        == """- [ ] `check-success=jenkins/fake-tests`
- [X] `check-success=tests`
- `label=foo`
  - [X] #1"""
    )

    outcome = await checks_status.get_outcome_from_conditions(
        mock.Mock(),
        FAKE_REPO,
        typing.cast(list[condition_value_querier.BasePullRequest], pulls),
        c,
    )
    assert outcome == TrainCarOutcome.CHECKS_FAILED


async def assert_queue_rule_checks_status(
    conds: list[typing.Any],
    pull: conftest.FakePullRequest,
    expected_outcome: TrainCarOutcome,
) -> None:
    pull.sync_checks()
    schema = voluptuous.Schema(
        voluptuous.All(
            [voluptuous.Coerce(cond_config.RuleConditionSchema)],
            voluptuous.Coerce(conditions.QueueRuleMergeConditions),
        ),
    )

    c = schema(conds)

    await c([pull])
    outcome = await checks_status.get_outcome_from_conditions(
        mock.Mock(),
        FAKE_REPO,
        [typing.cast(condition_value_querier.BasePullRequest, pull)],
        c,
    )
    assert outcome == expected_outcome


async def test_rules_checks_basic() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            # Thursday
            "current-datetime": datetime.datetime(2022, 11, 24, tzinfo=datetime.UTC),
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "label": empty,
            "check-success": empty,
            "check-neutral": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = ["check-success=fake-ci", "label=foobar", "schedule=MON-FRI"]

    # Label missing and nothing reported
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.CONDITIONS_FAILED,
    )

    # Label missing and success
    pull.attrs["check-success"] = ["fake-ci"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.CONDITIONS_FAILED,
    )

    # label ok and nothing reported
    pull.attrs["label"] = ["foobar"]
    pull.attrs["check-success"] = empty
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["fake-ci"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = ["whatever"]
    pull.attrs["check-failure"] = ["foo", "fake-ci"]
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported
    pull.attrs["check-pending"] = ["whatever"]
    pull.attrs["check-failure"] = ["foo"]
    pull.attrs["check-success"] = ["fake-ci", "test-starter"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # Pending reported and schedule missing
    # Saturday
    pull.attrs["current-datetime"] = datetime.datetime(
        2022,
        11,
        26,
        tzinfo=datetime.UTC,
    )
    pull.attrs["check-pending"] = ["fake-ci"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported and schedule missing
    # Saturday
    pull.attrs["current-datetime"] = datetime.datetime(
        2022,
        11,
        26,
        tzinfo=datetime.UTC,
    )
    pull.attrs["check-pending"] = ["whatever"]
    pull.attrs["check-failure"] = ["foo", "fake-ci"]
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported and schedule missing
    # Saturday
    pull.attrs["current-datetime"] = datetime.datetime(
        2022,
        11,
        26,
        tzinfo=datetime.UTC,
    )
    pull.attrs["check-pending"] = ["whatever"]
    pull.attrs["check-failure"] = ["foo"]
    pull.attrs["check-success"] = ["fake-ci", "test-starter"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_SCHEDULE,
    )


async def test_rules_checks_with_and_or() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "label": [],
            "check-success": [],
            "check-failure": [],
            "check-pending": [],
            "check-neutral": [],
            "check-skipped": [],
            "check-stale": [],
        },
    )
    conds = [
        {"or": ["check-success=fake-ci", "label=skip-tests"]},
        "check-success=other-ci",
    ]

    # Label missing and nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Label missing and half success
    pull.attrs["check-success"] = ["fake-ci"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Label missing and half success and half pending
    pull.attrs["check-success"] = ["fake-ci"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Label missing and all success
    pull.attrs["check-success"] = ["fake-ci", "other-ci"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # Label missing and half failure
    pull.attrs["check-success"] = ["fake-ci"]
    pull.attrs["check-failure"] = ["other-ci"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Label missing and half failure bus
    pull.attrs["check-success"] = ["other-ci"]
    pull.attrs["check-failure"] = ["fake-ci"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # label ok and nothing reported
    pull.attrs["label"] = ["skip-tests"]
    pull.attrs["check-success"] = empty
    pull.attrs["check-failure"] = empty
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # label ok and failure
    pull.attrs["label"] = ["skip-tests"]
    pull.attrs["check-success"] = empty
    pull.attrs["check-failure"] = ["other-ci"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # label ok and failure
    pull.attrs["label"] = ["skip-tests"]
    pull.attrs["check-success"] = empty
    pull.attrs["check-failure"] = ["fake-ci"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # label ok and success
    pull.attrs["label"] = ["skip-tests"]
    pull.attrs["check-pending"] = ["fake-ci"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["other-ci"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )


async def test_rules_checks_status_with_negative_conditions1() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-neutral": empty,
            "check-timed-out": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = [
        "check-success=test-starter",
        "check-pending!=foo",
        "check-failure!=foo",
        "check-timed-out!=foo",
    ]

    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["foo"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-timed-out"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["foo"]
    pull.attrs["check-timed-out"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Timeout reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-timed-out"] = ["foo"]
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-timed-out"] = empty
    pull.attrs["check-success"] = ["test-starter", "foo"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # half reported, sorry..., UNDEFINED BEHAVIOR
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )


async def test_rules_checks_status_with_negative_conditions2() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-neutral": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = [
        "check-success=test-starter",
        "-check-pending=foo",
        "-check-failure=foo",
    ]

    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["foo"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["foo"]
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter", "foo"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # half reported, sorry..., UNDEFINED BEHAVIOR
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )


async def test_rules_checks_status_with_negative_conditions3() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-neutral": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = [
        "check-success=test-starter",
        "#check-pending=0",
        "#check-failure=0",
    ]

    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["foo"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["foo"]
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter", "foo"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # half reported, sorry..., UNDEFINED BEHAVIOR
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["test-starter"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )


async def test_rules_checks_status_with_or_conditions() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-neutral": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = [
        {
            "or": ["check-success=ci-1", "check-success=ci-2"],
        },
    ]

    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["ci-1"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["ci-2"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # Pending reported
    pull.attrs["check-pending"] = ["ci-1"]
    pull.attrs["check-failure"] = ["ci-2"]
    pull.attrs["check-success"] = empty
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["ci-1"]
    pull.attrs["check-success"] = ["ci-2"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # Success reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["ci-1", "ci-2"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # half reported success
    pull.attrs["check-failure"] = empty
    pull.attrs["check-pending"] = empty
    pull.attrs["check-success"] = ["ci-1"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # half reported failure, wait for ci-2 to finish
    pull.attrs["check-failure"] = ["ci-1"]
    pull.attrs["check-pending"] = empty
    pull.attrs["check-success"] = empty
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)


async def test_rules_checks_status_expected_failure() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-neutral": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = ["check-failure=ci-1"]

    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["ci-1"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = empty
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["ci-1"]
    pull.attrs["check-success"] = empty
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # Success reported, no way!
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["ci-1"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)


async def test_rules_checks_status_regular() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-neutral": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = ["check-success=ci-1", "check-success=ci-2"]

    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["ci-1"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["ci-2"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["ci-1"]
    pull.attrs["check-success"] = ["ci-2"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["ci-1", "ci-2"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # half reported success
    pull.attrs["check-failure"] = empty
    pull.attrs["check-pending"] = empty
    pull.attrs["check-success"] = ["ci-1"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # half reported failure, fail early
    pull.attrs["check-failure"] = ["ci-1"]
    pull.attrs["check-pending"] = empty
    pull.attrs["check-success"] = empty
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)


async def test_rules_checks_status_regex() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": empty,
            "check-failure": empty,
            "check-pending": empty,
            "check-neutral": empty,
            "check-skipped": empty,
            "check-stale": empty,
        },
    )
    conds = ["check-success~=^ci-1$", "check-success~=^ci-2$"]

    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["ci-1"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["ci-2"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["ci-1"]
    pull.attrs["check-success"] = ["ci-2"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["ci-1", "ci-2"]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )

    # half reported success
    pull.attrs["check-failure"] = empty
    pull.attrs["check-pending"] = empty
    pull.attrs["check-success"] = ["ci-1"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # half reported failure, fail early
    pull.attrs["check-failure"] = ["ci-1"]
    pull.attrs["check-pending"] = empty
    pull.attrs["check-success"] = empty
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)


async def test_rules_checks_status_depop() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "main",
            "head": "feature-1",
            "check-success": [],
            "check-failure": [],
            "check-pending": [],
            "check-neutral": [],
            "check-skipped": [],
            "check-stale": [],
            "approved-reviews-by": ["me"],
            "changes-requested-reviews-by": [],
            "label": ["mergeit"],
        },
    )
    conds = [
        "check-success=Summary",
        "check-success=c-ci/status",
        "check-success=c-ci/s-c-t",
        "check-success=c-ci/c-p-validate",
        "#approved-reviews-by>=1",
        "approved-reviews-by=me",
        "-label=flag:wait",
        {
            "or": [
                "check-success=c-ci/status",
                "check-neutral=c-ci/status",
                "check-skipped=c-ci/status",
            ],
        },
        {
            "or": [
                "check-success=c-ci/s-c-t",
                "check-neutral=c-ci/s-c-t",
                "check-skipped=c-ci/s-c-t",
            ],
        },
        {
            "or": [
                "check-success=c-ci/c-p-validate",
                "check-neutral=c-ci/c-p-validate",
                "check-skipped=c-ci/c-p-validate",
            ],
        },
        "#approved-reviews-by>=1",
        "#changes-requested-reviews-by=0",
    ]
    # Nothing reported
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = ["Summary", "continuous-integration/jenkins/pr-head"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = ["c-ci/status"]
    pull.attrs["check-failure"] = empty
    pull.attrs["check-success"] = [
        "Summary",
        "continuous-integration/jenkins/pr-head",
        "c-ci/s-c-t",
    ]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Pending reported
    pull.attrs["check-pending"] = [
        "c-ci/status",
        "c-ci/s-c-t",
        "c-ci/c-p-validate",
    ]
    pull.attrs["check-failure"] = ["c-ci/g-validate"]
    pull.attrs["check-success"] = ["Summary"]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.WAITING_FOR_CI)

    # Failure reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = [
        "c-ci/s-c-t",
        "c-ci/g-validate",
    ]
    pull.attrs["check-success"] = [
        "Summary",
        "c-ci/status",
        "c-ci/c-p-validate",
    ]
    await assert_queue_rule_checks_status(conds, pull, TrainCarOutcome.CHECKS_FAILED)

    # Success reported
    pull.attrs["check-pending"] = empty
    pull.attrs["check-failure"] = ["c-ci/g-validate"]
    pull.attrs["check-success"] = [
        "Summary",
        "c-ci/status",
        "c-ci/s-c-t",
        "c-ci/c-p-validate",
    ]
    await assert_queue_rule_checks_status(
        conds,
        pull,
        TrainCarOutcome.WAITING_FOR_MERGE,
    )


async def test_rules_checks_status_ceph() -> None:
    pull = conftest.FakePullRequest(
        {
            "number": 1,
            "author": "me",
            "base": "devel",
            "head": "feature-1",
            "check-failure": [],
            "check-neutral": [],
            "check-skipped": [],
            "check-stale": [],
            "approved-reviews-by": ["me", "other"],
            "changes-requested-reviews-by": [],
            "label": ["mergeit"],
            "check-success": ["Summary", "DCO", "build"],
            "check-success-or-neutral": ["Summary", "DCO", "build"],
            "check-success-or-neutral-or-pending": [
                "Summary",
                "DCO",
                "golangci-lint",
                "commitlint",
                "build",
                "codespell",
                "multi-arch-build",
                "go-test",
                "lint-extras",
                "mod-check",
                "go-test-api",
                "Rule: automatic merge (merge)",
                "ci/centos/mini-e2e/k8s-1.20",
                "ci/centos/upgrade-tests-rbd",
                "ci/centos/mini-e2e-helm/k8s-1.22",
                "ci/centos/upgrade-tests-cephfs",
                "ci/centos/mini-e2e/k8s-1.22",
                "ci/centos/k8s-e2e-external-storage/1.22",
                "ci/centos/mini-e2e-helm/k8s-1.21",
                "ci/centos/k8s-e2e-external-storage/1.21",
                "ci/centos/mini-e2e/k8s-1.21",
                "ci/centos/mini-e2e-helm/k8s-1.20",
            ],
            "status-success": ["Summary", "DCO", "build"],
            "check-pending": [
                "golangci-lint",
                "commitlint",
                "codespell",
                "multi-arch-build",
                "go-test",
                "lint-extras",
                "mod-check",
                "go-test-api",
                "Rule: automatic merge (merge)",
                "ci/centos/mini-e2e/k8s-1.20",
                "ci/centos/upgrade-tests-rbd",
                "ci/centos/mini-e2e-helm/k8s-1.22",
                "ci/centos/upgrade-tests-cephfs",
                "ci/centos/mini-e2e/k8s-1.22",
                "ci/centos/k8s-e2e-external-storage/1.22",
                "ci/centos/mini-e2e-helm/k8s-1.21",
                "ci/centos/k8s-e2e-external-storage/1.21",
                "ci/centos/mini-e2e/k8s-1.21",
                "ci/centos/mini-e2e-helm/k8s-1.20",
            ],
        },
    )
    pull.attrs["check"] = (
        pull.attrs.get("check-success", [])  # type: ignore[operator,assignment]
        + pull.attrs.get("check-neutral", [])
        + pull.attrs.get("check-pending", [])
        + pull.attrs.get("check-stale", [])
        + pull.attrs.get("check-failure", [])
        + pull.attrs.get("check-skipped", [])
    )

    tree = {
        "and": [
            {"!=": ["label", "DNM"]},
            {"~=": ["base", "^(devel)|(release-.+)$"]},
            {">=": ["#approved-reviews-by", 2]},
            {"=": ["approved-reviews-by", "@ceph/ceph-csi-maintainers"]},
            {"=": ["#changes-requested-reviews-by", 0]},
            {"=": ["status-success", "codespell"]},
            {"=": ["status-success", "multi-arch-build"]},
            {"=": ["status-success", "go-test"]},
            {"=": ["status-success", "golangci-lint"]},
            {"=": ["status-success", "commitlint"]},
            {"=": ["status-success", "mod-check"]},
            {"=": ["status-success", "lint-extras"]},
            {"=": ["status-success", "ci/centos/k8s-e2e-external-storage/1.21"]},
            {"=": ["status-success", "ci/centos/k8s-e2e-external-storage/1.22"]},
            {"=": ["status-success", "ci/centos/mini-e2e-helm/k8s-1.20"]},
            {"=": ["status-success", "ci/centos/mini-e2e-helm/k8s-1.21"]},
            {"=": ["status-success", "ci/centos/mini-e2e-helm/k8s-1.22"]},
            {"=": ["status-success", "ci/centos/mini-e2e/k8s-1.20"]},
            {"=": ["status-success", "ci/centos/mini-e2e/k8s-1.21"]},
            {"=": ["status-success", "ci/centos/mini-e2e/k8s-1.22"]},
            {"=": ["status-success", "ci/centos/upgrade-tests-cephfs"]},
            {"=": ["status-success", "ci/centos/upgrade-tests-rbd"]},
            {"=": ["status-success", "DCO"]},
            {
                "not": {
                    "or": [
                        {"=": ["label", "wip"]},
                        {"=": ["label", "in-progress"]},
                    ],
                },
            },
        ],
    }
    f = filter.IncompleteChecksFilter(
        typing.cast(filter.TreeT, tree),
        pending_checks=pull.attrs["check-pending"],  # type: ignore[arg-type]
        all_checks=pull.attrs["check"],  # type: ignore[arg-type]
    )

    async def fake_get_team_members(*_args: typing.Any) -> list[str]:
        return ["me", "other", "foo", "bar"]

    repo_with_team = mock.Mock(
        repo={"owner": {"login": "ceph"}},
        installation=mock.Mock(
            get_team_members=mock.Mock(side_effect=fake_get_team_members),
        ),
    )

    live_resolvers.configure_filter(repo_with_team, f)
    assert isinstance(await f(pull), filter.UnknownType)
