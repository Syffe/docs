import typing
from unittest import mock

import first
import pytest

from mergify_engine import github_types
from mergify_engine import subscription
from mergify_engine.actions import queue
from mergify_engine.rules import conditions
from mergify_engine.tests import utils
from mergify_engine.tests.unit import conftest


@mock.patch.object(
    conditions,
    "get_queue_conditions",
    mock.AsyncMock(return_value=None),
)
@pytest.mark.parametrize(
    "labels,expected_priority",
    (
        (["queue-bar"], 2000),
        (["queue-foo"], 12000),
        (["queue-foo", "high"], 13000),
        (["queue-foo", "medium"], 12000),
        (["queue-foo", "low"], 11000),
        (["queue-foo", "high", "low"], 13000),
        (["queue-foo", "medium", "low"], 12000),
        (["queue-foo", "medium", "high"], 13000),
        (["queue-foo", "low", "medium", "high"], 13000),
        # ensure priority rules is more important that the action priority
        (["queue-bar", "less"], 15),
    ),
)
@pytest.mark.subscription(
    subscription.Features.QUEUE_ACTION,
)
async def test_queue_effective_priority(
    context_getter: conftest.ContextGetterFixture,
    labels: list[str],
    expected_priority: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = await utils.load_mergify_config(
        """queue_rules:
- name: foo
  merge_conditions: []
  priority_rules:
  - name: default
    conditions:
    - label=medium
    priority: medium
  - name: high
    conditions:
    - label=high
    priority: high
  - name: low
    conditions:
    - label=low
    priority: low

- name: bar
  merge_conditions: []
  priority_rules:
  - name: default
    conditions:
    - label=less
    priority: 15

pull_request_rules:
- name: bar
  conditions:
    - label=queue-bar
  actions:
    queue:
      name: bar
- name: foo
  conditions:
    - label=queue-foo
  actions:
    queue:
      name: foo
""",
    )

    ctxt = await context_getter(1, labels=[{"name": label} for label in labels])
    ctxt.repository._caches.mergify_config.set(config)
    ctxt.repository._caches.branch_protections.set(
        github_types.GitHubRefType("main"),
        None,
    )
    evaluator = await config["pull_request_rules"].get_pull_request_rules_evaluator(
        ctxt,
    )
    rule = first.first(r for r in evaluator.matching_rules if r.conditions.match)
    assert rule is not None
    queue_action = typing.cast(queue.QueueAction, rule.actions["queue"])
    await queue_action.load_context(ctxt, rule)
    executor = await queue_action.executor_class.create(queue_action, ctxt, rule)
    await executor._set_action_queue_rule()
    priority = await executor.queue_rule.get_context_effective_priority(ctxt)
    assert priority == expected_priority
