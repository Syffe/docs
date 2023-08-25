import typing

import pytest
import voluptuous

from mergify_engine import context
from mergify_engine import rules
from mergify_engine.rules.config import queue_rules


def test_has_only_one_of_min_value() -> None:
    with pytest.raises(
        ValueError, match=r"^Need at least 2 keys to check for exclusivity$"
    ):
        queue_rules._has_only_one_of("foo")


def test_has_only_one() -> None:
    queue_rules._has_only_one_of("foo", "bar")({"foo": 1, "baz": 2})


def test_has_only_one_invalid() -> None:
    with pytest.raises(voluptuous.Invalid, match=r"^Must contain only one of foo,bar$"):
        queue_rules._has_only_one_of("foo", "bar")({"foo": 1, "bar": 2})


async def test_evaluate_on_queue_pull_request() -> None:
    class MockedContext:
        @property
        def pull(self) -> dict[str, typing.Any]:
            return {"number": 123}

        @property
        async def checks(self) -> dict[str, str]:
            return {"ci/fake-ci-queue": "success"}

    class MockedQueueContext:
        @property
        async def checks(self) -> dict[str, str]:
            return {}

    pr = context.QueuePullRequest(MockedContext(), MockedQueueContext())  # type: ignore[arg-type]

    qrules = voluptuous.Schema(queue_rules.QueueRulesSchema)(
        rules.YamlSchema(
            """
queue_rules:
    - name: default
      merge_conditions:
        - status-success=ci/fake-ci-merge
      queue_conditions:
        - status-success=ci/fake-ci-queue
"""
        )["queue_rules"]
    )

    eval_result = await qrules["default"].evaluate([pr])
    assert 123 in eval_result.queue_conditions._evaluated_conditions
    result_conditions = eval_result.queue_conditions._evaluated_conditions[123]
    assert result_conditions._conditions[0].label == "status-success=ci/fake-ci-queue"
    assert result_conditions._conditions[0].match
    assert result_conditions.match
