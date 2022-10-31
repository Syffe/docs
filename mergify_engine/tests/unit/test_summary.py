import voluptuous

from mergify_engine import rules
from mergify_engine.rules import conditions
from mergify_engine.tests.unit import conftest


SCHEMA = voluptuous.Schema(
    voluptuous.All(
        [voluptuous.Coerce(rules.RuleConditionSchema)],
        voluptuous.Coerce(conditions.QueueRuleConditions),
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
        c: conditions.QueueRuleConditions = SCHEMA(
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
        c: conditions.QueueRuleConditions = SCHEMA(
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
  - [X] #2
"""
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
        c: conditions.QueueRuleConditions = SCHEMA(
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
    - [ ] #2
"""
        in await gen_summary()
    )
