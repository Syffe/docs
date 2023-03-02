from collections import abc
import dataclasses
import typing

import voluptuous

from mergify_engine import context
from mergify_engine import queue
from mergify_engine import rules
from mergify_engine.rules import conditions as conditions_mod


PriorityRuleName = typing.NewType("PriorityRuleName", str)
EvaluatedPriorityRule = typing.NewType("EvaluatedPriorityRule", "PriorityRule")
PriorityRulesEvaluator = rules.GenericRulesEvaluator[
    "PriorityRule", EvaluatedPriorityRule
]


@dataclasses.dataclass
class PriorityRule:
    name: PriorityRuleName
    conditions: conditions_mod.PriorityRuleConditions
    priority: queue.PriorityT

    class T_from_dict(typing.TypedDict):
        name: PriorityRuleName
        conditions: conditions_mod.PriorityRuleConditions
        priority: queue.PriorityT

    @classmethod
    def from_dict(cls, d: T_from_dict) -> "PriorityRule":
        return cls(**d)

    async def evaluate(
        self, pulls: list[context.BasePullRequest]
    ) -> EvaluatedPriorityRule:
        evaluated_rule = typing.cast(EvaluatedPriorityRule, self)
        await evaluated_rule.conditions(pulls)
        return evaluated_rule

    def copy(self) -> "PriorityRule":
        return self.__class__(
            name=self.name,
            conditions=conditions_mod.PriorityRuleConditions(
                self.conditions.condition.copy().conditions
            ),
            priority=self.priority,
        )


@dataclasses.dataclass
class PriorityRules:
    rules: list[PriorityRule]

    def __post_init__(self) -> None:
        names: set[PriorityRuleName] = set()
        for rule in self.rules:
            if rule.name in names:
                raise voluptuous.error.Invalid(
                    f"priority_rules names must be unique, found `{rule.name}` twice"
                )
            names.add(rule.name)

    def __iter__(self) -> abc.Iterator[PriorityRule]:
        return iter(self.rules)

    async def get_context_priority(self, ctxt: context.Context) -> int | None:
        if not self.rules:
            return None

        priority_rules = [rule.copy() for rule in self.rules]
        priority_rules_evaluator = await PriorityRulesEvaluator.create(
            priority_rules,
            ctxt.repository,
            [ctxt.pull_request],
            False,
        )
        matching_priority_rules = [
            rule
            for rule in priority_rules_evaluator.matching_rules
            if rule.conditions.match
        ]

        final_priority = None
        for rule in matching_priority_rules:
            rule_priority = queue.Priority(rule.priority)
            # https://github.com/python/mypy/issues/13973
            if final_priority is None or rule_priority > final_priority:  # type: ignore[unreachable]
                final_priority = rule_priority

        return final_priority
