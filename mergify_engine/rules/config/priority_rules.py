from __future__ import annotations

import dataclasses
import typing

import voluptuous

from mergify_engine import condition_value_querier
from mergify_engine import queue
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules import generic_evaluator


if typing.TYPE_CHECKING:
    from collections import abc

    from mergify_engine import context

PriorityRuleName = typing.NewType("PriorityRuleName", str)
EvaluatedPriorityRule = typing.NewType("EvaluatedPriorityRule", "PriorityRule")
PriorityRulesEvaluator = generic_evaluator.GenericRulesEvaluator[
    "PriorityRule",
    EvaluatedPriorityRule,
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
    def from_dict(cls, d: T_from_dict) -> PriorityRule:
        return cls(**d)

    def get_conditions_used_by_evaluator(self) -> conditions_mod.PriorityRuleConditions:
        return self.conditions

    async def evaluate(
        self,
        pulls: list[condition_value_querier.BasePullRequest],
    ) -> EvaluatedPriorityRule:
        evaluated_rule = typing.cast(EvaluatedPriorityRule, self)
        await evaluated_rule.conditions(pulls)
        return evaluated_rule

    def copy(self) -> PriorityRule:
        return self.__class__(
            name=self.name,
            conditions=conditions_mod.PriorityRuleConditions(
                self.conditions.condition.copy().conditions,
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
                    f"priority_rules names must be unique, found `{rule.name}` twice",
                )
            names.add(rule.name)

    def __iter__(self) -> abc.Iterator[PriorityRule]:
        return iter(self.rules)

    async def get_context_priority(self, ctxt: context.Context) -> int:
        if not self.rules:
            return queue.PriorityAliases.medium.value

        priority_rules = [rule.copy() for rule in self.rules]
        priority_rules_evaluator = await PriorityRulesEvaluator.create(
            priority_rules,
            ctxt.repository,
            [condition_value_querier.PullRequest(ctxt)],
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

        if final_priority is None:
            return queue.PriorityAliases.medium.value
        return final_priority
