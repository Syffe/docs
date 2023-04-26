from collections import abc
import dataclasses
import typing

import voluptuous

from mergify_engine import context
from mergify_engine import rules
from mergify_engine.rules import conditions as conditions_mod
from mergify_engine.rules.config import conditions as cond_config


PartitionRuleName = typing.NewType("PartitionRuleName", str)
DEFAULT_PARTITION_NAME = PartitionRuleName("__default__")

EvaluatedPartitionRule = typing.NewType("EvaluatedPartitionRule", "PartitionRule")

PartitionRulesEvaluator = rules.GenericRulesEvaluator[
    "PartitionRule", EvaluatedPartitionRule
]


@dataclasses.dataclass
class PartitionRule:
    name: PartitionRuleName
    conditions: conditions_mod.PartitionRuleConditions

    class T_from_dict(typing.TypedDict):
        name: PartitionRuleName
        conditions: conditions_mod.PartitionRuleConditions

    @classmethod
    def from_dict(cls, d: T_from_dict) -> "PartitionRule":
        return cls(**d)

    async def evaluate(
        self, pulls: list[context.BasePullRequest]
    ) -> EvaluatedPartitionRule:
        evaluated_part_rule = typing.cast(EvaluatedPartitionRule, self)
        await evaluated_part_rule.conditions(pulls)
        return evaluated_part_rule

    def copy(self) -> "PartitionRule":
        return self.__class__(
            name=self.name,
            conditions=conditions_mod.PartitionRuleConditions(
                self.conditions.condition.copy().conditions
            ),
        )


@dataclasses.dataclass
class PartitionRules:
    rules: list[PartitionRule]

    def __post_init__(self) -> None:
        names: set[PartitionRuleName] = set()
        for rule in self.rules:
            if rule.name in names:
                raise voluptuous.error.Invalid(
                    f"partition_rules names must be unique, found `{rule.name}` twice"
                )

            if rule.name == DEFAULT_PARTITION_NAME:
                raise voluptuous.error.Invalid(
                    f"`{DEFAULT_PARTITION_NAME}` is a reserved partition name and cannot be used"
                )

            names.add(rule.name)

    def __contains__(self, partition_name: PartitionRuleName) -> bool:
        return any(rule.name == partition_name for rule in self.rules)

    def __iter__(self) -> abc.Iterator[PartitionRule]:
        return iter(self.rules)

    def __len__(self) -> int:
        return len(self.rules)

    def __getitem__(self, key: PartitionRuleName) -> PartitionRule:
        for rule in self.rules:
            if rule.name == key:
                return rule
        raise KeyError(f"{key} not found")

    async def get_evaluated_partition_names_from_context(
        self, ctxt: context.Context
    ) -> list[PartitionRuleName]:
        if not self.rules:
            return []

        partition_rules = [rule.copy() for rule in self.rules]
        evaluator = await PartitionRulesEvaluator.create(
            partition_rules,
            ctxt.repository,
            [ctxt.pull_request],
            False,
        )

        if not any(rule.conditions.match for rule in evaluator.matching_rules):
            # no match at all = match all partitions
            return [rule.name for rule in self.rules]

        return [rule.name for rule in evaluator.matching_rules if rule.conditions.match]


PartitionRulesSchema = voluptuous.All(
    [
        voluptuous.All(
            {
                voluptuous.Required("name"): str,
                voluptuous.Required("conditions"): voluptuous.All(
                    [voluptuous.Coerce(cond_config.RuleConditionSchema)],
                    voluptuous.Coerce(conditions_mod.QueueRuleMergeConditions),
                ),
            },
            voluptuous.Coerce(PartitionRule.from_dict),
        )
    ],
    voluptuous.Coerce(PartitionRules),
)
