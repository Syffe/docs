from __future__ import annotations

import dataclasses
import typing

import daiquiri

from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers
from mergify_engine.rules import types


if typing.TYPE_CHECKING:
    from mergify_engine import condition_value_querier
    from mergify_engine import context
    from mergify_engine.rules import conditions as conditions_mod

LOG = daiquiri.getLogger(__name__)


@dataclasses.dataclass
class GenericRulesEvaluator(typing.Generic[types.T_Rule, types.T_EvaluatedRule]):
    """A rules that matches a pull request."""

    # Fixed attributes that are not considered when looking for the
    # next matching rules.
    FIXED_ATTRIBUTES = (
        "head",
        "author",
        "merged_by",
    )

    # Base attributes that are looked for in the rules matching, but
    # should not appear in summary.
    # Note(Syffe): this list was created after GitHub integrated the possibility to change
    # the base branch of a PR. It invalidated the unchangeable state of this base attribute.
    # and thus we needed to process it separately from the other base attributes.
    BASE_CHANGEABLE_ATTRIBUTES = ("base",)

    # The list of pull request rules to match against.
    rules: list[types.T_Rule]

    # The rules matching the pull request.
    matching_rules: list[types.T_EvaluatedRule] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    # The rules that can't be computed due to runtime error (eg: team resolution failure)
    faulty_rules: list[types.T_EvaluatedRule] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    # The rules not matching the pull request.
    ignored_rules: list[types.T_EvaluatedRule] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    # The rules not matching the base changeable attributes
    not_applicable_base_changeable_attributes_rules: list[
        types.T_EvaluatedRule
    ] = dataclasses.field(init=False, default_factory=list)

    # The rules evaluated with the pull request.
    evaluated_rules: list[types.T_EvaluatedRule] = dataclasses.field(
        init=False,
        default_factory=list,
    )

    @classmethod
    async def create(
        cls,
        rules: list[types.T_Rule],
        repository: context.Repository,
        pulls: list[condition_value_querier.BasePullRequest],
        rule_hidden_from_merge_queue: bool,
    ) -> GenericRulesEvaluator[types.T_Rule, types.T_EvaluatedRule]:
        self = cls(rules)

        for rule in self.rules:
            evaluated_rule_conditions = rule.get_conditions_used_by_evaluator()
            live_resolvers.apply_configure_filter(repository, evaluated_rule_conditions)

            evaluated_rule = typing.cast(  # type: ignore[redundant-cast]
                types.T_EvaluatedRule,
                await rule.evaluate(pulls),
            )
            self.evaluated_rules.append(evaluated_rule)
            del rule

            # NOTE(sileht):
            # In the summary, we display rules in five groups:
            # * rules where all attributes match -> matching_rules
            # * rules where only BASE_ATTRIBUTES match (filter out rule written for other branches) -> matching_rules
            # * rules that won't work due to a configuration issue detected at runtime (eg team doesn't exists) -> faulty_rules
            # * rules where only BASE_ATTRIBUTES don't match (mainly to hide rule written for other branches) -> ignored_rules
            # * rules where only BASE_CHANGEABLE ATTRIBUTES don't match (they rarely change but are handled)-> not_applicable_base_changeable_attributes_rules
            categorized = False

            if rule_hidden_from_merge_queue and not evaluated_rule_conditions.match:
                if await cls.can_attributes_make_rule_always_false(
                    repository,
                    evaluated_rule_conditions,
                    self.BASE_CHANGEABLE_ATTRIBUTES,
                    pulls,
                ):
                    self.not_applicable_base_changeable_attributes_rules.append(
                        evaluated_rule,
                    )

                if await cls.can_attributes_make_rule_always_false(
                    repository,
                    evaluated_rule_conditions,
                    self.FIXED_ATTRIBUTES,
                    pulls,
                ):
                    self.ignored_rules.append(evaluated_rule)
                    categorized = True

                if not categorized and evaluated_rule_conditions.is_faulty():
                    self.faulty_rules.append(evaluated_rule)
                    categorized = True

            if not categorized:
                self.matching_rules.append(evaluated_rule)

        return self

    @staticmethod
    async def can_attributes_make_rule_always_false(
        repository: context.Repository,
        conditions: conditions_mod.BaseRuleConditions,
        base_attributes: tuple[str, ...],
        pulls: list[condition_value_querier.BasePullRequest],
    ) -> bool:
        tree = conditions.extract_raw_filter_tree()
        for pull in pulls:
            f = filter.FixedAttributesFilter(
                tree,
                fixed_attributes=base_attributes,
            )
            live_resolvers.configure_filter(repository, f)

            try:
                ret = await f(pull)
            except live_resolvers.LiveResolutionFailureError:
                return False

            if ret in {
                True,
                filter.UnknownOnlyAttribute,
                filter.UnknownOrTrueAttribute,
            }:
                return False

        return True
