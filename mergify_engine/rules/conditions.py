# -*- encoding: utf-8 -*-
#
# Copyright © 2018-2021 Mergify SAS
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import abc
import dataclasses
import textwrap
import typing

import daiquiri
import voluptuous

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.clients import http
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers
from mergify_engine.rules import parser


LOG = daiquiri.getLogger(__name__)


# This helps mypy breaking the recursive definition
FakeTreeT = typing.Dict[str, typing.Any]

RuleConditionNode = typing.Union[
    "RuleConditionCombination", "RuleConditionNegation", "RuleCondition"
]

ConditionFilterKeyT = typing.Callable[[RuleConditionNode], bool]


@dataclasses.dataclass
class RuleCondition:
    """This describe a leaf of the `conditions:` tree, eg:

    label=foobar
    -merged
    """

    condition: typing.Union[str, FakeTreeT]
    label: typing.Optional[str] = None
    description: typing.Optional[str] = None
    partial_filter: filter.Filter[bool] = dataclasses.field(init=False)
    match: bool = dataclasses.field(init=False, default=False)
    _used: bool = dataclasses.field(init=False, default=False)
    evaluation_error: typing.Optional[str] = dataclasses.field(init=False, default=None)

    def __post_init__(self) -> None:
        self.update(self.condition)

    def update(self, condition_raw: typing.Union[str, FakeTreeT]) -> None:
        self.condition = condition_raw

        try:
            if isinstance(condition_raw, str):
                condition = parser.parse(condition_raw)
            else:
                condition = condition_raw
            self.partial_filter = filter.BinaryFilter(
                typing.cast(filter.TreeT, condition)
            )
        except (parser.ConditionParsingError, filter.InvalidQuery) as e:
            raise voluptuous.Invalid(
                message=f"Invalid condition '{condition_raw}'. {str(e)}",
                error_message=str(e),
            )

    def update_attribute_name(self, new_name: str) -> None:
        tree = typing.cast(filter.TreeT, self.partial_filter.tree)
        negate = "-" in tree
        tree = tree.get("-", tree)
        operator = list(tree.keys())[0]
        name, value = list(tree.values())[0]
        if name.startswith(filter.Filter.LENGTH_OPERATOR):
            new_name = f"{filter.Filter.LENGTH_OPERATOR}{new_name}"

        new_tree: FakeTreeT = {operator: (new_name, value)}
        if negate:
            new_tree = {"-": new_tree}
        self.update(new_tree)

    def __str__(self) -> str:
        if self.label is not None:
            return self.label
        elif isinstance(self.condition, str):
            return self.condition
        else:
            return str(self.partial_filter)

    def copy(self) -> "RuleCondition":
        rc = RuleCondition(self.condition, self.label, self.description)
        rc.partial_filter.value_expanders = self.partial_filter.value_expanders
        return rc

    async def __call__(self, obj: filter.GetAttrObjectT) -> bool:
        if self._used:
            raise RuntimeError(f"{self.__class__.__name__} cannot be re-used")

        self._used = True
        try:
            self.match = await self.partial_filter(obj)
        except live_resolvers.LiveResolutionFailure as e:
            self.match = False
            self.evaluation_error = e.reason

        return self.match

    def get_attribute_name(self) -> str:
        tree = typing.cast(filter.TreeT, self.partial_filter.tree)
        tree = tree.get("-", tree)
        name = list(tree.values())[0][0]
        if name.startswith(filter.Filter.LENGTH_OPERATOR):
            return str(name[1:])
        return str(name)


class RuleConditionGroup(abc.ABC):
    @abc.abstractmethod
    async def copy(self):
        pass

    async def __call__(self, obj: filter.GetAttrObjectT) -> bool:
        if getattr(self, "_used", False):
            raise RuntimeError(f"{self.__class__.__name__} cannot be re-used")

        self._used = True
        self.match = await self._get_filter_result(obj)

        return self.match

    @abc.abstractmethod
    async def _get_filter_result(self, obj: filter.GetAttrObjectT) -> bool:
        pass

    @property
    @abc.abstractmethod
    def operator_label(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def conditions(self) -> list[RuleConditionNode]:
        pass

    def get_summary(self) -> str:
        return self._walk_for_summary(self.conditions)

    def get_unmatched_summary(self) -> str:
        return self._walk_for_summary(self.conditions, filter_key=lambda c: not c.match)

    @classmethod
    def _walk_for_summary(
        cls,
        conditions: list[RuleConditionNode],
        level: int = 0,
        filter_key: typing.Optional[ConditionFilterKeyT] = None,
    ) -> str:
        summary = ""
        for condition in conditions:
            if filter_key and not filter_key(condition):
                continue

            if isinstance(condition, RuleCondition):
                summary += cls._get_rule_condition_summary(condition)
            elif isinstance(condition, RuleConditionGroup):
                checked = "X" if condition.match else " "
                summary += f"- [{checked}] {condition.operator_label}:"
                if condition.description:
                    summary += f" [{condition.description}]"
                summary += "\n"
                for _sum in cls._walk_for_summary(
                    condition.conditions, level + 1, filter_key=filter_key
                ):
                    summary += _sum
            else:
                raise RuntimeError(f"Unsupported condition type: {type(condition)}")

        return textwrap.indent(summary, "  " * min(level, 1))

    @staticmethod
    def _get_rule_condition_summary(condition: RuleCondition) -> str:
        summary = ""
        checked = "X" if condition.match else " "
        summary += f"- [{checked}] `{condition}`"
        if condition.description:
            summary += f" [{condition.description}]"
        if condition.evaluation_error:
            summary += f" ⚠️ {condition.evaluation_error}"
        summary += "\n"
        return summary

    def has_unmatched_conditions(self) -> bool:
        for condition in self.conditions:
            if not condition.match:
                return True

            if (
                isinstance(condition, RuleConditionGroup)
                and condition.has_unmatched_conditions()
            ):
                return True

        return False

    def walk(
        self, conditions: None | list[RuleConditionNode] = None
    ) -> typing.Iterator[RuleCondition]:
        if conditions is None:
            conditions = self.conditions

        for condition in conditions:
            if isinstance(condition, RuleCondition):
                yield condition
            elif isinstance(condition, RuleConditionGroup):
                for sub_condition in self.walk(condition.conditions):
                    yield sub_condition
            else:
                raise RuntimeError(f"Unsupported condition type: {type(condition)}")

    def extract_raw_filter_tree(self, condition: RuleConditionNode) -> filter.TreeT:
        if isinstance(condition, RuleCondition):
            return typing.cast(filter.TreeT, condition.partial_filter.tree)
        elif isinstance(condition, RuleConditionCombination):
            return typing.cast(
                filter.TreeT,
                {
                    condition.operator: [
                        self.extract_raw_filter_tree(c) for c in condition.conditions
                    ]
                },
            )
        elif isinstance(condition, RuleConditionNegation):
            return typing.cast(
                filter.TreeT,
                {condition.operator: self.extract_raw_filter_tree(condition.condition)},
            )
        else:
            raise RuntimeError(f"Unsupported condition type: {type(condition)}")


@dataclasses.dataclass
class RuleConditionCombination(RuleConditionGroup):
    """This describe a group leafs of the `conditions:` tree linked by and or or."""

    data: dataclasses.InitVar[
        dict[typing.Literal["and", "or"], list[RuleConditionNode]]
    ]
    operator: typing.Literal["and", "or"] = dataclasses.field(init=False)
    _conditions: list[RuleConditionNode] = dataclasses.field(init=False)
    description: typing.Optional[str] = None
    match: bool = dataclasses.field(init=False, default=False)

    def __post_init__(
        self,
        data: dict[typing.Literal["and", "or"], list[RuleConditionNode]],
    ) -> None:
        if len(data) != 1:
            raise RuntimeError("Invalid condition")

        self.operator, self._conditions = next(iter(data.items()))

    async def _get_filter_result(self, obj: filter.GetAttrObjectT) -> bool:
        return await filter.BinaryFilter(
            typing.cast(filter.TreeT, {self.operator: self.conditions})
        )(obj)

    @property
    def operator_label(self) -> str:
        return "all of" if self.operator == "and" else "any of"

    @property
    def conditions(self) -> list[RuleConditionNode]:
        return self._conditions

    def extract_raw_filter_tree(
        self, condition: None | RuleConditionNode = None
    ) -> filter.TreeT:
        if condition is None:
            condition = self

        return super().extract_raw_filter_tree(condition)

    def is_faulty(self) -> bool:
        return any(c.evaluation_error for c in self.walk())

    def copy(self) -> "RuleConditionCombination":
        return self.__class__(
            {self.operator: [c.copy() for c in self.conditions]},
            description=self.description,
        )


@dataclasses.dataclass
class RuleConditionNegation(RuleConditionGroup):
    """This describe a group leafs of the `conditions:` tree linked by not."""

    data: dataclasses.InitVar[dict[typing.Literal["not"], RuleConditionCombination]]
    operator: typing.Literal["not"] = dataclasses.field(init=False)
    condition: RuleConditionCombination = dataclasses.field(init=False)
    description: typing.Optional[str] = None
    match: bool = dataclasses.field(init=False, default=False)

    def __post_init__(
        self, data: dict[typing.Literal["not"], RuleConditionCombination]
    ) -> None:
        if len(data) != 1:
            raise RuntimeError("Invalid condition")

        self.operator, self.condition = next(iter(data.items()))

    def copy(self) -> "RuleConditionNegation":
        return self.__class__(
            {self.operator: self.condition.copy()}, description=self.description
        )

    @property
    def conditions(self) -> list[RuleConditionNode]:
        return [self.condition]

    @property
    def operator_label(self) -> str:
        return "not"

    async def _get_filter_result(self, obj: filter.GetAttrObjectT) -> bool:
        return await filter.BinaryFilter(
            typing.cast(filter.TreeT, {self.operator: self.condition})
        )(obj)


@dataclasses.dataclass
class QueueRuleConditions:
    conditions: dataclasses.InitVar[list[RuleConditionNode]]
    condition: RuleConditionCombination = dataclasses.field(init=False)
    _evaluated_conditions: typing.Dict[
        github_types.GitHubPullRequestNumber, RuleConditionCombination
    ] = dataclasses.field(default_factory=dict, init=False, repr=False)
    match: bool = dataclasses.field(init=False, default=False)
    _used: bool = dataclasses.field(init=False, default=False)

    def __post_init__(self, conditions: list[RuleConditionNode]) -> None:
        self.condition = RuleConditionCombination({"and": conditions})

    def copy(self) -> "QueueRuleConditions":
        return QueueRuleConditions(self.condition.copy().conditions)

    def extract_raw_filter_tree(self) -> filter.TreeT:
        return self.condition.extract_raw_filter_tree()

    async def __call__(
        self, pull_requests: typing.List[context.BasePullRequest]
    ) -> bool:
        if self._used:
            raise RuntimeError(f"{self.__class__.__name__} cannot be re-used")
        self._used = True

        for pull in pull_requests:
            c = self.condition.copy()
            await c(pull)
            self._evaluated_conditions[
                await pull.number  # type: ignore[attr-defined]
            ] = c

        self.match = all(c.match for c in self._evaluated_conditions.values())
        return self.match

    @classmethod
    def _get_rule_condition_summary(
        cls,
        conditions: typing.Mapping[github_types.GitHubPullRequestNumber, RuleCondition],
    ) -> str:

        first_key = next(iter(conditions))
        display_detail = (
            conditions[first_key].get_attribute_name()
            not in context.QueuePullRequest.QUEUE_ATTRIBUTES
        )

        summary = "- "
        if not display_detail:
            checked = "X" if conditions[first_key].match else " "
            summary += f"[{checked}] "
        summary += f"`{conditions[first_key]}`"

        if conditions[first_key].description:
            summary += f" [{conditions[first_key].description}]"
        summary += "\n"

        if display_detail:
            for pull_number, cond in conditions.items():
                checked = "X" if cond.match else " "
                summary += f"  - [{checked}] #{pull_number}"
                if cond.evaluation_error:
                    summary += f" ⚠️ {cond.evaluation_error}"
                summary += "\n"

        return summary

    @classmethod
    def _walk_for_summary(
        cls,
        evaluated_conditions: typing.Mapping[
            github_types.GitHubPullRequestNumber, RuleConditionNode
        ],
        level: int = -1,
    ) -> str:
        if not evaluated_conditions:
            raise RuntimeError("Empty conditions group")

        summary = ""
        first_key = next(iter(evaluated_conditions))
        first_evaluated_condition = evaluated_conditions[first_key]

        if isinstance(first_evaluated_condition, RuleCondition):
            evaluated_conditions = typing.cast(
                typing.Mapping[
                    github_types.GitHubPullRequestNumber,
                    RuleCondition,
                ],
                evaluated_conditions,
            )
            summary += cls._get_rule_condition_summary(evaluated_conditions)

        elif isinstance(first_evaluated_condition, RuleConditionGroup):
            evaluated_conditions = typing.cast(
                typing.Mapping[
                    github_types.GitHubPullRequestNumber,
                    RuleConditionCombination,
                ],
                evaluated_conditions,
            )
            if level >= 0:
                label = first_evaluated_condition.operator_label
                if first_evaluated_condition.description:
                    label += f" [{first_evaluated_condition.description}]"

                global_match = all(c.match for c in evaluated_conditions.values())
                checked = "X" if global_match else " "
                summary += f"- [{checked}] {label}:\n"

            inner_conditions = []
            for i in range(len(first_evaluated_condition.conditions)):
                inner_conditions.append(
                    {p: c.conditions[i] for p, c in evaluated_conditions.items()}
                )
            for inner_condition in inner_conditions:
                for _sum in cls._walk_for_summary(inner_condition, level + 1):
                    summary += _sum
        else:
            raise RuntimeError(
                f"Unsupported condition type: {type(first_evaluated_condition).__name__}"
            )

        return textwrap.indent(summary, "  " * min(level, 1))

    def get_summary(self) -> str:
        if self._used:
            return self._walk_for_summary(self._evaluated_conditions)
        else:
            return self.condition.get_summary()

    def is_faulty(self) -> bool:
        if self._used:
            return any(c.is_faulty() for c in self._evaluated_conditions.values())
        else:
            return self.condition.is_faulty()

    def walk(self) -> typing.Iterator[RuleCondition]:
        if self._used:
            for conditions in self._evaluated_conditions.values():
                for cond in conditions.walk():
                    yield cond
        else:
            for cond in self.condition.walk():
                yield cond


BRANCH_PROTECTION_CONDITION_TAG = "🛡 GitHub branch protection"


async def get_branch_protection_conditions(
    repository: context.Repository, ref: github_types.GitHubRefType, *, strict: bool
) -> list[RuleConditionNode]:
    protection = await repository.get_branch_protection(ref)
    conditions: list[RuleConditionNode] = []
    if protection:
        if "required_status_checks" in protection:
            conditions.extend(
                [
                    RuleConditionCombination(
                        {
                            "or": [
                                RuleCondition(f"check-success={check}"),
                                RuleCondition(f"check-neutral={check}"),
                                RuleCondition(f"check-skipped={check}"),
                            ]
                        },
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    )
                    for check in protection["required_status_checks"]["contexts"]
                ]
            )
            if (
                strict
                and "strict" in protection["required_status_checks"]
                and protection["required_status_checks"]["strict"]
            ):
                conditions.append(
                    RuleCondition(
                        "#commits-behind=0",
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    )
                )

        if (
            "required_pull_request_reviews" in protection
            and protection["required_pull_request_reviews"][
                "required_approving_review_count"
            ]
            > 0
        ):
            conditions.extend(
                [
                    RuleCondition(
                        f"#approved-reviews-by>={protection['required_pull_request_reviews']['required_approving_review_count']}",
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    ),
                    RuleCondition(
                        "#changes-requested-reviews-by=0",
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    ),
                ]
            )

        if (
            "required_conversation_resolution" in protection
            and protection["required_conversation_resolution"]["enabled"]
        ):
            conditions.append(
                RuleCondition(
                    "#review-threads-unresolved=0",
                    description=BRANCH_PROTECTION_CONDITION_TAG,
                )
            )

    return conditions


async def get_depends_on_conditions(ctxt: context.Context) -> list[RuleConditionNode]:
    conds: list[RuleConditionNode] = []

    for pull_request_number in ctxt.get_depends_on():
        try:
            dep_ctxt = await ctxt.repository.get_pull_request_context(
                pull_request_number
            )
        except http.HTTPNotFound:
            description = f"⛓️ ⚠️ *pull request not found* (#{pull_request_number})"
        else:
            description = f"⛓️ **{dep_ctxt.pull['title']}** ([#{pull_request_number}]({dep_ctxt.pull['html_url']}))"
        conds.append(
            RuleCondition(
                {"=": ("depends-on", f"#{pull_request_number}")},
                description=description,
            )
        )
    return conds


@dataclasses.dataclass
class PullRequestRuleConditions:
    conditions: dataclasses.InitVar[list[RuleConditionNode]]
    condition: RuleConditionCombination = dataclasses.field(init=False)

    def __post_init__(self, conditions: list[RuleConditionNode]) -> None:
        self.condition = RuleConditionCombination({"and": conditions})

    async def __call__(self, objs: typing.List[context.BasePullRequest]) -> bool:
        if len(objs) > 1:
            raise RuntimeError(
                f"{self.__class__.__name__} take only one pull request at a time"
            )
        return await self.condition(objs[0])

    def extract_raw_filter_tree(self) -> filter.TreeT:
        return self.condition.extract_raw_filter_tree()

    def get_summary(self) -> str:
        return self.condition.get_summary()

    def get_unmatched_summary(self) -> str:
        return self.condition.get_unmatched_summary()

    def has_unmatched_conditions(self) -> bool:
        return self.condition.has_unmatched_conditions()

    @property
    def match(self) -> bool:
        return self.condition.match

    def is_faulty(self) -> bool:
        return self.condition.is_faulty()

    def walk(self) -> typing.Iterator[RuleCondition]:
        for cond in self.condition.walk():
            yield cond

    def copy(self) -> "PullRequestRuleConditions":
        return PullRequestRuleConditions(self.condition.copy().conditions)
