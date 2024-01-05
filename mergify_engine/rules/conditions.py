from __future__ import annotations

import abc as abstract
from collections import abc
import dataclasses
import datetime  # noqa: TCH003
import functools
import html
import textwrap
import typing

import daiquiri
from first import first
import pydantic
import typing_extensions
import voluptuous

from mergify_engine import condition_value_querier
from mergify_engine import constants
from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.clients import http
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers
from mergify_engine.rules import parser


if typing.TYPE_CHECKING:
    from mergify_engine import context

LOG = daiquiri.getLogger(__name__)


# This helps mypy breaking the recursive definition
FakeTreeT = dict[str, typing.Any]

# FIXME(sileht): mypy doesn't work with | here as string as not interpretated
# as Type, but as raw string
# from __futures__ import annotations + quote removal, is supposed to fix that,
# but the file can't load in our type are recursively dependant
# I hope cpython will fix the issue before releasing __futures__.annotations
RuleConditionNode = typing.Union[
    "RuleConditionCombination",
    "RuleConditionNegation",
    "RuleCondition",
]

ConditionFilterKeyT = abc.Callable[[RuleConditionNode], bool]

EvaluatedConditionNodeT = abc.Mapping[
    github_types.GitHubPullRequestNumber,
    RuleConditionNode,
]
EvaluatedConditionT = abc.Mapping[github_types.GitHubPullRequestNumber, "RuleCondition"]
EvaluatedConditionGroupT = abc.Mapping[
    github_types.GitHubPullRequestNumber,
    typing.Union["RuleConditionCombination", "RuleConditionNegation"],
]


@dataclasses.dataclass
class RuleConditionFilters:
    condition: dataclasses.InitVar[filter.TreeT]

    boolean: filter.Filter[bool] = dataclasses.field(init=False)
    next_evaluation: filter.Filter[datetime.datetime] = dataclasses.field(init=False)
    related_checks: (
        filter.Filter[filter.ListValuesFilterResult] | None
    ) = dataclasses.field(init=False)

    def __post_init__(self, condition: filter.TreeT) -> None:
        self.boolean = filter.BinaryFilter(condition)
        self.next_evaluation = filter.NearDatetimeFilter(condition)

        # FIXME(MRGFY-2608): The `with_length_operator=True` has been added to make
        # `ConditionEvaluationResult` work with pydantic v2.
        # But this revealed that when we have a condition
        # that asserts the number of check (no matter the type, failure, success, etc),
        # like `#check-failure>0`, then there is nothing in the `related_checks`
        # because the filter expects to match with a string.
        # To make it work we would either need to put every check possible in the `related_checks`, but
        # this would maybe be problematic if the rules are evaluated when not all the checks have been added
        # to the pull request, or to rework how the `related_checks` works with `#check-.*` conditions.
        attribute = self.get_attribute_name(with_length_operator=True)
        if attribute.startswith(("check-", "status-")):
            new_tree = self._replace_attribute_name(
                condition,
                "check",
                overwrite_operator="=",
            )
            self.related_checks = filter.ListValuesFilter(
                typing.cast(filter.TreeT, new_tree),
            )
        else:
            self.related_checks = None

    def get_attribute_name(self, with_length_operator: bool = False) -> str:
        tree = typing.cast(filter.TreeT, self.boolean.tree)
        tree = tree.get("-", tree)
        values = typing.cast(list[filter.TreeBinaryLeafT], list(tree.values()))
        name = values[0][0]
        if name.startswith(filter.Filter.LENGTH_OPERATOR) and not with_length_operator:
            return str(name[1:])
        return str(name)

    @staticmethod
    def _replace_attribute_name(
        tree: filter.TreeT,
        new_name: str,
        overwrite_operator: str | None = None,
    ) -> FakeTreeT:
        negate = "-" in tree
        tree = tree.get("-", tree)
        operator = overwrite_operator or next(iter(tree.keys()))
        values = typing.cast(list[filter.TreeBinaryLeafT], list(tree.values()))
        name, value = values[0]
        if name.startswith(filter.Filter.LENGTH_OPERATOR):
            new_name = f"{filter.Filter.LENGTH_OPERATOR}{new_name}"

        new_tree: FakeTreeT = {operator: (new_name, value)}
        if negate:
            return {"-": new_tree}
        return new_tree


@dataclasses.dataclass
class RuleCondition:
    """This describe a leaf of the `conditions:` tree, eg:

    label=foobar
    -merged
    """

    filters: RuleConditionFilters
    label: str | None = None
    description: str | None = None

    match: bool = dataclasses.field(init=False, default=False)
    _used: bool = dataclasses.field(init=False, default=False)
    evaluation_error: str | None = dataclasses.field(init=False, default=None)
    related_checks: list[str] = dataclasses.field(init=False, default_factory=list)
    next_evaluation_at: datetime.datetime = dataclasses.field(
        init=False,
        default=date.DT_MAX,
    )

    @classmethod
    def from_tree(
        cls,
        condition: FakeTreeT,
        label: str | None = None,
        description: str | None = None,
    ) -> RuleCondition:
        return cls(
            RuleConditionFilters(typing.cast(filter.TreeT, condition)),
            label,
            description,
        )

    @classmethod
    def from_string(
        cls,
        condition: str,
        description: str | None = None,
        allow_command_attributes: bool = False,
    ) -> RuleCondition:
        try:
            return cls.from_tree(
                parser.parse(condition, allow_command_attributes),
                condition,
                description,
            )
        except (parser.ConditionParsingError, filter.InvalidQuery) as e:
            # Escape HTML chars that a user can insert in configuration
            escaped_condition = html.escape(condition)
            raise voluptuous.Invalid(
                message=f"Invalid condition '{escaped_condition}'. {e!s}",
                error_message=str(e),
            )

    def make_always_true(self) -> None:
        self.update({">": ("number", 0)})

    def update(self, condition: FakeTreeT) -> None:
        self.filters = RuleConditionFilters(typing.cast(filter.TreeT, condition))
        self.label = None

    def __str__(self) -> str:
        if self.label is not None:
            return self.label
        return str(self.filters.boolean)

    def copy(self) -> RuleCondition:
        return RuleCondition(self.filters, self.label, self.description)

    async def __call__(self, obj: filter.GetAttrObjectT) -> bool:
        if self._used:
            raise RuntimeError(f"{self.__class__.__name__} cannot be re-used")

        self._used = True
        try:
            self.match = await self.filters.boolean(obj)
            if self.filters.related_checks is not None:
                self.related_checks = (await self.filters.related_checks(obj)).values
            self.next_evaluation_at = await self.filters.next_evaluation(obj)
        except live_resolvers.LiveResolutionFailure as e:
            self.match = False
            self.evaluation_error = e.reason

        return self.match

    def get_attribute_name(self, with_length_operator: bool = False) -> str:
        return self.filters.get_attribute_name(with_length_operator)

    @property
    def value(self) -> typing.Any:
        tree = typing.cast(filter.TreeT, self.filters.boolean.tree)
        tree = tree.get("-", tree)
        _, tree = tree.get("@", ("", tree))  # type: ignore[misc]
        values = typing.cast(list[filter.TreeBinaryLeafT], list(tree.values()))
        return values[0][1]

    @property
    def operator(self) -> str:
        tree = typing.cast(filter.TreeT, self.filters.boolean.tree)
        tree = tree.get("-", tree)
        _, tree = tree.get("@", ("", tree))  # type: ignore[misc]
        return next(iter(tree.keys()))


class RuleConditionGroup(abstract.ABC):
    @abstract.abstractmethod
    def copy(self) -> RuleConditionGroup:
        pass

    async def __call__(self, obj: filter.GetAttrObjectT) -> bool:
        if getattr(self, "_used", False):
            raise RuntimeError(f"{self.__class__.__name__} cannot be re-used")

        self._used = True
        self.match = await self._get_filter_result(obj)

        return self.match

    @abstract.abstractmethod
    async def _get_filter_result(self, obj: filter.GetAttrObjectT) -> bool:
        pass

    @property
    @abstract.abstractmethod
    def operator_label(self) -> str:
        pass

    @property
    @abstract.abstractmethod
    def conditions(self) -> list[RuleConditionNode]:
        pass

    def get_summary(self) -> str:
        return self.get_evaluation_result().as_markdown()

    def get_unmatched_summary(self) -> str:
        return self.get_evaluation_result(
            filter_key=lambda c: not c.match,
        ).as_markdown()

    def get_evaluation_result(
        self,
        filter_key: ConditionFilterKeyT | None = None,
    ) -> ConditionEvaluationResult:
        return ConditionEvaluationResult.from_rule_condition_node(
            self,  # type:ignore [arg-type]
            filter_key=filter_key,
        )

    @staticmethod
    def _conditions_sort_key(
        condition: RuleConditionNode,
        should_match: bool,
    ) -> tuple[bool, int, typing.Any, typing.Any]:
        """
        Group conditions based on (in order):
        - If `condition.match` != `should_match`
        - If they are a normal condition (first) or a grouped condition (last)
        - Their name
        - Their description

        If `should_match=True`, all matching conditions will appear first,
        otherwise all non-matching conditions will appear first.
        """

        if isinstance(condition, RuleCondition):
            return (
                should_match != condition.match,
                0,
                str(condition),
                condition.description or "",
            )

        # RuleConditionGroup
        return (
            should_match != condition.match,
            1,
            condition.operator_label,
            condition.description or "",
        )

    @staticmethod
    def _get_conditions_ordered(
        conditions: list[RuleConditionNode],
        should_match: bool,
    ) -> list[RuleConditionNode]:
        cond_cpy = conditions.copy()
        cond_cpy.sort(
            key=functools.partial(
                RuleConditionGroup._conditions_sort_key,
                should_match=should_match,
            ),
        )
        return cond_cpy

    def walk(
        self,
        conditions: None | list[RuleConditionNode] = None,
        parent_condition_matching: bool = False,
        yield_only_failing_conditions: bool = False,
    ) -> abc.Iterator[RuleCondition]:
        if conditions is None:
            conditions = self.conditions

        ordered_conditions = RuleConditionGroup._get_conditions_ordered(
            conditions,
            should_match=parent_condition_matching,
        )

        for condition in ordered_conditions:
            if yield_only_failing_conditions and condition._used and condition.match:
                continue

            if isinstance(condition, RuleCondition):
                yield condition
            elif isinstance(condition, RuleConditionGroup):
                yield from self.walk(
                    condition.conditions,
                    parent_condition_matching=condition.match,
                    yield_only_failing_conditions=yield_only_failing_conditions,
                )
            else:
                raise RuntimeError(f"Unsupported condition type: {type(condition)}")

    def extract_raw_filter_tree(self, condition: RuleConditionNode) -> filter.TreeT:
        if isinstance(condition, RuleCondition):
            return typing.cast(filter.TreeT, condition.filters.boolean.tree)

        if isinstance(condition, RuleConditionCombination):
            return typing.cast(
                filter.TreeT,
                {
                    condition.operator: [
                        self.extract_raw_filter_tree(c) for c in condition.conditions
                    ],
                },
            )

        if isinstance(condition, RuleConditionNegation):
            return typing.cast(
                filter.TreeT,
                {condition.operator: self.extract_raw_filter_tree(condition.condition)},
            )

        raise RuntimeError(f"Unsupported condition type: {type(condition)}")


@dataclasses.dataclass
class RuleConditionCombination(RuleConditionGroup):
    """This describe a group leafs of the `conditions:` tree linked by and or or."""

    data: dataclasses.InitVar[
        dict[typing.Literal["and", "or"], list[RuleConditionNode]]
    ]
    operator: typing.Literal["and", "or"] = dataclasses.field(init=False)
    _conditions: list[RuleConditionNode] = dataclasses.field(init=False)
    description: str | None = None
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
            typing.cast(filter.TreeT, {self.operator: self.conditions}),
        )(obj)

    @property
    def operator_label(self) -> str:
        return "all of" if self.operator == "and" else "any of"

    @property
    def conditions(self) -> list[RuleConditionNode]:
        return self._conditions

    def extract_raw_filter_tree(
        self,
        condition: None | RuleConditionNode = None,
    ) -> filter.TreeT:
        if condition is None:
            condition = self

        return super().extract_raw_filter_tree(condition)

    def is_faulty(self) -> bool:
        return any(c.evaluation_error for c in self.walk())

    def copy(self) -> RuleConditionCombination:
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
    description: str | None = None
    match: bool = dataclasses.field(init=False, default=False)

    def __post_init__(
        self,
        data: dict[typing.Literal["not"], RuleConditionCombination],
    ) -> None:
        if len(data) != 1:
            raise RuntimeError("Invalid condition")

        self.operator, self.condition = next(iter(data.items()))

    def copy(self) -> RuleConditionNegation:
        return self.__class__(
            {self.operator: self.condition.copy()},
            description=self.description,
        )

    @property
    def conditions(self) -> list[RuleConditionNode]:
        return [self.condition]

    @property
    def operator_label(self) -> str:
        return "not"

    async def _get_filter_result(self, obj: filter.GetAttrObjectT) -> bool:
        return await filter.BinaryFilter(
            typing.cast(filter.TreeT, {self.operator: self.condition}),
        )(obj)


BRANCH_PROTECTION_CONDITION_TAG = "🛡 GitHub branch protection"


def get_mergify_configuration_change_conditions(
    action_name: str,
    allow_merging_configuration_change: bool,
) -> RuleConditionNode:
    description = (
        f"📌 {action_name} -> allow_merging_configuration_change setting requirement"
    )
    if allow_merging_configuration_change:
        return RuleConditionCombination(
            {
                "or": [
                    RuleCondition.from_tree(
                        {"=": ("mergify-configuration-changed", False)},
                    ),
                    RuleCondition.from_tree(
                        {
                            "=": (
                                "check-success",
                                constants.CONFIGURATION_CHANGED_CHECK_NAME,
                            ),
                        },
                    ),
                ],
            },
            description=description,
        )

    return RuleCondition.from_tree(
        {"=": ("mergify-configuration-changed", False)},
        description=description,
    )


async def get_branch_protection_conditions(
    repository: context.Repository,
    ref: github_types.GitHubRefType,
    *,
    strict: bool,
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
                                RuleCondition.from_tree(
                                    {"=": ("check-success", check)},
                                ),
                                RuleCondition.from_tree(
                                    {"=": ("check-neutral", check)},
                                ),
                                RuleCondition.from_tree(
                                    {"=": ("check-skipped", check)},
                                ),
                            ],
                        },
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    )
                    for check in protection["required_status_checks"]["contexts"]
                ],
            )
            if (
                strict
                and "strict" in protection["required_status_checks"]
                and protection["required_status_checks"]["strict"]
            ):
                conditions.append(
                    RuleCondition.from_tree(
                        {"=": ("#commits-behind", 0)},
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    ),
                )

        if (
            required_pull_request_reviews := protection.get(
                "required_pull_request_reviews",
            )
        ) is not None:
            if (
                required_pull_request_reviews["require_code_owner_reviews"]
                and required_pull_request_reviews["required_approving_review_count"] > 0
            ):
                conditions.append(
                    RuleCondition.from_tree(
                        {"=": ("branch-protection-review-decision", "APPROVED")},
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    ),
                )

            if required_pull_request_reviews["required_approving_review_count"] > 0:
                conditions.extend(
                    [
                        RuleCondition.from_tree(
                            {
                                ">=": (
                                    "#approved-reviews-by",
                                    required_pull_request_reviews[
                                        "required_approving_review_count"
                                    ],
                                ),
                            },
                            description=BRANCH_PROTECTION_CONDITION_TAG,
                        ),
                        RuleCondition.from_tree(
                            {"=": ("#changes-requested-reviews-by", 0)},
                            description=BRANCH_PROTECTION_CONDITION_TAG,
                        ),
                    ],
                )

        if (
            "required_conversation_resolution" in protection
            and protection["required_conversation_resolution"]["enabled"]
        ):
            conditions.append(
                RuleCondition.from_tree(
                    {"=": ("#review-threads-unresolved", 0)},
                    description=BRANCH_PROTECTION_CONDITION_TAG,
                ),
            )

    return conditions


async def get_depends_on_conditions(ctxt: context.Context) -> list[RuleConditionNode]:
    conds: list[RuleConditionNode] = []

    for pull_request_number in ctxt.get_depends_on():
        try:
            dep_ctxt = await ctxt.repository.get_pull_request_context(
                pull_request_number,
            )
        except http.HTTPNotFound:
            description = f"⛓️ ⚠️ *pull request not found* (#{pull_request_number})"
        else:
            # Escape HTML chars in PR title, for security
            escaped_pr_title = html.escape(dep_ctxt.pull["title"])
            description = f"⛓️ **{escaped_pr_title}** ([#{pull_request_number}]({dep_ctxt.pull['html_url']}))"
        conds.append(
            RuleCondition.from_tree(
                {"=": ("depends-on", f"#{pull_request_number}")},
                description=description,
            ),
        )
    return conds


def get_merge_after_condition(ctxt: context.Context) -> RuleConditionNode | None:
    merge_after = ctxt.get_merge_after()
    if merge_after is None:
        return None

    description = "🕒 Merge-After: " + merge_after.isoformat()

    return RuleCondition.from_tree(
        {">=": ("current-datetime", merge_after)},
        description=description,
    )


async def get_queue_conditions(
    ctxt: context.Context,
    for_queue_name: str | None = None,
) -> RuleConditionNode | None:
    queue_conditions: dict[str, RuleConditionNode] = {}
    for rule in ctxt.repository.mergify_config["queue_rules"]:
        if for_queue_name is not None and rule.name != for_queue_name:
            continue

        if (
            rule.require_branch_protection
            and rule.branch_protection_injection_mode == "queue"
        ):
            branch_protections = await get_branch_protection_conditions(
                ctxt.repository,
                ctxt.pull["base"]["ref"],
                strict=False,
            )
            if branch_protections:
                and_conds: list[RuleConditionNode] = branch_protections
                if rule.queue_conditions.condition._conditions:
                    and_conds.extend(rule.queue_conditions.condition.copy().conditions)

                conditions = RuleConditionCombination({"and": and_conds})
            else:
                conditions = rule.queue_conditions.condition.copy()

        else:
            conditions = rule.queue_conditions.condition.copy()

        conditions.description = f"📌 queue conditions of queue `{rule.name}`"
        queue_conditions[rule.name] = conditions

    if len(queue_conditions) >= 1:
        return RuleConditionCombination(
            {"or": list(queue_conditions.values())},
            description="🔀 queue conditions",
        )

    return None


BaseRuleConditionsType = typing.TypeVar(
    "BaseRuleConditionsType",
    bound="BaseRuleConditions",
)


@dataclasses.dataclass
class BaseRuleConditions:
    conditions: dataclasses.InitVar[list[RuleConditionNode]]
    condition: RuleConditionCombination = dataclasses.field(init=False)

    def __post_init__(self, conditions: list[RuleConditionNode]) -> None:
        self.condition = RuleConditionCombination({"and": conditions})

    async def __call__(
        self,
        objs: list[condition_value_querier.BasePullRequest],
    ) -> bool:
        if len(objs) > 1:
            raise RuntimeError(
                f"{self.__class__.__name__} take only one pull request at a time",
            )
        return await self.condition(objs[0])

    def extract_raw_filter_tree(self) -> filter.TreeT:
        return self.condition.extract_raw_filter_tree()

    @property
    def match(self) -> bool:
        return self.condition.match

    def is_faulty(self) -> bool:
        return self.condition.is_faulty()

    def walk(self) -> abc.Iterator[RuleCondition]:
        yield from self.condition.walk()

    def copy(self: BaseRuleConditionsType) -> BaseRuleConditionsType:
        return self.__class__(self.condition.copy().conditions)


@dataclasses.dataclass
class PullRequestRuleConditions(BaseRuleConditions):
    def get_summary(self) -> str:
        return self.condition.get_summary()

    def get_unmatched_summary(self) -> str:
        return self.condition.get_unmatched_summary()

    def get_evaluation_result(self) -> ConditionEvaluationResult:
        return self.condition.get_evaluation_result()


@dataclasses.dataclass
class PriorityRuleConditions(BaseRuleConditions):
    pass


@dataclasses.dataclass
class PartitionRuleConditions(BaseRuleConditions):
    pass


@dataclasses.dataclass
class QueueRuleMergeConditions(BaseRuleConditions):
    _evaluated_conditions: dict[
        github_types.GitHubPullRequestNumber,
        RuleConditionCombination,
    ] = dataclasses.field(default_factory=dict, init=False, repr=False)
    _match: bool = dataclasses.field(init=False, default=False)
    _used: bool = dataclasses.field(init=False, default=False)

    @property
    def match(self) -> bool:
        return self._match

    async def __call__(
        self,
        pull_requests: list[condition_value_querier.BasePullRequest],
    ) -> bool:
        if self._used:
            raise RuntimeError(f"{self.__class__.__name__} cannot be re-used")
        self._used = True

        for pull in pull_requests:
            c = self.condition.copy()
            await c(pull)
            self._evaluated_conditions[
                typing.cast(
                    github_types.GitHubPullRequestNumber,
                    await pull.get_attribute_value("number"),
                )
            ] = c

        self._match = all(c.match for c in self._evaluated_conditions.values())
        return self._match

    @staticmethod
    def _conditions_sort_key(
        condition: EvaluatedConditionNodeT,
        should_match: bool,
    ) -> tuple[bool, int, typing.Any, typing.Any]:
        """
        Group conditions based on (in order):
        - If `condition.match` != `should_match`
        - If they are a normal condition (first) or a grouped condition (last)
        - Their name
        - Their description

        If `should_match=True`, all matching conditions will appear first,
        otherwise all non-matching conditions will appear first.
        """

        first_key = next(iter(condition))
        if isinstance(condition[first_key], RuleCondition):
            return (
                should_match != all(cond.match for cond in condition.values()),
                0,
                str(condition[first_key]),
                condition[first_key].description or "",
            )

        # RuleConditionGroup
        return (
            should_match != all(cond.match for cond in condition.values()),
            1,
            condition[first_key].operator_label,  # type: ignore[union-attr]
            condition[first_key].description or "",
        )

    @staticmethod
    def _get_conditions_ordered(
        conditions: list[EvaluatedConditionNodeT],
        should_match: bool,
    ) -> list[EvaluatedConditionNodeT]:
        cond_cpy = conditions.copy()
        cond_cpy.sort(
            key=functools.partial(
                QueueRuleMergeConditions._conditions_sort_key,
                should_match=should_match,
            ),
        )
        return cond_cpy

    def get_summary(self, display_evaluations: bool | None = None) -> str:
        if self._used:
            return self.get_evaluation_result(display_evaluations).as_markdown()

        return self.condition.get_summary()

    def get_evaluation_result(
        self,
        display_evaluations: bool | None = None,
    ) -> QueueConditionEvaluationResult:
        return QueueConditionEvaluationResult.from_evaluated_condition_node(
            self._evaluated_conditions,
            display_evaluations,
        )

    def is_faulty(self) -> bool:
        if self._used:
            return any(c.is_faulty() for c in self._evaluated_conditions.values())
        return self.condition.is_faulty()

    def walk(
        self,
        yield_only_failing_conditions: bool = False,
    ) -> abc.Iterator[RuleCondition]:
        if self._used:
            for conditions in self._evaluated_conditions.values():
                if not yield_only_failing_conditions or (
                    yield_only_failing_conditions and not conditions.match
                ):
                    yield from conditions.walk(
                        yield_only_failing_conditions=yield_only_failing_conditions,
                    )
        else:
            yield from self.condition.walk()


@pydantic.dataclasses.dataclass
class ConditionEvaluationResult:
    match: bool
    label: str
    is_label_user_input: bool
    description: str | None = None
    evaluation_error: str | None = None
    related_checks: list[str] = dataclasses.field(default_factory=list)
    next_evaluation_at: datetime.datetime | None = None
    subconditions: list[ConditionEvaluationResult] = dataclasses.field(
        default_factory=list,
    )

    class Serialized(typing_extensions.TypedDict):
        match: bool
        label: str
        is_label_user_input: bool
        description: str | None
        evaluation_error: str | None
        related_checks: list[str]
        next_evaluation_at: datetime.datetime | None
        subconditions: list[ConditionEvaluationResult.Serialized]

    @classmethod
    def from_rule_condition_node(
        cls,
        rule_condition_node: RuleConditionNode,
        filter_key: ConditionFilterKeyT | None,
    ) -> ConditionEvaluationResult:
        if isinstance(rule_condition_node, RuleConditionGroup):
            return cls(
                match=rule_condition_node.match,
                label=rule_condition_node.operator_label,
                is_label_user_input=False,
                description=rule_condition_node.description,
                subconditions=cls._create_subconditions(
                    rule_condition_node,
                    filter_key,
                ),
            )

        if isinstance(rule_condition_node, RuleCondition):
            return cls(
                match=rule_condition_node.match,
                label=str(rule_condition_node),
                is_label_user_input=True,
                description=rule_condition_node.description,
                evaluation_error=rule_condition_node.evaluation_error,
                related_checks=rule_condition_node.related_checks,
                next_evaluation_at=rule_condition_node.next_evaluation_at,
            )

        raise RuntimeError(f"Unsupported condition type: {type(rule_condition_node)}")

    @classmethod
    def _create_subconditions(
        cls,
        condition_group: RuleConditionGroup,
        filter_key: ConditionFilterKeyT | None,
    ) -> list[ConditionEvaluationResult]:
        sorted_subconditions = RuleConditionGroup._get_conditions_ordered(
            condition_group.conditions,
            condition_group.match,
        )
        return [
            cls.from_rule_condition_node(c, filter_key)
            for c in sorted_subconditions
            if filter_key is None or filter_key(c)
        ]

    @classmethod
    def deserialize(
        cls,
        data: ConditionEvaluationResult.Serialized,
    ) -> ConditionEvaluationResult:
        return cls(
            match=data["match"],
            label=data["label"],
            is_label_user_input=data["is_label_user_input"],
            description=data["description"],
            evaluation_error=data["evaluation_error"],
            related_checks=data.get("related_checks", []),
            next_evaluation_at=data.get("next_evaluation_at"),
            subconditions=[cls.deserialize(c) for c in data["subconditions"]],
        )

    def serialized(self) -> ConditionEvaluationResult.Serialized:
        return typing.cast(
            ConditionEvaluationResult.Serialized,
            dataclasses.asdict(self),
        )

    def as_markdown(self) -> str:
        return "\n".join(self._markdown_iterator())

    def _markdown_iterator(self) -> abc.Generator[str, None, None]:
        for condition in self.subconditions:
            text = condition._as_markdown_element()
            if condition.subconditions:
                text += "\n"
                text += textwrap.indent(condition.as_markdown(), "  ")
            yield text

    def _as_markdown_element(self) -> str:
        check = "X" if self.match else " "
        label = f"`{self.label}`" if self.is_label_user_input else self.label
        text = f"- [{check}] {label}"

        if self.subconditions:
            text += ":"
        if self.description:
            text += f" [{self.description}]"
        if self.evaluation_error:
            text += f" ⚠️ {self.evaluation_error}"

        return text


@pydantic.dataclasses.dataclass
class QueueConditionEvaluationResult:
    match: bool
    label: str
    is_label_user_input: bool
    description: str | None = None
    attribute_name: str | None = None
    operator: str | None = None
    schedule: date.Schedule | None = None
    subconditions: list[QueueConditionEvaluationResult] = dataclasses.field(
        default_factory=list,
    )
    evaluations: list[QueueConditionEvaluationResult.Evaluation] = dataclasses.field(
        default_factory=list,
    )
    display_evaluations_: bool | None = None

    def copy(self) -> QueueConditionEvaluationResult:
        return QueueConditionEvaluationResult(
            match=self.match,
            label=self.label,
            is_label_user_input=self.is_label_user_input,
            description=self.description,
            attribute_name=self.attribute_name,
            operator=self.operator,
            schedule=self.schedule,
            subconditions=[s.copy() for s in self.subconditions],
            evaluations=[e.copy() for e in self.evaluations],
            display_evaluations_=self.display_evaluations_,
        )

    @pydantic.dataclasses.dataclass
    class Evaluation:
        pull_request: github_types.GitHubPullRequestNumber
        match: bool
        evaluation_error: str | None = None
        related_checks: list[str] = dataclasses.field(default_factory=list)
        next_evaluation_at: datetime.datetime | None = None

        class Serialized(typing.TypedDict):
            pull_request: github_types.GitHubPullRequestNumber
            match: bool
            evaluation_error: str | None
            related_checks: list[str]
            next_evaluation_at: datetime.datetime | None

        def copy(self) -> QueueConditionEvaluationResult.Evaluation:
            return QueueConditionEvaluationResult.Evaluation(
                pull_request=self.pull_request,
                match=self.match,
                evaluation_error=self.evaluation_error,
                related_checks=self.related_checks,
                next_evaluation_at=self.next_evaluation_at,
            )

        @classmethod
        def from_evaluated_condition(
            cls,
            pull_number: github_types.GitHubPullRequestNumber,
            condition: RuleCondition,
        ) -> QueueConditionEvaluationResult.Evaluation:
            next_evaluation_at = (
                condition.next_evaluation_at
                if condition.next_evaluation_at != date.DT_MAX
                else None
            )

            return cls(
                pull_request=pull_number,
                match=condition.match,
                evaluation_error=condition.evaluation_error,
                related_checks=condition.related_checks,
                next_evaluation_at=next_evaluation_at,
            )

    class Serialized(typing.TypedDict):
        match: bool
        label: str
        is_label_user_input: bool
        description: str | None
        attribute_name: str | None
        operator: str | None
        schedule: date.Schedule.Serialized | None
        subconditions: list[QueueConditionEvaluationResult.Serialized]
        evaluations: list[QueueConditionEvaluationResult.Evaluation.Serialized]

    @property
    def display_evaluations(self) -> bool:
        if self.display_evaluations_ is not None:
            return self.display_evaluations_

        return (
            self.attribute_name
            not in condition_value_querier.QueuePullRequest.QUEUE_ATTRIBUTES
        )

    @classmethod
    def from_evaluated_condition_node(
        cls,
        evaluated_condition_node: EvaluatedConditionNodeT,
        display_evaluations: bool | None = None,
    ) -> QueueConditionEvaluationResult:
        first_evaluated_condition = first(evaluated_condition_node.values())

        if isinstance(first_evaluated_condition, RuleConditionGroup):
            evaluated_condition_group = typing.cast(
                EvaluatedConditionGroupT,
                evaluated_condition_node,
            )
            global_match = all(c.match for c in evaluated_condition_group.values())

            return cls(
                match=global_match,
                label=first_evaluated_condition.operator_label,
                description=first_evaluated_condition.description,
                subconditions=cls._create_subconditions(
                    evaluated_condition_group,
                    global_match,
                    display_evaluations,
                ),
                is_label_user_input=False,
            )

        if isinstance(first_evaluated_condition, RuleCondition):
            evaluated_condition = typing.cast(
                EvaluatedConditionT,
                evaluated_condition_node,
            )
            schedule = (
                first_evaluated_condition.value
                if isinstance(first_evaluated_condition.value, date.Schedule)
                else None
            )

            return cls(
                match=first_evaluated_condition.match,
                label=str(first_evaluated_condition),
                description=first_evaluated_condition.description,
                attribute_name=first_evaluated_condition.get_attribute_name(),
                operator=first_evaluated_condition.operator,
                schedule=schedule,
                evaluations=[
                    cls.Evaluation.from_evaluated_condition(pull_request, condition)
                    for pull_request, condition in evaluated_condition.items()
                ],
                is_label_user_input=True,
                display_evaluations_=display_evaluations,
            )

        raise RuntimeError(
            f"Unsupported condition type: {type(first_evaluated_condition)}",
        )

    @classmethod
    def _create_subconditions(
        cls,
        evaluated_condition_group: EvaluatedConditionGroupT,
        global_match: bool,
        display_evaluations: bool | None = None,
    ) -> list[QueueConditionEvaluationResult]:
        first_evaluated_condition = next(iter(evaluated_condition_group.values()))
        evaluated_subconditions: list[EvaluatedConditionNodeT] = [
            {p: c.conditions[i] for p, c in evaluated_condition_group.items()}
            for i, _ in enumerate(first_evaluated_condition.conditions)
        ]
        sorted_subconditions = QueueRuleMergeConditions._get_conditions_ordered(
            evaluated_subconditions,
            global_match,
        )
        return [
            cls.from_evaluated_condition_node(c, display_evaluations)
            for c in sorted_subconditions
        ]

    @classmethod
    def deserialize(
        cls,
        data: QueueConditionEvaluationResult.Serialized,
    ) -> QueueConditionEvaluationResult:
        schedule = (
            date.Schedule.deserialize(schedule_data)
            if (schedule_data := data.get("schedule")) is not None
            else None
        )

        return cls(
            match=data["match"],
            label=data["label"],
            description=data["description"],
            attribute_name=data["attribute_name"],
            operator=data.get("operator"),
            schedule=schedule,
            subconditions=[cls.deserialize(c) for c in data["subconditions"]],
            evaluations=[
                QueueConditionEvaluationResult.Evaluation(**e)
                for e in data["evaluations"]
            ],
            is_label_user_input=data["is_label_user_input"],
        )

    def serialized(self) -> QueueConditionEvaluationResult.Serialized:
        return QueueConditionEvaluationResult.Serialized(
            match=self.match,
            label=self.label,
            is_label_user_input=self.is_label_user_input,
            description=self.description,
            attribute_name=self.attribute_name,
            operator=self.operator,
            schedule=self.schedule.serialized() if self.schedule is not None else None,
            subconditions=[c.serialized() for c in self.subconditions],
            evaluations=[
                typing.cast(
                    QueueConditionEvaluationResult.Evaluation.Serialized,
                    dataclasses.asdict(e),
                )
                for e in self.evaluations
            ],
        )

    def as_json_dict(self) -> QueueConditionEvaluationJsonSerialized:
        schedule = (
            self.schedule.as_json_dict(reverse=self.operator == "!=")
            if self.schedule is not None
            else None
        )

        return QueueConditionEvaluationJsonSerialized(
            match=self.match,
            label=self.label,
            description=self.description,
            schedule=schedule,
            subconditions=[c.as_json_dict() for c in self.subconditions],
            evaluations=[
                QueueConditionEvaluationJsonSerialized.Evaluation(
                    pull_request=evaluation.pull_request,
                    match=evaluation.match,
                    evaluation_error=evaluation.evaluation_error,
                    related_checks=evaluation.related_checks,
                    next_evaluation_at=evaluation.next_evaluation_at,
                )
                for evaluation in self.evaluations
            ],
        )

    def as_markdown(self) -> str:
        return "\n".join(self._markdown_iterator())

    def _markdown_iterator(self) -> abc.Generator[str, None, None]:
        for condition in self.subconditions:
            text = condition._as_markdown_element()
            if condition.subconditions:
                text += "\n"
                text += textwrap.indent(condition.as_markdown(), "  ")
            yield text

    def _as_markdown_element(self) -> str:
        label = f"`{self.label}`" if self.is_label_user_input else self.label

        if self.evaluations and self.display_evaluations:
            text = f"- {label}"
        else:
            check = "X" if self.match else " "
            text = f"- [{check}] {label}"

        if self.description:
            text += f" [{self.description}]"
        if self.subconditions:
            text += ":"

        if self.evaluations and self.display_evaluations:
            for evaluation in self.evaluations:
                check = "X" if evaluation.match else " "
                text += f"\n  - [{check}] #{evaluation.pull_request}"
                if evaluation.evaluation_error:
                    text += f" ⚠️ {evaluation.evaluation_error}"

        return text

    def get_evaluation_match_from_pr_number(
        self,
        pr_number: github_types.GitHubPullRequestNumber,
    ) -> bool | None:
        for evaluation in self.evaluations:
            if evaluation.pull_request == pr_number:
                return evaluation.match
        return None

    def get_related_checks(self) -> set[str]:
        related_checks = set()
        for evaluation in self.evaluations:
            related_checks.update(set(evaluation.related_checks))

        for subcondition in self.subconditions:
            related_checks.update(subcondition.get_related_checks())

        return related_checks


@pydantic.dataclasses.dataclass
class QueueConditionEvaluationJsonSerialized:
    # Due to some pydantic limitation, this type cannot be defined inside
    # `QueueConditionEvaluationResult` without having an error in the API.

    match: bool
    label: str
    description: str | None
    schedule: date.ScheduleJSON | None
    subconditions: list[QueueConditionEvaluationJsonSerialized]
    evaluations: list[QueueConditionEvaluationJsonSerialized.Evaluation]

    class Evaluation(typing_extensions.TypedDict):
        pull_request: github_types.GitHubPullRequestNumber
        match: bool
        evaluation_error: str | None
        related_checks: list[str]
        next_evaluation_at: datetime.datetime | None


def re_evaluate_schedule_conditions(
    conditions: list[QueueConditionEvaluationResult],
    from_time: datetime.datetime,
) -> list[QueueConditionEvaluationResult]:
    for cond in conditions:
        if cond.attribute_name == "schedule":
            _, op, condition_value, _, _, _ = parser.parse_raw_condition(cond.label)
            schedule_obj = parser.parse_schedule(condition_value)
            schedule_match = schedule_obj == from_time
            cond.match = (schedule_match and op == "=") or (
                not schedule_match and op == "!="
            )
            for cond_eval in cond.evaluations:
                cond_eval.match = cond.match

        elif cond.subconditions:
            cond.subconditions = re_evaluate_schedule_conditions(
                cond.subconditions,
                from_time,
            )
            cond.match = all(c.match for c in cond.subconditions)

    return conditions


def get_farthest_datetime_from_non_match_schedule_condition(
    conditions: list[QueueConditionEvaluationResult],
    pr_number: github_types.GitHubPullRequestNumber,
    from_time: datetime.datetime,
) -> datetime.datetime | None:
    farthest_datetime_from_non_match_conditions = None
    for cond in conditions:
        if cond.match or cond.get_evaluation_match_from_pr_number(pr_number):
            continue

        if cond.attribute_name == "schedule":
            _, op, condition_value, _, _, _ = parser.parse_raw_condition(cond.label)
            schedule_obj = parser.parse_schedule(condition_value)
            schedule_next_datetime = schedule_obj.get_next_datetime(from_time)
            if (
                farthest_datetime_from_non_match_conditions is None
                # https://github.com/python/mypy/issues/13973
                or schedule_next_datetime > farthest_datetime_from_non_match_conditions  # type: ignore[unreachable]
            ):
                farthest_datetime_from_non_match_conditions = schedule_next_datetime

        elif cond.subconditions:
            farthest_datetime_from_subconditions = (
                get_farthest_datetime_from_non_match_schedule_condition(
                    cond.subconditions,
                    pr_number,
                    from_time,
                )
            )
            if farthest_datetime_from_subconditions is not None and (
                farthest_datetime_from_non_match_conditions is None
                or farthest_datetime_from_subconditions
                > farthest_datetime_from_non_match_conditions
            ):
                farthest_datetime_from_non_match_conditions = (
                    farthest_datetime_from_subconditions
                )

    return farthest_datetime_from_non_match_conditions
