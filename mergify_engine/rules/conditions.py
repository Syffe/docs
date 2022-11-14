from __future__ import annotations

import abc as abstract
from collections import abc
import dataclasses
import functools
import html
import textwrap
import typing

import daiquiri
from first import first
import voluptuous

from mergify_engine import context
from mergify_engine import github_types
from mergify_engine.clients import http
from mergify_engine.rules import filter
from mergify_engine.rules import live_resolvers
from mergify_engine.rules import parser


LOG = daiquiri.getLogger(__name__)


# This helps mypy breaking the recursive definition
FakeTreeT = dict[str, typing.Any]

# FIXME(sileht): mypy doesn't work with | here as string as not interpretated
# as Type, but as raw string
# from __futures__ import annotations + quote removal, is supposed to fix that,
# but the file can't load in our type are recursively dependant
# I hope cpython will fix the issue before releasing __futures__.annotations
RuleConditionNode = typing.Union[
    "RuleConditionCombination", "RuleConditionNegation", "RuleCondition"  # noqa : NU003
]

ConditionFilterKeyT = abc.Callable[[RuleConditionNode], bool]

EvaluatedConditionNodeT = abc.Mapping[
    github_types.GitHubPullRequestNumber, RuleConditionNode
]
EvaluatedConditionT = abc.Mapping[github_types.GitHubPullRequestNumber, "RuleCondition"]
EvaluatedConditionGroupT = abc.Mapping[
    github_types.GitHubPullRequestNumber,
    typing.Union["RuleConditionCombination", "RuleConditionNegation"],  # noqa : NU003
]


DEPRECATE_CURRENT_CONDITIONS_BOOLEAN = False
DEPRECATED_CURRENT_CONDITIONS_NAMES = (
    "current-time",
    "current-day-of-week",
    "current-day",
    "current-month",
    "current-year",
    "current-timestamp",
)
DEPRECATED_CURRENT_CONDITIONS_MESSAGE = f"""⚠️  The following conditions are deprecated and must be replaced with the `schedule` condition: {', '.join([f"`{n}`" for n in DEPRECATED_CURRENT_CONDITIONS_NAMES])}.
A brownout day is planned for the whole day of January 11th, 2023.
Those conditions will be removed on February 11th, 2023.

For more informations and examples on how to use the `schedule` condition: https://docs.mergify.com/conditions/#attributes, https://docs.mergify.com/configuration/#time
"""


@dataclasses.dataclass
class RuleCondition:
    """This describe a leaf of the `conditions:` tree, eg:

    label=foobar
    -merged
    """

    condition: str | FakeTreeT
    label: str | None = None
    description: str | None = None
    allow_command_attributes: bool = False
    partial_filter: filter.Filter[bool] = dataclasses.field(init=False)
    match: bool = dataclasses.field(init=False, default=False)
    _used: bool = dataclasses.field(init=False, default=False)
    evaluation_error: str | None = dataclasses.field(init=False, default=None)

    def __post_init__(self) -> None:
        self.update(self.condition)

    def update(self, condition_raw: str | FakeTreeT) -> None:
        self.condition = condition_raw

        try:
            if isinstance(condition_raw, str):
                condition = parser.parse(condition_raw, self.allow_command_attributes)
            else:
                condition = condition_raw
            self.partial_filter = filter.BinaryFilter(
                typing.cast(filter.TreeT, condition)
            )
        except (parser.ConditionParsingError, filter.InvalidQuery) as e:
            # Escape HTML chars that a user can insert in configuration
            escaped_condition_raw = html.escape(str(condition_raw))
            raise voluptuous.Invalid(
                message=f"Invalid condition '{escaped_condition_raw}'. {str(e)}",
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
        rc = RuleCondition(
            self.condition, self.label, self.description, self.allow_command_attributes
        )
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
            filter_key=lambda c: not c.match
        ).as_markdown()

    def get_evaluation_result(
        self, filter_key: ConditionFilterKeyT | None = None
    ) -> ConditionEvaluationResult:
        return ConditionEvaluationResult(
            self, filter_key=filter_key  # type:ignore [arg-type]
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
                RuleConditionGroup._conditions_sort_key, should_match=should_match
            )
        )
        return cond_cpy

    def walk(
        self,
        conditions: None | list[RuleConditionNode] = None,
        parent_condition_matching: bool = False,
    ) -> abc.Iterator[RuleCondition]:
        if conditions is None:
            conditions = self.conditions

        ordered_conditions = RuleConditionGroup._get_conditions_ordered(
            conditions, should_match=parent_condition_matching
        )

        for condition in ordered_conditions:
            if isinstance(condition, RuleCondition):
                yield condition
            elif isinstance(condition, RuleConditionGroup):
                yield from self.walk(
                    condition.conditions, parent_condition_matching=condition.match
                )
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
    description: str | None = None
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
    _evaluated_conditions: dict[
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

    async def __call__(self, pull_requests: list[context.BasePullRequest]) -> bool:
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

    @staticmethod
    def _conditions_sort_key(
        condition: EvaluatedConditionNodeT, should_match: bool
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
        conditions: list[EvaluatedConditionNodeT], should_match: bool
    ) -> list[EvaluatedConditionNodeT]:
        cond_cpy = conditions.copy()
        cond_cpy.sort(
            key=functools.partial(
                QueueRuleConditions._conditions_sort_key, should_match=should_match
            )
        )
        return cond_cpy

    def get_summary(self) -> str:
        if self._used:
            summary = self.get_evaluation_result().as_markdown()
        else:
            summary = self.condition.get_summary()

        for cond in self.walk():
            if cond.get_attribute_name() in DEPRECATED_CURRENT_CONDITIONS_NAMES:
                return summary + "\n" + DEPRECATED_CURRENT_CONDITIONS_MESSAGE

        return summary

    def get_evaluation_result(self) -> QueueConditionEvaluationResult:
        return QueueConditionEvaluationResult(self._evaluated_conditions)

    def is_faulty(self) -> bool:
        if self._used:
            return any(c.is_faulty() for c in self._evaluated_conditions.values())
        else:
            return self.condition.is_faulty()

    def walk(self) -> abc.Iterator[RuleCondition]:
        if self._used:
            for conditions in self._evaluated_conditions.values():
                yield from conditions.walk()
        else:
            yield from self.condition.walk()


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
            required_pull_request_reviews := protection.get(
                "required_pull_request_reviews"
            )
        ) is not None:

            if required_pull_request_reviews["require_code_owner_reviews"]:
                conditions.append(
                    RuleCondition(
                        "branch-protection-review-decision=APPROVED",
                        description=BRANCH_PROTECTION_CONDITION_TAG,
                    )
                )

            if required_pull_request_reviews["required_approving_review_count"] > 0:
                conditions.extend(
                    [
                        RuleCondition(
                            f"#approved-reviews-by>={required_pull_request_reviews['required_approving_review_count']}",
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
            # Escape HTML chars in PR title, for security
            escaped_pr_title = html.escape(dep_ctxt.pull["title"])
            description = f"⛓️ **{escaped_pr_title}** ([#{pull_request_number}]({dep_ctxt.pull['html_url']}))"
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

    async def __call__(self, objs: list[context.BasePullRequest]) -> bool:
        if len(objs) > 1:
            raise RuntimeError(
                f"{self.__class__.__name__} take only one pull request at a time"
            )
        return await self.condition(objs[0])

    def extract_raw_filter_tree(self) -> filter.TreeT:
        return self.condition.extract_raw_filter_tree()

    def get_summary(self) -> str:
        for cond in self.walk():
            if cond.get_attribute_name() in DEPRECATED_CURRENT_CONDITIONS_NAMES:
                return (
                    self.condition.get_summary()
                    + "\n"
                    + DEPRECATED_CURRENT_CONDITIONS_MESSAGE
                )

        return self.condition.get_summary()

    def get_unmatched_summary(self) -> str:
        for cond in self.walk():
            if cond.get_attribute_name() in DEPRECATED_CURRENT_CONDITIONS_NAMES:
                return (
                    self.condition.get_unmatched_summary()
                    + "\n"
                    + DEPRECATED_CURRENT_CONDITIONS_MESSAGE
                )

        return self.condition.get_unmatched_summary()

    @property
    def match(self) -> bool:
        return self.condition.match

    def is_faulty(self) -> bool:
        return self.condition.is_faulty()

    def walk(self) -> abc.Iterator[RuleCondition]:
        yield from self.condition.walk()

    def copy(self) -> "PullRequestRuleConditions":
        return PullRequestRuleConditions(self.condition.copy().conditions)


@dataclasses.dataclass
class ConditionEvaluationResult:
    init_data: dataclasses.InitVar[RuleConditionNode]
    filter_key: dataclasses.InitVar[ConditionFilterKeyT | None] = None

    match: bool = dataclasses.field(init=False)
    label: str = dataclasses.field(init=False)
    description: str | None = dataclasses.field(init=False, default=None)
    evaluation_error: str | None = dataclasses.field(init=False, default=None)
    subconditions: list[ConditionEvaluationResult] = dataclasses.field(
        init=False, default_factory=list
    )

    def __post_init__(
        self, init_data: RuleConditionNode, filter_key: ConditionFilterKeyT | None
    ) -> None:
        if isinstance(init_data, RuleConditionGroup):
            self.match = init_data.match
            self.label = init_data.operator_label
            self.description = init_data.description
            self.subconditions = self._create_subconditions(init_data, filter_key)
        elif isinstance(init_data, RuleCondition):
            self.match = init_data.match
            self.label = f"`{init_data}`"
            self.description = init_data.description
            self.evaluation_error = init_data.evaluation_error
        else:
            raise RuntimeError(f"Unsupported condition type: {type(init_data)}")

    def _create_subconditions(
        self,
        condition_group: RuleConditionGroup,
        filter_key: ConditionFilterKeyT | None,
    ) -> list[ConditionEvaluationResult]:
        sorted_subconditions = RuleConditionGroup._get_conditions_ordered(
            condition_group.conditions, self.match
        )
        return [
            ConditionEvaluationResult(c, filter_key)
            for c in sorted_subconditions
            if filter_key is None or filter_key(c)
        ]

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
        text = f"- [{check}] {self.label}"

        if self.description:
            text += f" [{self.description}]"
        if self.evaluation_error:
            text += f" ⚠️ {self.evaluation_error}"
        if self.subconditions:
            text += ":"

        return text


@dataclasses.dataclass
class QueueConditionEvaluationResult:
    init_data: dataclasses.InitVar[EvaluatedConditionNodeT]

    match: bool = dataclasses.field(init=False)
    label: str = dataclasses.field(init=False)
    description: str | None = dataclasses.field(init=False, default=None)
    attribute_name: str | None = dataclasses.field(init=False, default=None)
    subconditions: list[QueueConditionEvaluationResult] = dataclasses.field(
        init=False, default_factory=list
    )
    pull_request_evaluations: dict[
        github_types.GitHubPullRequestNumber, Evaluation
    ] = dataclasses.field(init=False, default_factory=dict)

    class Evaluation(typing.NamedTuple):
        match: bool
        evaluation_error: str | None

    def __post_init__(self, init_data: EvaluatedConditionNodeT) -> None:
        first_evaluated_condition = first(init_data.values())

        if isinstance(first_evaluated_condition, RuleConditionGroup):
            init_data = typing.cast(EvaluatedConditionGroupT, init_data)

            self.match = all(c.match for c in init_data.values())
            self.label = first_evaluated_condition.operator_label
            self.description = first_evaluated_condition.description
            self.subconditions = self._create_subconditions(init_data)
        elif isinstance(first_evaluated_condition, RuleCondition):
            init_data = typing.cast(EvaluatedConditionT, init_data)

            self.match = first_evaluated_condition.match
            self.label = f"`{first_evaluated_condition}`"
            self.description = first_evaluated_condition.description
            self.pull_request_evaluations = {
                pull_request: QueueConditionEvaluationResult.Evaluation(
                    condition.match, condition.evaluation_error
                )
                for pull_request, condition in init_data.items()
            }
            self.attribute_name = first_evaluated_condition.get_attribute_name()
        else:
            raise RuntimeError(
                f"Unsupported condition type: {type(first_evaluated_condition)}"
            )

    @property
    def display_pull_request_evaluations(self) -> bool:
        return self.attribute_name not in context.QueuePullRequest.QUEUE_ATTRIBUTES

    def _create_subconditions(
        self, evaluated_condition_group: EvaluatedConditionGroupT
    ) -> list[QueueConditionEvaluationResult]:
        first_evaluated_condition = next(iter(evaluated_condition_group.values()))
        evaluated_subconditions: list[EvaluatedConditionNodeT] = [
            {p: c.conditions[i] for p, c in evaluated_condition_group.items()}
            for i, _ in enumerate(first_evaluated_condition.conditions)
        ]
        sorted_subconditions = QueueRuleConditions._get_conditions_ordered(
            evaluated_subconditions, self.match
        )
        return [QueueConditionEvaluationResult(c) for c in sorted_subconditions]

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
        if self.pull_request_evaluations and self.display_pull_request_evaluations:
            text = f"- {self.label}"
        else:
            check = "X" if self.match else " "
            text = f"- [{check}] {self.label}"

        if self.description:
            text += f" [{self.description}]"
        if self.subconditions:
            text += ":"

        if self.pull_request_evaluations and self.display_pull_request_evaluations:
            for pull_number, evaluation in self.pull_request_evaluations.items():
                check = "X" if evaluation.match else " "
                text += f"\n  - [{check}] #{pull_number}"
                if evaluation.evaluation_error:
                    text += f" ⚠️ {evaluation.evaluation_error}"

        return text
