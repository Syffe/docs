from __future__ import annotations

from collections import abc
import dataclasses
import datetime
import enum
import inspect
import operator
import re
import typing

import jinja2

from mergify_engine import date


_T = typing.TypeVar("_T")


class InvalidQuery(Exception):
    pass


class ParseError(InvalidQuery):
    def __init__(self, tree: "TreeT") -> None:
        super().__init__(f"Unable to parse tree: {str(tree)}")
        self.tree = tree


class UnknownAttribute(InvalidQuery, ValueError):
    def __init__(self, key: str) -> None:
        super().__init__(f"Unknown attribute: {str(key)}")
        self.key = key


class UnknownOperator(InvalidQuery, ValueError):
    def __init__(self, operator: str) -> None:
        super().__init__(f"Unknown operator: {str(operator)}")
        self.operator = operator


class InvalidOperator(InvalidQuery, TypeError):
    def __init__(self, operator: str) -> None:
        super().__init__(f"Invalid operator: {str(operator)}")
        self.operator = operator


class InvalidArguments(InvalidQuery, ValueError):
    def __init__(self, arguments: typing.Any) -> None:
        super().__init__(f"Invalid arguments: {str(arguments)}")
        self.arguments = arguments


def _identity(value: _T) -> _T:
    return value


def _format_attribute_value(value: _T) -> _T | list[str] | str:
    if (
        isinstance(value, list)
        and value
        and getattr(value[0], "__string_like__", False)
    ):
        return [str(v) for v in value]
    if getattr(value, "__string_like__", False):
        return str(value)
    return value


TreeBinaryLeafT = tuple[str, typing.Any]

TreeT = typing.TypedDict(
    "TreeT",
    {
        "-": "TreeT",
        "=": TreeBinaryLeafT,
        "<": TreeBinaryLeafT,
        ">": TreeBinaryLeafT,
        "<=": TreeBinaryLeafT,
        ">=": TreeBinaryLeafT,
        "!=": TreeBinaryLeafT,
        "~=": TreeBinaryLeafT,
        "@": "TreeT",
        "or": abc.Iterable["TreeT"],
        "and": abc.Iterable["TreeT"],
        "not": "TreeT",
    },
    total=False,
)


class GetAttrObject(typing.Protocol):
    def __getattribute__(self, key: typing.Any) -> typing.Any:
        ...


GetAttrObjectT = typing.TypeVar("GetAttrObjectT", bound=GetAttrObject)
FilterResultT = typing.TypeVar("FilterResultT")
CompiledTreeT = abc.Callable[[GetAttrObjectT], abc.Awaitable[FilterResultT]]


ValueCompilerT = abc.Callable[[typing.Any], typing.Any]


UnaryOperatorT = abc.Callable[[typing.Any], FilterResultT]
BinaryOperatorT = tuple[
    abc.Callable[[typing.Any, typing.Any], FilterResultT],
    abc.Callable[[abc.Iterable[object]], FilterResultT],
    ValueCompilerT,
]
MultipleOperatorT = abc.Callable[..., FilterResultT]


@dataclasses.dataclass(repr=False)
class Filter(typing.Generic[FilterResultT]):
    tree: TreeT | CompiledTreeT[GetAttrObject, FilterResultT]
    unary_operators: dict[str, UnaryOperatorT[FilterResultT]]
    binary_operators: dict[str, BinaryOperatorT[FilterResultT]]
    multiple_operators: dict[str, MultipleOperatorT[FilterResultT]]

    value_expanders: dict[
        str, abc.Callable[[typing.Any], list[typing.Any]]
    ] = dataclasses.field(default_factory=dict, init=False)

    _eval: CompiledTreeT[GetAttrObject, FilterResultT] = dataclasses.field(init=False)

    def __post_init__(self) -> None:
        self._eval = self.build_evaluator(self.tree)

    def __str__(self) -> str:
        return self._tree_to_str(self.tree)

    def _tree_to_str(
        self, tree: TreeT | CompiledTreeT[GetAttrObject, FilterResultT]
    ) -> str:
        if callable(tree):
            raise RuntimeError("Cannot convert compiled tree")

        # We don't do any kind of validation here since build_evaluator does
        # that.
        op, nodes = list(tree.items())[0]

        if op in self.multiple_operators:
            return "(" + f" {op} ".join(self._tree_to_str(n) for n in nodes) + ")"  # type: ignore[attr-defined]

        if op in self.unary_operators:
            return op + self._tree_to_str(nodes)  # type: ignore[arg-type]

        if op in self.binary_operators:
            if isinstance(nodes[1], bool):  # type: ignore[index]
                if self.binary_operators[op][0] != operator.eq:
                    raise InvalidOperator(op)
                return ("" if nodes[1] else "-") + str(nodes[0])  # type: ignore[index]
            elif isinstance(nodes[1], datetime.datetime):  # type: ignore[index]
                return (
                    str(nodes[0])  # type: ignore[index]
                    + op
                    + nodes[1].replace(tzinfo=None).isoformat(timespec="seconds")  # type: ignore[index]
                )
            elif isinstance(nodes[1], datetime.time):  # type: ignore[index]
                return (
                    str(nodes[0])  # type: ignore[index]
                    + op
                    + nodes[1].replace(tzinfo=None).isoformat(timespec="minutes")  # type: ignore[index]
                )
            else:
                return str(nodes[0]) + op + str(nodes[1])  # type: ignore[index]
        raise InvalidOperator(op)  # pragma: no cover

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}({str(self)})"

    async def __call__(self, obj: GetAttrObjectT) -> FilterResultT:
        return await self._eval(obj)

    LENGTH_OPERATOR = "#"

    @staticmethod
    def _to_list(item: _T | abc.Iterable[_T]) -> list[_T]:
        if isinstance(item, str):
            return [typing.cast(_T, item)]

        if isinstance(item, abc.Iterable):
            return list(item)

        return [item]

    async def _get_attribute_values(
        self,
        obj: GetAttrObjectT,
        attribute_name: str,
        op: abc.Callable[[typing.Any], typing.Any],
    ) -> list[typing.Any]:
        try:
            attr = getattr(obj, attribute_name)
            if inspect.iscoroutine(attr):
                attr = await attr
        except AttributeError:
            raise UnknownAttribute(attribute_name)

        try:
            values = op(attr)
        except TypeError:
            raise InvalidOperator(attribute_name)

        return self._to_list(values)

    async def _find_attribute_values(
        self,
        obj: GetAttrObjectT,
        attribute_name: str,
    ) -> list[typing.Any]:
        if attribute_name.startswith(self.LENGTH_OPERATOR):
            try:
                return await self._get_attribute_values(
                    obj, attribute_name, _format_attribute_value
                )
            except UnknownAttribute:
                return await self._get_attribute_values(obj, attribute_name[1:], len)
        else:
            return await self._get_attribute_values(
                obj, attribute_name, _format_attribute_value
            )

    def build_evaluator(
        self,
        tree: TreeT | CompiledTreeT[GetAttrObject, FilterResultT],
    ) -> CompiledTreeT[GetAttrObject, FilterResultT]:
        if callable(tree):
            return tree

        if len(tree) != 1:
            raise ParseError(tree)

        operator_name, nodes = list(tree.items())[0]

        if operator_name == "@":
            # NOTE(sileht): the value is already a TreeT, so just evaluate it.
            # e.g., {"@", ("schedule", {"and": [{"=", ("time", "10:10"), ...}]})}
            return self.build_evaluator(typing.cast(tuple[str, TreeT], nodes)[1])

        try:
            multiple_op = self.multiple_operators[operator_name]
        except KeyError:
            try:
                unary_operator = self.unary_operators[operator_name]
            except KeyError:
                try:
                    binary_operator = self.binary_operators[operator_name]
                except KeyError:
                    raise UnknownOperator(operator_name)
                nodes = typing.cast(TreeBinaryLeafT, nodes)
                return self._handle_binary_op(binary_operator, nodes)
            nodes = typing.cast(TreeT, nodes)
            return self._handle_unary_op(unary_operator, nodes)
        if not isinstance(nodes, abc.Iterable):
            raise InvalidArguments(nodes)
        return self._handle_multiple_op(multiple_op, nodes)

    def _eval_binary_op(
        self,
        op: BinaryOperatorT[FilterResultT],
        attribute_name: str,
        attribute_values: list[typing.Any],
        ref_values_expanded: list[typing.Any],
    ) -> FilterResultT:
        binary_op, iterable_op, _ = op
        return iterable_op(
            binary_op(attribute_value, ref_value)
            for attribute_value in attribute_values
            for ref_value in ref_values_expanded
        )

    def _handle_binary_op(
        self,
        op: BinaryOperatorT[FilterResultT],
        nodes: TreeBinaryLeafT,
    ) -> CompiledTreeT[GetAttrObject, FilterResultT]:
        if len(nodes) != 2:
            raise InvalidArguments(nodes)

        attribute_name, reference_value = nodes
        _, _, compile_fn = op
        if isinstance(reference_value, JinjaTemplateWrapper):
            reference_value.set_compile_func(compile_fn)
        else:
            try:
                reference_value = compile_fn(reference_value)
            except Exception as e:
                raise InvalidArguments(str(e))

        async def _op(obj: GetAttrObjectT) -> FilterResultT:
            nonlocal reference_value
            attribute_values = await self._find_attribute_values(obj, attribute_name)
            reference_value_expander = self.value_expanders.get(
                attribute_name, self._to_list
            )

            if isinstance(reference_value, JinjaTemplateWrapper):
                reference_value = await reference_value.render_async(obj)

            ref_values_expanded = reference_value_expander(reference_value)
            if inspect.iscoroutine(ref_values_expanded):
                ref_values_expanded = await typing.cast(
                    abc.Awaitable[typing.Any], ref_values_expanded
                )

            return self._eval_binary_op(
                op, attribute_name, attribute_values, ref_values_expanded
            )

        return _op

    def _handle_unary_op(
        self, unary_op: UnaryOperatorT[FilterResultT], nodes: TreeT
    ) -> CompiledTreeT[GetAttrObject, FilterResultT]:
        element = self.build_evaluator(nodes)

        async def _unary_op(values: GetAttrObjectT) -> FilterResultT:
            return unary_op(await element(values))

        return _unary_op

    def _handle_multiple_op(
        self,
        multiple_op: MultipleOperatorT[FilterResultT],
        nodes: abc.Iterable[TreeT | CompiledTreeT[GetAttrObject, FilterResultT]],
    ) -> CompiledTreeT[GetAttrObject, FilterResultT]:
        elements = [self.build_evaluator(node) for node in nodes]

        async def _multiple_op(values: GetAttrObjectT) -> FilterResultT:
            return multiple_op([await element(values) for element in elements])

        return _multiple_op


def BinaryFilter(
    tree: TreeT | CompiledTreeT[GetAttrObject, bool],
) -> "Filter[bool]":
    return Filter[bool](
        tree,
        {
            "-": operator.not_,
            "not": operator.not_,
        },
        {
            "=": (operator.eq, any, _identity),
            "<": (lambda a, b: a is not None and a < b, any, _identity),
            ">": (lambda a, b: a is not None and a > b, any, _identity),
            "<=": (lambda a, b: a == b or (a is not None and a <= b), any, _identity),
            ">=": (lambda a, b: a == b or (a is not None and a >= b), any, _identity),
            "!=": (operator.ne, all, _identity),
            "~=": (lambda a, b: a is not None and b.search(a), any, re.compile),
        },
        {
            "or": any,
            "and": all,
        },
    )


@dataclasses.dataclass
class ListValuesFilterResult:
    values: list[typing.Any] = dataclasses.field(default_factory=list)
    filtered_values: list[typing.Any] = dataclasses.field(default_factory=list)

    @classmethod
    def from_iterable(cls, values: abc.Iterable[object]) -> "ListValuesFilterResult":
        values = typing.cast(abc.Iterable[tuple[typing.Any, bool]], values)
        obj = cls()
        for value, match in values:
            if match:
                obj.values.append(value)
            else:
                obj.filtered_values.append(value)
        return obj

    @classmethod
    def negate(cls, other: "ListValuesFilterResult") -> "ListValuesFilterResult":
        return cls(values=other.filtered_values, filtered_values=other.values)


@dataclasses.dataclass
class _ListValuesOp:
    op: abc.Callable[[typing.Any, typing.Any], bool]

    def __call__(self, value: typing.Any, ref: typing.Any) -> tuple[typing.Any, bool]:
        return value, self.op(value, ref)


@dataclasses.dataclass(repr=False)
class _ListValuesFilter(Filter[ListValuesFilterResult]):
    def _eval_binary_op(
        self,
        op: BinaryOperatorT[ListValuesFilterResult],
        attribute_name: str,
        attribute_values: list[typing.Any],
        ref_values_expanded: list[typing.Any],
    ) -> ListValuesFilterResult:
        obj = super()._eval_binary_op(
            op, attribute_name, attribute_values, ref_values_expanded
        )
        # NOTE(sileht): needed for mypy other it think isinstance will always return False)
        inner_op = typing.cast(_ListValuesOp, op[0])
        if isinstance(inner_op, _ListValuesOp) and inner_op.op is operator.eq:
            if ref_values_expanded[0] not in obj.values:
                obj.values.append(ref_values_expanded[0])
        return obj


def ListValuesFilter(
    tree: TreeT | CompiledTreeT[GetAttrObject, ListValuesFilterResult],
) -> "_ListValuesFilter":
    return _ListValuesFilter(
        tree,
        {
            "-": ListValuesFilterResult.negate,
            "not": ListValuesFilterResult.negate,
        },
        {
            "=": (  # type: ignore[dict-item]
                _ListValuesOp(operator.eq),
                ListValuesFilterResult.from_iterable,
                _identity,
            ),
            "!=": (  # type: ignore[dict-item]
                _ListValuesOp(operator.ne),
                ListValuesFilterResult.from_iterable,
                _identity,
            ),
            "~=": (  # type: ignore[dict-item]
                _ListValuesOp(lambda a, b: a is not None and b.search(a)),
                ListValuesFilterResult.from_iterable,
                re.compile,
            ),
            "<": (  # type: ignore[dict-item]
                _ListValuesOp(lambda a, b: a is not None and a < b),
                ListValuesFilterResult.from_iterable,
                _identity,
            ),
            ">": (  # type: ignore[dict-item]
                _ListValuesOp(lambda a, b: a is not None and a > b),
                ListValuesFilterResult.from_iterable,
                _identity,
            ),
            "<=": (  # type: ignore[dict-item]
                _ListValuesOp(lambda a, b: a == b or (a is not None and a <= b)),
                ListValuesFilterResult.from_iterable,
                _identity,
            ),
            ">=": (  # type: ignore[dict-item]
                _ListValuesOp(lambda a, b: a == b or (a is not None and a >= b)),
                ListValuesFilterResult.from_iterable,
                _identity,
            ),
        },
        {
            # TODO(charly): how should they behave? For now, this filter is not
            # used with RuleConditionGroup. Could it eventually perform an
            # union/intersection?
            # "or": _identity,
            # "and": _identity,
        },
    )


def _minimal_datetime(dts: abc.Iterable[object]) -> datetime.datetime:
    _dts = list(typing.cast(list[datetime.datetime], Filter._to_list(dts)))
    if len(_dts) == 0:
        return date.DT_MAX
    else:
        return min(_dts)


def _as_datetime(value: typing.Any) -> datetime.datetime:
    if isinstance(value, datetime.datetime):
        return value
    elif isinstance(value, date.RelativeDatetime):
        return value.value
    elif isinstance(value, datetime.timedelta):
        dt = date.utcnow()
        return dt + value
    else:
        return date.DT_MAX


def _dt_max(value: typing.Any, ref: typing.Any) -> datetime.datetime:
    return date.DT_MAX


def _dt_identity_max(value: typing.Any) -> datetime.datetime:
    return date.DT_MAX


def _dt_in_future(value: datetime.datetime) -> datetime.datetime:
    if value < date.utcnow():
        return date.DT_MAX
    return value


def _dt_op(
    op: abc.Callable[[typing.Any, typing.Any], bool],
) -> abc.Callable[[typing.Any, typing.Any], datetime.datetime]:
    def _operator(value: typing.Any, ref: typing.Any) -> datetime.datetime:
        if value is None:
            return date.DT_MAX
        try:
            dt_value = _as_datetime(value).astimezone(datetime.timezone.utc)

            if isinstance(ref, date.Schedule):
                return ref.get_next_datetime(dt_value)

            dt_ref = _as_datetime(ref).astimezone(datetime.timezone.utc)

            handle_equality = op in (
                operator.eq,
                operator.ne,
                operator.le,
                operator.ge,
            )

            if handle_equality and dt_value == dt_ref:
                # NOTE(sileht): The condition will change...
                if isinstance(ref, date.RelativeDatetime):
                    return date.utcnow() + datetime.timedelta(minutes=1)

                return _dt_in_future(dt_ref + datetime.timedelta(minutes=1))
            elif isinstance(ref, date.RelativeDatetime):
                return _dt_in_future(dt_value + (date.utcnow() - dt_ref))
            elif dt_value < dt_ref:
                return _dt_in_future(dt_ref)
            else:
                return date.DT_MAX
        except OverflowError:
            return date.DT_MAX

    return _operator


def NearDatetimeFilter(
    tree: TreeT | CompiledTreeT[GetAttrObject, datetime.datetime],
) -> "Filter[datetime.datetime]":
    """
    The principles:
    * the attribute can't be mapped to a datetime -> datetime.datetime.max
    * the time/datetime attribute can't change in the future -> datetime.datetime.max
    * the time/datetime attribute can change in the future -> return when
    * we have a list of time/datetime, we pick the more recent
    """
    return Filter[datetime.datetime](
        tree,
        {
            "not": _minimal_datetime,
            # NOTE(sileht): This is not allowed in parser on all time-based attributes
            # so we can just return DT_MAX for all other attributes
            "-": _dt_identity_max,
        },
        {
            "=": (_dt_op(operator.eq), _minimal_datetime, _identity),
            "<": (_dt_op(operator.lt), _minimal_datetime, _identity),
            ">": (_dt_op(operator.gt), _minimal_datetime, _identity),
            "<=": (_dt_op(operator.le), _minimal_datetime, _identity),
            ">=": (_dt_op(operator.ge), _minimal_datetime, _identity),
            "!=": (_dt_op(operator.ne), _minimal_datetime, _identity),
            # NOTE(sileht): This is not allowed in parser on all time based attributes
            # so we can just return DT_MAX for all other attributes
            "~=": (_dt_max, _minimal_datetime, re.compile),
        },
        {
            "or": _minimal_datetime,
            "and": _minimal_datetime,
        },
    )


# NOTE(sileht): Sentinel object (eg: `marker = object()`) can't be expressed
# with typing yet use the proposed workaround instead:
#   https://github.com/python/typing/issues/689
#   https://www.python.org/dev/peps/pep-0661/
class IncompleteMarkerType(enum.Enum):
    _MARKER = 0


IncompleteCheck: typing.Final = IncompleteMarkerType._MARKER


IncompleteChecksResult = bool | IncompleteMarkerType


def IncompleteChecksAll(
    values: abc.Iterable[object],
) -> IncompleteChecksResult:
    values = typing.cast(abc.Iterable[IncompleteChecksResult], values)
    found_unknown = False
    for v in values:
        if v is False:
            return False
        elif v is IncompleteCheck:
            found_unknown = True
    if found_unknown:
        return IncompleteCheck
    return True


def IncompleteChecksAny(
    values: abc.Iterable[object],
) -> IncompleteChecksResult:
    values = typing.cast(abc.Iterable[IncompleteChecksResult], values)
    found_true = False
    for v in values:
        if v is IncompleteCheck:
            return IncompleteCheck
        elif v:
            found_true = True
    return found_true


def IncompleteChecksNegate(value: IncompleteChecksResult) -> IncompleteChecksResult:
    if value is IncompleteCheck:
        return IncompleteCheck
    else:
        return not value


def cast_ret_to_incomplete_check_result(
    op: abc.Callable[[typing.Any, typing.Any], bool]
) -> abc.Callable[[typing.Any, typing.Any], IncompleteChecksResult]:
    return typing.cast(
        abc.Callable[[typing.Any, typing.Any], IncompleteChecksResult], op
    )


@dataclasses.dataclass(repr=False)
class IncompleteChecksFilter(Filter[IncompleteChecksResult]):
    tree: TreeT | CompiledTreeT[GetAttrObject, bool]
    unary_operators: dict[
        str, UnaryOperatorT[IncompleteChecksResult]
    ] = dataclasses.field(
        default_factory=lambda: {
            "-": IncompleteChecksNegate,
            "not": IncompleteChecksNegate,
        }
    )
    binary_operators: dict[
        str, BinaryOperatorT[IncompleteChecksResult]
    ] = dataclasses.field(
        default_factory=lambda: {
            "=": (
                cast_ret_to_incomplete_check_result(operator.eq),
                IncompleteChecksAny,
                _identity,
            ),
            "<": (
                cast_ret_to_incomplete_check_result(operator.lt),
                IncompleteChecksAny,
                _identity,
            ),
            ">": (
                cast_ret_to_incomplete_check_result(operator.gt),
                IncompleteChecksAny,
                _identity,
            ),
            "<=": (
                cast_ret_to_incomplete_check_result(operator.le),
                IncompleteChecksAny,
                _identity,
            ),
            ">=": (
                cast_ret_to_incomplete_check_result(operator.ge),
                IncompleteChecksAny,
                _identity,
            ),
            "!=": (
                cast_ret_to_incomplete_check_result(operator.ne),
                IncompleteChecksAll,
                _identity,
            ),
            "~=": (
                cast_ret_to_incomplete_check_result(
                    lambda a, b: a is not None and b.search(a)
                ),
                IncompleteChecksAny,
                re.compile,
            ),
        }
    )
    multiple_operators: dict[
        str, MultipleOperatorT[IncompleteChecksResult]
    ] = dataclasses.field(
        default_factory=lambda: {
            "or": IncompleteChecksAny,
            "and": IncompleteChecksAll,
        }
    )
    pending_checks: list[str] = dataclasses.field(default_factory=list)
    all_checks: list[str] = dataclasses.field(default_factory=list)

    def _eval_binary_op(
        self,
        op: BinaryOperatorT[IncompleteChecksResult],
        attribute_name: str,
        attribute_values: list[typing.Any],
        ref_values_expanded: list[typing.Any],
    ) -> IncompleteChecksResult:
        if not self.is_complete(op, attribute_name, ref_values_expanded):
            return IncompleteCheck

        return super()._eval_binary_op(
            op, attribute_name, attribute_values, ref_values_expanded
        )

    def is_complete(
        self,
        op: BinaryOperatorT[FilterResultT],
        attribute_name: str,
        ref_values: list[typing.Any],
    ) -> bool:
        binary_op, iterable_op, _ = op

        if attribute_name.startswith(Filter.LENGTH_OPERATOR):
            real_attr_name = attribute_name[1:]
        else:
            real_attr_name = attribute_name

        if real_attr_name.startswith("check-") or real_attr_name.startswith("status-"):
            if attribute_name.startswith(Filter.LENGTH_OPERATOR):
                if len(self.pending_checks) != 0:
                    return False
            else:
                if real_attr_name in (
                    "check-pending",
                    "check-success-or-neutral-or-pending",
                ):
                    return not bool(self.pending_checks)

                final_checks = set(self.all_checks) - set(self.pending_checks)
                final_check = iterable_op(
                    binary_op(check, ref_value)
                    for check in final_checks
                    for ref_value in ref_values
                )
                if final_check:
                    return True

                # Ensure the check we are waiting for is somewhere
                at_least_one_check = any(
                    binary_op(check, ref_value)
                    for check in self.all_checks
                    for ref_value in ref_values
                )
                if not at_least_one_check:
                    return False

                if self.pending_checks:
                    # Ensure the check we are waiting for is not in pending list
                    pending_result = iterable_op(
                        binary_op(check, ref_value)
                        for check in self.pending_checks
                        for ref_value in ref_values
                    )
                    if pending_result:
                        return False
        return True


@dataclasses.dataclass
class JinjaTemplateWrapper:
    env: jinja2.Environment
    template: str
    used_variables: set[str]
    compile_func: ValueCompilerT = _identity

    def set_compile_func(self, compile_func: ValueCompilerT) -> None:
        self.compile_func = compile_func

    async def render_async(self, obj: GetAttrObjectT) -> typing.Any:
        infos = {}
        for k in sorted(self.used_variables):
            infos[k] = await getattr(obj, k)

        return self.compile_func(
            await self.env.from_string(self.template).render_async(**infos)
        )
