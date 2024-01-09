import dataclasses
import datetime
import typing
import zoneinfo

import jinja2
import jinja2.sandbox
import pytest

from mergify_engine import condition_value_querier
from mergify_engine import date
from mergify_engine.rules import filter
from mergify_engine.rules import parser
from mergify_engine.tests.tardis import time_travel


class FakePR(dict):  # type: ignore[type-arg]
    def get_attribute_value(self, k: typing.Any) -> typing.Any:
        try:
            return self[k]
        except KeyError:
            raise condition_value_querier.PullRequestAttributeError(name=k)


class AsyncFakePR:
    def __init__(self, data: dict[str, typing.Any]) -> None:
        self.data = data

    async def get_attribute_value(self, k: typing.Any) -> typing.Any:
        try:
            return self.data[k]
        except KeyError:
            raise condition_value_querier.PullRequestAttributeError(name=k)

    async def items(self) -> dict[str, typing.Any]:
        return self.data


async def test_binary() -> None:
    f = filter.BinaryFilter({"=": ("foo", 1)})
    assert await f(FakePR({"foo": 1}))
    assert not await f(FakePR({"foo": 2}))


async def test_string() -> None:
    f = filter.BinaryFilter({"=": ("foo", "bar")})
    assert await f(FakePR({"foo": "bar"}))
    assert not await f(FakePR({"foo": 2}))


async def test_not() -> None:
    f = filter.BinaryFilter({"-": {"=": ("foo", 1)}})
    assert not await f(FakePR({"foo": 1}))
    assert await f(FakePR({"foo": 2}))


async def test_len() -> None:
    f = filter.BinaryFilter({"=": ("#foo", 3)})
    assert await f(FakePR({"foo": "bar"}))
    with pytest.raises(filter.InvalidOperatorError):
        await f(FakePR({"foo": 2}))
    assert not await f(FakePR({"foo": "a"}))
    assert not await f(FakePR({"foo": "abcedf"}))
    assert await f(FakePR({"foo": [10, 20, 30]}))
    assert not await f(FakePR({"foo": [10, 20]}))
    assert not await f(FakePR({"foo": [10, 20, 40, 50]}))
    f = filter.BinaryFilter({">": ("#foo", 3)})
    assert await f(FakePR({"foo": "barz"}))
    with pytest.raises(filter.InvalidOperatorError):
        await f(FakePR({"foo": 2}))
    assert not await f(FakePR({"foo": "a"}))
    assert await f(FakePR({"foo": "abcedf"}))
    assert await f(FakePR({"foo": [10, "abc", 20, 30]}))
    assert not await f(FakePR({"foo": [10, 20]}))
    assert not await f(FakePR({"foo": []}))


@dataclasses.dataclass
class Obj:
    foo: str
    bar: str
    __string_like__ = True

    def __str__(self) -> str:
        return f"{self.foo}a{self.bar}"


async def test_object() -> None:
    f = filter.BinaryFilter({"=": ("foo", "bar")})
    assert await f(FakePR({"foo": Obj("b", "r")}))
    assert not await f(FakePR({"foo": Obj("o", "k")}))

    assert await f(FakePR({"foo": [Obj("b", "r")]}))
    assert not await f(FakePR({"foo": [Obj("o", "k")]}))

    f = filter.BinaryFilter({"~=": ("foo", "^bar")})
    assert await f(FakePR({"foo": Obj("b", "r")}))
    assert not await f(FakePR({"foo": Obj("o", "k")}))

    assert await f(FakePR({"foo": [Obj("b", "r")]}))
    assert not await f(FakePR({"foo": [Obj("o", "k")]}))


async def test_optimized_len() -> None:
    f = filter.BinaryFilter({"=": ("#foo", 3)})
    assert await f(FakePR({"foo": "bar"}))
    with pytest.raises(filter.InvalidOperatorError):
        await f(FakePR({"foo": 2}))
    assert not await f(FakePR({"foo": "a"}))
    assert await f(FakePR({"#foo": 3, "foo": "a"}))
    assert not await f(FakePR({"#foo": 2, "foo": "abc"}))


async def test_regexp() -> None:
    f = filter.BinaryFilter({"~=": ("foo", "^f")})
    assert await f(FakePR({"foo": "foobar"}))
    assert await f(FakePR({"foo": "foobaz"}))
    assert not await f(FakePR({"foo": "x"}))
    assert not await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({"~=": ("foo", "^$")})
    assert await f(FakePR({"foo": ""}))
    assert not await f(FakePR({"foo": "x"}))

    f = filter.BinaryFilter({"~=": ("foo", "(?i)FoObAr")})
    assert await f(FakePR({"foo": "foobar"}))
    assert await f(FakePR({"foo": "FOOBAR"}))


async def test_regexp_invalid() -> None:
    with pytest.raises(filter.InvalidArgumentsError):
        filter.BinaryFilter({"~=": ("foo", r"([^\s\w])(\s*\1+")})


async def test_set_value_expanders() -> None:
    f = filter.BinaryFilter(
        {"=": ("foo", "@bar")},
    )
    f.value_expanders["foo"] = lambda x: [x.replace("@", "foo")]
    assert await f(FakePR({"foo": "foobar"}))
    assert not await f(FakePR({"foo": "x"}))


async def test_set_value_expanders_unset_at_init() -> None:
    f = filter.BinaryFilter({"=": ("foo", "@bar")})
    f.value_expanders = {"foo": lambda x: [x.replace("@", "foo")]}
    assert await f(FakePR({"foo": "foobar"}))
    assert not await f(FakePR({"foo": "x"}))


async def test_does_not_contain() -> None:
    f = filter.BinaryFilter({"!=": ("foo", 1)})
    assert await f(FakePR({"foo": []}))
    assert await f(FakePR({"foo": [2, 3]}))
    assert not await f(FakePR({"foo": (1, 2)}))


async def test_set_value_expanders_does_not_contain() -> None:
    f = filter.BinaryFilter({"!=": ("foo", "@bar")})
    f.value_expanders["foo"] = lambda _: ["foobaz", "foobar"]
    assert not await f(FakePR({"foo": "foobar"}))
    assert not await f(FakePR({"foo": "foobaz"}))
    assert await f(FakePR({"foo": "foobiz"}))


async def test_contains() -> None:
    f = filter.BinaryFilter({"=": ("foo", 1)})
    assert await f(FakePR({"foo": [1, 2]}))
    assert not await f(FakePR({"foo": [2, 3]}))
    assert await f(FakePR({"foo": (1, 2)}))
    f = filter.BinaryFilter({">": ("foo", 2)})
    assert not await f(FakePR({"foo": [1, 2]}))
    assert await f(FakePR({"foo": [2, 3]}))


async def test_none() -> None:
    f = filter.BinaryFilter({"=": ("foo", 1)})
    assert not await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({"=": ("foo", None)})
    assert await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({"!=": ("foo", 1)})
    assert await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({">=": ("foo", 1)})
    assert not await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({"<=": ("foo", 1)})
    assert not await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({"<": ("foo", 1)})
    assert not await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({">": ("foo", 1)})
    assert not await f(FakePR({"foo": None}))

    f = filter.BinaryFilter({"~=": ("foo", "^foo")})
    assert not await f(FakePR({"foo": None}))


async def test_unknown_attribute() -> None:
    f = filter.BinaryFilter({"=": ("foo", 1)})
    with pytest.raises(filter.UnknownAttributeError):
        await f(FakePR({"bar": 1}))


async def test_parse_error() -> None:
    with pytest.raises(filter.ParseError):
        filter.BinaryFilter({})


async def test_unknown_operator() -> None:
    with pytest.raises(filter.UnknownOperatorError):
        filter.BinaryFilter({"oops": (1, 2)})  # type: ignore[arg-type]


async def test_invalid_arguments() -> None:
    with pytest.raises(filter.InvalidArgumentsError):
        filter.BinaryFilter({"=": (1, 2, 3)})  # type: ignore[typeddict-item]


@pytest.mark.parametrize(
    "filter_",
    (filter.BinaryFilter, filter.NearDatetimeFilter, filter.IncompleteChecksFilter),
)
async def test_str(filter_: filter.Filter[typing.Any]) -> None:
    assert str(filter_({"~=": ("foo", "^f")})) == "foo~=^f"  # type: ignore[type-var]
    assert str(filter_({"-": {"=": ("foo", 1)}})) == "-foo=1"  # type: ignore[type-var]
    assert str(filter_({"=": ("foo", True)})) == "foo"  # type: ignore[type-var]
    assert str(filter_({"=": ("bar", False)})) == "-bar"  # type: ignore[type-var]
    with pytest.raises(filter.InvalidOperatorError):
        str(filter_({">=": ("bar", False)}))  # type: ignore[type-var]


def dtime(day: int) -> datetime.datetime:
    return date.utcnow().replace(day=day)


@time_travel("2012-01-14")
async def test_datetime_binary() -> None:
    assert (
        str(
            filter.BinaryFilter({">=": ("foo", dtime(5))}),
        )
        == "foo>=2012-01-05T00:00:00"
    )
    assert (
        str(
            filter.BinaryFilter({"<=": ("foo", dtime(5).replace(hour=23, minute=59))}),
        )
        == "foo<=2012-01-05T23:59:00"
    )
    assert (
        str(
            filter.BinaryFilter({"<=": ("foo", dtime(5).replace(hour=3, minute=9))}),
        )
        == "foo<=2012-01-05T03:09:00"
    )

    f = filter.BinaryFilter({"<=": ("foo", date.utcnow())})
    assert await f(FakePR({"foo": dtime(14)}))
    assert await f(FakePR({"foo": dtime(2)}))
    assert await f(FakePR({"foo": dtime(5)}))
    assert not await f(FakePR({"foo": dtime(18)}))
    assert not await f(FakePR({"foo": dtime(23)}))

    f = filter.BinaryFilter({">=": ("foo", date.utcnow())})
    assert await f(FakePR({"foo": dtime(14)}))
    assert not await f(FakePR({"foo": dtime(2)}))
    assert not await f(FakePR({"foo": dtime(5)}))
    assert await f(FakePR({"foo": dtime(18)}))
    assert await f(FakePR({"foo": dtime(23)}))


@time_travel("2012-01-14")
async def test_time_binary() -> None:
    assert (
        str(
            filter.BinaryFilter({">=": ("foo", date.Time(0, 0, date.UTC))}),
        )
        == "foo>=00:00"
    )
    assert (
        str(
            filter.BinaryFilter({"<=": ("foo", date.Time(23, 59, date.UTC))}),
        )
        == "foo<=23:59"
    )
    assert (
        str(
            filter.BinaryFilter({"<=": ("foo", date.Time(3, 9, date.UTC))}),
        )
        == "foo<=03:09"
    )
    assert (
        str(
            filter.BinaryFilter(
                {"<=": ("foo", date.Time(3, 9, zoneinfo.ZoneInfo("Europe/Paris")))},
            ),
        )
        == "foo<=03:09[Europe/Paris]"
    )

    now = date.utcnow()

    f = filter.BinaryFilter({"<=": ("foo", date.Time(5, 8, date.UTC))})
    assert await f(FakePR({"foo": now.replace(hour=5, minute=8)}))
    assert await f(FakePR({"foo": now.replace(hour=2, minute=1)}))
    assert await f(FakePR({"foo": now.replace(hour=5, minute=1)}))
    assert not await f(FakePR({"foo": now.replace(hour=6, minute=2)}))
    assert not await f(FakePR({"foo": now.replace(hour=8, minute=9)}))

    f = filter.BinaryFilter({">=": ("foo", date.Time(5, 8, date.UTC))})
    assert await f(FakePR({"foo": now.replace(hour=5, minute=8)}))
    assert not await f(FakePR({"foo": now.replace(hour=2, minute=1)}))
    assert not await f(FakePR({"foo": now.replace(hour=5, minute=1)}))
    assert await f(FakePR({"foo": now.replace(hour=6, minute=2)}))
    assert await f(FakePR({"foo": now.replace(hour=8, minute=9)}))

    f = filter.BinaryFilter(
        {">=": ("foo", date.Time(5, 8, zoneinfo.ZoneInfo("Europe/Paris")))},
    )
    assert await f(FakePR({"foo": now.replace(hour=4, minute=8)}))
    assert not await f(FakePR({"foo": now.replace(hour=1, minute=1)}))
    assert not await f(FakePR({"foo": now.replace(hour=4, minute=1)}))
    assert await f(FakePR({"foo": now.replace(hour=5, minute=2)}))
    assert await f(FakePR({"foo": now.replace(hour=7, minute=9)}))


@time_travel("2012-01-14T05:08:00")
async def test_datetime_near_datetime() -> None:
    # Condition on past date
    for op in ("<=", ">=", ">", "<", "=", "!="):
        f = filter.NearDatetimeFilter(
            typing.cast(filter.TreeT, {op: ("updated-at", dtime(8))}),
        )
        assert await f(FakePR({"updated-at": dtime(2)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(10)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(14)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(18)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(8)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": None})) == date.DT_MAX

    # Condition on future date with equality
    for op in ("<=", ">=", "=", "!="):
        f = filter.NearDatetimeFilter(
            typing.cast(filter.TreeT, {op: ("updated-at", dtime(16))}),
        )
        assert await f(FakePR({"updated-at": dtime(2)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(10)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(14)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(18)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(16)})) == dtime(16).replace(
            minute=9,
        ), str(f)
        assert await f(FakePR({"updated-at": None})) == date.DT_MAX

    # Condition on future date without equality
    for op in ("<", ">"):
        f = filter.NearDatetimeFilter(
            typing.cast(filter.TreeT, {op: ("updated-at", dtime(16))}),
        )
        assert await f(FakePR({"updated-at": dtime(2)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(10)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(14)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(18)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(16)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": None})) == date.DT_MAX


def rdtime(day: int) -> date.RelativeDatetime:
    return date.RelativeDatetime(dtime(day))


@time_travel("2012-01-14T05:08:00")
async def test_relative_datetime_binary() -> None:
    tree = parser.parse("updated-at<2 days ago")
    f = filter.BinaryFilter(tree)
    assert await f(FakePR({"updated-at-relative": rdtime(2)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(10)})), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(12)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(14)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(16)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(18)}))), tree

    tree = parser.parse("updated-at<=2 days ago")
    f = filter.BinaryFilter(tree)
    assert await f(FakePR({"updated-at-relative": rdtime(2)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(10)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(12)})), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(14)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(16)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(18)}))), tree

    tree = parser.parse("updated-at>2 days ago")
    f = filter.BinaryFilter(tree)
    assert not (await f(FakePR({"updated-at-relative": rdtime(2)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(10)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(12)}))), tree
    assert await f(FakePR({"updated-at-relative": rdtime(14)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(16)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(18)})), tree

    tree = parser.parse("updated-at>=2 days ago")
    f = filter.BinaryFilter(tree)
    assert not (await f(FakePR({"updated-at-relative": rdtime(2)}))), tree
    assert not (await f(FakePR({"updated-at-relative": rdtime(10)}))), tree
    assert await f(FakePR({"updated-at-relative": rdtime(12)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(14)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(16)})), tree
    assert await f(FakePR({"updated-at-relative": rdtime(18)})), tree


async def test_relative_datetime_neardatetime_filter() -> None:
    with time_travel("2012-01-10T05:08:00Z") as frozen_time:
        tzinfo = date.UTC
        day10 = frozen_time().replace(tzinfo=tzinfo)
        day14 = day10 + datetime.timedelta(days=4)
        day18 = day14 + datetime.timedelta(days=4)

        frozen_time.move_to(day10)
        tree = parser.parse("updated-at<2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)
        assert await f(FakePR({"updated-at-relative": None})) == date.DT_MAX

        frozen_time.move_to(day14)
        tree = parser.parse("updated-at<2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)

        frozen_time.move_to(day18)
        tree = parser.parse("updated-at<2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == date.DT_MAX

        frozen_time.move_to(day10)
        tree = parser.parse("updated-at>2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)

        frozen_time.move_to(day14)
        tree = parser.parse("updated-at>2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)

        frozen_time.move_to(day18)
        tree = parser.parse("updated-at>2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == date.DT_MAX

        frozen_time.move_to(day10)
        tree = parser.parse("updated-at<=2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)

        frozen_time.move_to(day14)
        tree = parser.parse("updated-at<=2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)

        frozen_time.move_to(day18)
        tree = parser.parse("updated-at<=2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == date.DT_MAX

        frozen_time.move_to(day10)
        tree = parser.parse("updated-at>=2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)

        frozen_time.move_to(day14)
        tree = parser.parse("updated-at>=2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == dtime(16)

        frozen_time.move_to(day18)
        tree = parser.parse("updated-at>=2 days ago")
        f = filter.NearDatetimeFilter(tree)
        assert await f(FakePR({"updated-at-relative": rdtime(14)})) == date.DT_MAX


async def test_or() -> None:
    f = filter.BinaryFilter({"or": ({"=": ("foo", 1)}, {"=": ("bar", 1)})})
    assert await f(FakePR({"foo": 1, "bar": 1}))
    assert not await f(FakePR({"bar": 2, "foo": 2}))
    assert await f(FakePR({"bar": 2, "foo": 1}))
    assert await f(FakePR({"bar": 1, "foo": 2}))


async def test_and() -> None:
    f = filter.BinaryFilter({"and": ({"=": ("foo", 1)}, {"=": ("bar", 1)})})
    assert await f(FakePR({"bar": 1, "foo": 1}))
    assert not await f(FakePR({"bar": 2, "foo": 2}))
    assert not await f(FakePR({"bar": 2, "foo": 1}))
    assert not await f(FakePR({"bar": 1, "foo": 2}))
    with pytest.raises(filter.ParseError):
        filter.BinaryFilter({"or": {"foo": "whar"}})  # type: ignore[dict-item]


async def test_not_tree() -> None:
    f1 = filter.TreeT({"=": ("foo", 1)})
    f2 = filter.TreeT({"=": ("bar", 1)})

    f = filter.BinaryFilter({"not": {"or": (f1, f2)}})
    assert not await f(FakePR({"foo": 1, "bar": 1}))
    assert await f(FakePR({"bar": 2, "foo": 2}))
    assert not await f(FakePR({"bar": 2, "foo": 1}))
    assert not await f(FakePR({"bar": 1, "foo": 2}))

    f = filter.BinaryFilter({"not": {"and": (f1, f2)}})
    assert not await f(FakePR({"bar": 1, "foo": 1}))
    assert await f(FakePR({"bar": 2, "foo": 2}))
    assert await f(FakePR({"bar": 2, "foo": 1}))
    assert await f(FakePR({"bar": 1, "foo": 2}))


async def test_chain() -> None:
    f1 = filter.TreeT({"=": ("bar", 1)})
    f2 = filter.TreeT({"=": ("foo", 1)})
    f = filter.BinaryFilter({"and": (f1, f2)})
    assert await f(FakePR({"bar": 1, "foo": 1}))
    assert not await f(FakePR({"bar": 2, "foo": 2}))
    assert not await f(FakePR({"bar": 2, "foo": 1}))
    assert not await f(FakePR({"bar": 1, "foo": 2}))


async def test_parser_leaf() -> None:
    for string in ("head=foobar", "-base=main", "#files>3"):
        tree = parser.parse(string)
        assert string == str(filter.BinaryFilter(tree))


async def test_parser_group() -> None:
    string = str(
        filter.BinaryFilter({"and": ({"=": ("head", "foobar")}, {">": ("#files", 3)})}),
    )
    assert string == "(head=foobar and #files>3)"

    string = str(
        filter.BinaryFilter(
            {"not": {"and": ({"=": ("head", "foobar")}, {">": ("#files", 3)})}},
        ),
    )
    assert string == "not(head=foobar and #files>3)"


def get_scheduled_pr() -> FakePR:
    return FakePR(
        {
            "number": 3433,
            "current-datetime": date.utcnow(),
        },
    )


async def test_schedule_with_timezone() -> None:
    with time_travel("2021-10-19T22:01:31.610725Z") as frozen_time:
        tree = parser.parse("schedule=Mon-Fri 09:00-17:30[Europe/Paris]")
        f = filter.NearDatetimeFilter(tree)

        today = frozen_time().replace(tzinfo=date.UTC)

        next_refreshes = [
            "2021-10-20T07:00:00",
            "2021-10-20T15:30:01",
            "2021-10-21T07:00:00",
        ]
        for next_refresh in next_refreshes:
            next_refresh_dt = datetime.datetime.fromisoformat(next_refresh).replace(
                tzinfo=date.UTC,
            )
            assert await f(get_scheduled_pr()) == next_refresh_dt
            frozen_time.move_to(next_refresh_dt)

        frozen_time.move_to(today.replace(day=23))
        assert await f(get_scheduled_pr()) == datetime.datetime(
            2021,
            10,
            25,
            7,
            tzinfo=date.UTC,
        )

    with time_travel("2022-09-16T10:38:18.019100Z") as frozen_time:
        tree = parser.parse("schedule=Mon-Fri 06:00-17:00[America/Los_Angeles]")
        f = filter.NearDatetimeFilter(tree)

        today = frozen_time().replace(tzinfo=date.UTC)

        next_refreshes = [
            "2022-09-16T13:00:00",
        ]
        for next_refresh in next_refreshes:
            next_refresh_dt = datetime.datetime.fromisoformat(next_refresh).replace(
                tzinfo=date.UTC,
            )
            assert await f(get_scheduled_pr()) == next_refresh_dt
            frozen_time.move_to(next_refresh_dt)


async def test_current_datetime() -> None:
    with time_travel("2023-07-13"):
        tree = parser.parse("current-datetime<=2023-07-13T14:00[Europe/Paris]")
        f = filter.NearDatetimeFilter(tree)

        next_refresh_at = datetime.datetime.fromisoformat("2023-07-13T14:00").replace(
            tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
        )

        assert await f(get_scheduled_pr()) == next_refresh_at

    with time_travel("2023-07-14"):
        assert await f(get_scheduled_pr()) == date.DT_MAX


@pytest.mark.parametrize(
    "condition",
    (
        "current-datetime=2023-07-13T14:00/2023-07-13T16:00[Europe/Paris]",
        "current-datetime!=2023-07-13T14:00/2023-07-13T16:00[Europe/Paris]",
    ),
)
async def test_current_datetime_range(condition: str) -> None:
    tree = parser.parse(condition)
    f = filter.NearDatetimeFilter(tree)

    with time_travel("2023-07-13T13:00+02"):
        next_refresh_at = datetime.datetime.fromisoformat("2023-07-13T14:00").replace(
            tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
        )
        assert await f(get_scheduled_pr()) == next_refresh_at

    with time_travel("2023-07-13T15:00+02"):
        next_refresh_at = datetime.datetime.fromisoformat("2023-07-13T16:01").replace(
            tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
        )
        assert await f(get_scheduled_pr()) == next_refresh_at

    with time_travel("2023-07-13T17:00+02"):
        assert await f(get_scheduled_pr()) == date.DT_MAX


@pytest.mark.parametrize(
    (
        "condition",
        "time_travel_before_range",
        "time_travel_inside_range",
        "time_travel_after_range",
        "next_refresh_before_range",
        "next_refresh_inside_range",
        "next_refresh_after_range",
    ),
    (
        (
            "current-datetime=XXXX-07-14T00:00/XXXX-07-14T23:59[Europe/Paris]",
            "2023-07-13",  # freeze time before range
            "2023-07-14T10:00+02:00",  # freeze time inside range
            "2023-07-15",  # freeze time after range
            datetime.datetime(  # next refresh before range
                2023,
                7,
                14,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh inside range
                2023,
                7,
                15,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh after range
                2024,
                7,
                14,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
        ),
        (
            "current-datetime=2023-XX-14T00:00/2023-XX-14T23:59[Europe/Paris]",
            "2023-07-13",  # freeze time before range
            "2023-07-14T10:00+02:00",  # freeze time inside range
            "2023-07-15",  # freeze time after range
            datetime.datetime(  # next refresh before range
                2023,
                7,
                14,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh inside range
                2023,
                7,
                15,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh after range
                2023,
                8,
                14,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
        ),
        (
            "current-datetime=2023-XX-31T00:00/2023-XX-31T23:59[Europe/Paris]",
            "2023-01-29",  # freeze time before range
            "2023-01-31T10:00+02:00",  # freeze time inside range
            "2023-02-02",  # freeze time after range
            datetime.datetime(  # next refresh before range
                2023,
                1,
                31,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh inside range
                2023,
                2,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh after range
                2023,
                3,
                31,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
        ),
        (
            "current-datetime=XXXX-07-XXT00:00/XXXX-07-XXT23:59[Europe/Paris]",
            "2023-06-10",  # freeze time before range
            "2023-07-10",  # freeze time inside range
            "2023-08-01",  # freeze time after range
            datetime.datetime(  # next refresh before range
                2023,
                7,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh inside range
                2023,
                8,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh after range
                2024,
                7,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
        ),
        (
            "current-datetime=XXXX-07-XXT08:00/XXXX-07-XXT15:00[Europe/Paris]",
            "2023-06-10",  # freeze time before range
            "2023-07-10T10:00+02",  # freeze time inside range
            "2023-08-01",  # freeze time after range
            datetime.datetime(  # next refresh before range
                2023,
                7,
                1,
                8,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh inside range
                2023,
                7,
                10,
                15,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh after range
                2024,
                7,
                1,
                8,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
        ),
        (
            "current-datetime=2023-07-XXT00:00/2023-07-XXT23:59[Europe/Paris]",
            "2023-06-10",  # freeze time before range
            "2023-07-10",  # freeze time inside range
            "2023-08-01",  # freeze time after range
            datetime.datetime(  # next refresh before range
                2023,
                7,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh inside range
                2023,
                8,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh after range
                2024,
                7,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
        ),
        (
            "current-datetime=XXXX-XX-31T00:00/XXXX-XX-31T23:59[Europe/Paris]",
            "2023-01-29",  # freeze time before range
            "2023-01-31T10:00+02:00",  # freeze time inside range
            "2023-02-02",  # freeze time after range
            datetime.datetime(  # next refresh before range
                2023,
                1,
                31,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh inside range
                2023,
                2,
                1,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
            datetime.datetime(  # next refresh after range
                2023,
                3,
                31,
                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
            ),
        ),
    ),
)
async def test_current_datetime_range_uncertain_date(
    condition: str,
    time_travel_before_range: str,
    time_travel_inside_range: str,
    time_travel_after_range: str,
    next_refresh_before_range: datetime.datetime,
    next_refresh_inside_range: datetime.datetime,
    next_refresh_after_range: datetime.datetime,
) -> None:
    with time_travel(time_travel_before_range):
        tree = parser.parse(condition)
        f = filter.NearDatetimeFilter(tree)

        assert await f(get_scheduled_pr()) == next_refresh_before_range

    with time_travel(time_travel_inside_range):
        assert await f(get_scheduled_pr()) == next_refresh_inside_range

    with time_travel(time_travel_after_range):
        assert await f(get_scheduled_pr()) == next_refresh_after_range


async def test_text_jinja_template(
    jinja_environment: jinja2.sandbox.SandboxedEnvironment,
) -> None:
    template = filter.JinjaTemplateWrapper(jinja_environment, r"{{author}}", {"author"})
    f = filter.BinaryFilter({"=": ("sender", template)})
    assert await f(AsyncFakePR({"author": "foo", "sender": "foo"}))
    assert not await f(AsyncFakePR({"author": "foo", "sender": "bar"}))


async def test_regex_jinja_template(
    jinja_environment: jinja2.sandbox.SandboxedEnvironment,
) -> None:
    template = filter.JinjaTemplateWrapper(
        jinja_environment,
        r"{{author}} \d{6}",
        {"author"},
    )
    f = filter.BinaryFilter({"~=": ("body", template)})
    assert await f(AsyncFakePR({"author": "foo", "body": "hello foo 123456 !"}))
    assert not await f(AsyncFakePR({"author": "foo", "body": "foo"}))


async def test_schedule_neardatetime_filter() -> None:
    # Saturday
    with time_travel(datetime.datetime(2022, 11, 12, tzinfo=date.UTC)) as frozen_time:
        tree_eq = parser.parse("schedule=MON-FRI 08:00-17:00")
        tree_ne = parser.parse("schedule!=MON-FRI 08:00-17:00")
        f_eq = filter.NearDatetimeFilter(tree_eq)
        f_ne = filter.NearDatetimeFilter(tree_ne)
        # Correct datetime should be next Monday, 08:00:01 UTC
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 14, 8, tzinfo=date.UTC)

        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 14, 8, tzinfo=date.UTC)
        # Friday
        frozen_time.move_to(datetime.datetime(2022, 11, 11, tzinfo=date.UTC))
        # Correct datetime should be current day, at start_hour and start_minute of the schedule
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 8, tzinfo=date.UTC)
        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 8, tzinfo=date.UTC)

        # Friday, 15:00 UTC
        frozen_time.move_to(datetime.datetime(2022, 11, 11, 15, tzinfo=date.UTC))
        # Correct datetime should be current day, at end_hour and end_minute of the schedule + 1s
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 17, 0, 1, tzinfo=date.UTC)
        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 17, 0, 1, tzinfo=date.UTC)

        # Friday, 17:00 UTC
        frozen_time.move_to(datetime.datetime(2022, 11, 11, 17, tzinfo=date.UTC))
        # Correct datetime should be current day, 1 second after the end of the schedule
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 17, 0, 1, tzinfo=date.UTC)
        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 17, 0, 1, tzinfo=date.UTC)

        # Saturday
        frozen_time.move_to(datetime.datetime(2022, 11, 12, tzinfo=date.UTC))
        tree_eq = parser.parse("schedule=FRI-TUE 08:00-17:00")
        tree_ne = parser.parse("schedule!=FRI-TUE 08:00-17:00")
        f_eq = filter.NearDatetimeFilter(tree_eq)
        f_ne = filter.NearDatetimeFilter(tree_ne)
        # Correct datetime should be current day, 08:00:00 UTC
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 12, 8, tzinfo=date.UTC)
        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 12, 8, tzinfo=date.UTC)

        # Thursday
        frozen_time.move_to(datetime.datetime(2022, 11, 9, tzinfo=date.UTC))
        # Correct datetime should be Friday of the same week, 08:00:00 UTC
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 8, tzinfo=date.UTC)
        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 8, tzinfo=date.UTC)

        # Monday
        frozen_time.move_to(datetime.datetime(2022, 11, 7, tzinfo=date.UTC))
        # Correct datetime should be current day, at start_hour and start_minute of the schedule
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 7, 8, tzinfo=date.UTC)
        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 7, 8, tzinfo=date.UTC)

        # Tuesday, 18:00 UTC
        frozen_time.move_to(datetime.datetime(2022, 11, 8, 18, tzinfo=date.UTC))
        # Correct datetime should be Friday of the current week,
        # at start_hour and start_minute of the schedule
        assert await f_eq(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 8, tzinfo=date.UTC)
        assert await f_ne(
            FakePR({"current-datetime": date.utcnow()}),
        ) == datetime.datetime(2022, 11, 11, 8, tzinfo=date.UTC)


@pytest.mark.parametrize(
    ("tree", "pull_request", "expected_values", "expected_filtered_values"),
    [
        pytest.param(
            parser.parse("check-success=ci-1"),
            FakePR({"check-success": "ci-1"}),
            ["ci-1"],
            [],
            id='single value match with "=" operator',
        ),
        pytest.param(
            parser.parse("check-success=ci-1"),
            FakePR({"check-success": ["ci-1", "ci-2"]}),
            ["ci-1"],
            ["ci-2"],
            id='multiple value match with "=" operator',
        ),
        pytest.param(
            parser.parse("check-success!=ci-1"),
            FakePR({"check-success": ["ci-1", "ci-2"]}),
            ["ci-2"],
            ["ci-1"],
            id='multiple value match with "!=" operator',
        ),
        pytest.param(
            parser.parse("-check-success=ci-1"),
            FakePR({"check-success": ["ci-1", "ci-2"]}),
            ["ci-2"],
            ["ci-1"],
            id='multiple value match with "=" and "-" operators',
        ),
        pytest.param(
            {"not": {"=": ("check-success", "ci-1")}},
            FakePR({"check-success": ["ci-1", "ci-2"]}),
            ["ci-2"],
            ["ci-1"],
            id='multiple value match with "=" and "not" operators',
        ),
        pytest.param(
            parser.parse("check-success~=^ci"),
            FakePR({"check-success": ["ci-1", "summary"]}),
            ["ci-1"],
            ["summary"],
            id='multiple value match with "~=" operator',
        ),
        pytest.param(
            {"<": ("foo", 5)},
            FakePR({"foo": 4}),
            [4],
            [],
            id='single value match with "<" operator',
        ),
        pytest.param(
            {"<": ("foo", 5)},
            FakePR({"foo": 5}),
            [],
            [5],
            id='single value not match with "<" operator',
        ),
        pytest.param(
            {"<=": ("foo", 5)},
            FakePR({"foo": 5}),
            [5],
            [],
            id='single value match with "<=" operator',
        ),
        pytest.param(
            {"<=": ("foo", 5)},
            FakePR({"foo": 6}),
            [],
            [6],
            id='single value not match with "<=" operator',
        ),
        pytest.param(
            {">": ("foo", 5)},
            FakePR({"foo": 6}),
            [6],
            [],
            id='single value match with ">" operator',
        ),
        pytest.param(
            {">": ("foo", 5)},
            FakePR({"foo": 5}),
            [],
            [5],
            id='single value not match with ">" operator',
        ),
        pytest.param(
            {">=": ("foo", 5)},
            FakePR({"foo": 5}),
            [5],
            [],
            id='single value match with ">=" operator',
        ),
        pytest.param(
            {">=": ("foo", 5)},
            FakePR({"foo": 4}),
            [],
            [4],
            id='single value not match with ">=" operator',
        ),
    ],
)
async def test_list_values_filter(
    tree: filter.TreeT,
    pull_request: FakePR,
    expected_values: list[typing.Any],
    expected_filtered_values: list[typing.Any],
) -> None:
    filter_ = filter.ListValuesFilter(tree)

    result = await filter_(pull_request)

    assert result.values == expected_values
    assert result.filtered_values == expected_filtered_values
