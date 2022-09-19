import dataclasses
import datetime
import typing
import zoneinfo

from freezegun import freeze_time
import pytest

from mergify_engine import context
from mergify_engine import date
from mergify_engine.rules import filter
from mergify_engine.rules import parser


UTC = datetime.timezone.utc


class FakePR(dict):  # type: ignore[type-arg]
    def __getattr__(self, k: typing.Any) -> typing.Any:
        try:
            return self[k]
        except KeyError:
            raise context.PullRequestAttributeError(name=k)


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
    with pytest.raises(filter.InvalidOperator):
        await f(FakePR({"foo": 2}))
    assert not await f(FakePR({"foo": "a"}))
    assert not await f(FakePR({"foo": "abcedf"}))
    assert await f(FakePR({"foo": [10, 20, 30]}))
    assert not await f(FakePR({"foo": [10, 20]}))
    assert not await f(FakePR({"foo": [10, 20, 40, 50]}))
    f = filter.BinaryFilter({">": ("#foo", 3)})
    assert await f(FakePR({"foo": "barz"}))
    with pytest.raises(filter.InvalidOperator):
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
    with pytest.raises(filter.InvalidOperator):
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


async def test_regexp_invalid() -> None:
    with pytest.raises(filter.InvalidArguments):
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
    f.value_expanders["foo"] = lambda x: ["foobaz", "foobar"]
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
    with pytest.raises(filter.UnknownAttribute):
        await f(FakePR({"bar": 1}))


async def test_parse_error() -> None:
    with pytest.raises(filter.ParseError):
        filter.BinaryFilter({})


async def test_unknown_operator() -> None:
    with pytest.raises(filter.UnknownOperator):
        filter.BinaryFilter({"oops": (1, 2)})  # type: ignore[arg-type]


async def test_invalid_arguments() -> None:
    with pytest.raises(filter.InvalidArguments):
        filter.BinaryFilter({"=": (1, 2, 3)})  # type: ignore[typeddict-item]


async def test_str() -> None:
    assert "foo~=^f" == str(filter.BinaryFilter({"~=": ("foo", "^f")}))
    assert "-foo=1" == str(filter.BinaryFilter({"-": {"=": ("foo", 1)}}))
    assert "foo" == str(filter.BinaryFilter({"=": ("foo", True)}))
    assert "-bar" == str(filter.BinaryFilter({"=": ("bar", False)}))
    with pytest.raises(filter.InvalidOperator):
        str(filter.BinaryFilter({">=": ("bar", False)}))


def dtime(day: int) -> datetime.datetime:
    return date.utcnow().replace(day=day)


@freeze_time("2012-01-14")
async def test_datetime_binary() -> None:
    assert "foo>=2012-01-05T00:00:00" == str(
        filter.BinaryFilter({">=": ("foo", dtime(5))})
    )
    assert "foo<=2012-01-05T23:59:00" == str(
        filter.BinaryFilter({"<=": ("foo", dtime(5).replace(hour=23, minute=59))})
    )
    assert "foo<=2012-01-05T03:09:00" == str(
        filter.BinaryFilter({"<=": ("foo", dtime(5).replace(hour=3, minute=9))})
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


@freeze_time("2012-01-14")
async def test_time_binary() -> None:
    assert "foo>=00:00" == str(
        filter.BinaryFilter({">=": ("foo", date.Time(0, 0, UTC))})
    )
    assert "foo<=23:59" == str(
        filter.BinaryFilter({"<=": ("foo", date.Time(23, 59, UTC))})
    )
    assert "foo<=03:09" == str(
        filter.BinaryFilter({"<=": ("foo", date.Time(3, 9, UTC))})
    )
    assert "foo<=03:09[Europe/Paris]" == str(
        filter.BinaryFilter(
            {"<=": ("foo", date.Time(3, 9, zoneinfo.ZoneInfo("Europe/Paris")))}
        )
    )

    now = date.utcnow()

    f = filter.BinaryFilter({"<=": ("foo", date.Time(5, 8, UTC))})
    assert await f(FakePR({"foo": now.replace(hour=5, minute=8)}))
    assert await f(FakePR({"foo": now.replace(hour=2, minute=1)}))
    assert await f(FakePR({"foo": now.replace(hour=5, minute=1)}))
    assert not await f(FakePR({"foo": now.replace(hour=6, minute=2)}))
    assert not await f(FakePR({"foo": now.replace(hour=8, minute=9)}))

    f = filter.BinaryFilter({">=": ("foo", date.Time(5, 8, UTC))})
    assert await f(FakePR({"foo": now.replace(hour=5, minute=8)}))
    assert not await f(FakePR({"foo": now.replace(hour=2, minute=1)}))
    assert not await f(FakePR({"foo": now.replace(hour=5, minute=1)}))
    assert await f(FakePR({"foo": now.replace(hour=6, minute=2)}))
    assert await f(FakePR({"foo": now.replace(hour=8, minute=9)}))

    f = filter.BinaryFilter(
        {">=": ("foo", date.Time(5, 8, zoneinfo.ZoneInfo("Europe/Paris")))}
    )
    assert await f(FakePR({"foo": now.replace(hour=4, minute=8)}))
    assert not await f(FakePR({"foo": now.replace(hour=1, minute=1)}))
    assert not await f(FakePR({"foo": now.replace(hour=4, minute=1)}))
    assert await f(FakePR({"foo": now.replace(hour=5, minute=2)}))
    assert await f(FakePR({"foo": now.replace(hour=7, minute=9)}))


@freeze_time("2012-01-14")
async def test_dow_str() -> None:
    assert "foo>=Fri" == str(filter.BinaryFilter({">=": ("foo", date.DayOfWeek(5))}))
    assert "foo<=Sun" == str(filter.BinaryFilter({"<=": ("foo", date.DayOfWeek(7))}))
    assert "foo=Wed" == str(filter.BinaryFilter({"=": ("foo", date.DayOfWeek(3))}))


@pytest.mark.parametrize(
    "klass",
    (
        date.Day,
        date.Month,
    ),
)
@freeze_time("2012-01-14")
async def test_partial_datetime_str(
    klass: typing.Type[date.PartialDatetime],
) -> None:
    assert "foo>=5" == str(filter.BinaryFilter({">=": ("foo", klass(5))}))
    assert "foo<=4" == str(filter.BinaryFilter({"<=": ("foo", klass(4))}))
    assert "foo=3" == str(filter.BinaryFilter({"=": ("foo", klass(3))}))


async def test_partial_datetime_year_str() -> None:
    assert "foo>=2005" == str(filter.BinaryFilter({">=": ("foo", date.Year(2005))}))
    assert "foo<=2004" == str(filter.BinaryFilter({"<=": ("foo", date.Year(2004))}))
    assert "foo=2003" == str(filter.BinaryFilter({"=": ("foo", date.Year(2003))}))


@pytest.mark.parametrize(
    "klass",
    (
        date.Day,
        date.Month,
        lambda n: date.Year(2000 + n),
    ),
)
@freeze_time("2012-01-14")
async def test_partial_datetime_binary(
    klass: typing.Type[date.PartialDatetime],
) -> None:

    f = filter.BinaryFilter({"<=": ("foo", klass(5))})
    assert await f(FakePR({"foo": klass(2)}))
    assert await f(FakePR({"foo": klass(5)}))
    assert not await f(FakePR({"foo": klass(7)}))

    f = filter.BinaryFilter({"=": ("foo", klass(5))})
    assert await f(FakePR({"foo": klass(5)}))
    assert not await f(FakePR({"foo": klass(7)}))

    f = filter.BinaryFilter({">": ("foo", klass(5))})
    assert await f(FakePR({"foo": klass(7)}))
    assert not await f(FakePR({"foo": klass(2)}))
    assert not await f(FakePR({"foo": klass(5)}))


async def test_day_near_datetime() -> None:
    with freeze_time("2012-01-06T12:15:00", tz_offset=0) as frozen_time:
        today = frozen_time().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC
        )
        nextday = today.replace(day=today.day + 1)
        nextmonth = today.replace(month=today.month + 1, day=1)
        nextmonth_at_six = today.replace(month=today.month + 1, day=6)

        f = filter.NearDatetimeFilter({"<=": ("foo", date.Day(6))})
        frozen_time.move_to(today.replace(day=6))
        assert await f(FakePR({"foo": date.Day(6)})) == nextday
        frozen_time.move_to(today.replace(day=7))
        assert await f(FakePR({"foo": date.Day(7)})) == nextmonth
        frozen_time.move_to(today.replace(day=1))
        assert await f(FakePR({"foo": date.Day(1)})) == today
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"<": ("foo", date.Day(6))})
        frozen_time.move_to(today.replace(day=6))
        assert await f(FakePR({"foo": date.Day(6)})) == nextmonth
        frozen_time.move_to(today.replace(day=7))
        assert await f(FakePR({"foo": date.Day(7)})) == nextmonth
        frozen_time.move_to(today.replace(day=1))
        assert await f(FakePR({"foo": date.Day(1)})) == today
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">=": ("foo", date.Day(6))})
        frozen_time.move_to(today.replace(day=6))
        assert await f(FakePR({"foo": date.Day(6)})) == nextday
        frozen_time.move_to(today.replace(day=7))
        assert await f(FakePR({"foo": date.Day(7)})) == nextmonth
        frozen_time.move_to(today.replace(day=1))
        assert await f(FakePR({"foo": date.Day(1)})) == today
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">": ("foo", date.Day(6))})
        frozen_time.move_to(today.replace(day=6))
        assert await f(FakePR({"foo": date.Day(6)})) == nextmonth
        frozen_time.move_to(today.replace(day=7))
        assert await f(FakePR({"foo": date.Day(7)})) == nextmonth
        frozen_time.move_to(today.replace(day=1))
        assert await f(FakePR({"foo": date.Day(1)})) == today
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"=": ("foo", date.Day(6))})
        frozen_time.move_to(today.replace(day=6))
        assert await f(FakePR({"foo": date.Day(6)})) == nextday
        frozen_time.move_to(today.replace(day=7))
        assert await f(FakePR({"foo": date.Day(7)})) == nextmonth_at_six
        frozen_time.move_to(today.replace(day=1))
        assert await f(FakePR({"foo": date.Day(1)})) == today
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"!=": ("foo", date.Day(6))})
        frozen_time.move_to(today.replace(day=6))
        assert await f(FakePR({"foo": date.Day(6)})) == nextday
        frozen_time.move_to(today.replace(day=7))
        assert await f(FakePR({"foo": date.Day(7)})) == nextmonth_at_six
        frozen_time.move_to(today.replace(day=1))
        assert await f(FakePR({"foo": date.Day(1)})) == today
        assert await f(FakePR({"foo": None})) == date.DT_MAX


async def test_day_of_the_week_near_datetime() -> None:
    # 2012-01-06 is a Friday (5)
    with freeze_time("2012-01-06T12:15:00", tz_offset=0) as frozen_time:
        today = frozen_time().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC
        )
        on_monday = today.replace(day=2)
        on_saturday = today.replace(day=7)
        on_sunday = today.replace(day=8)
        next_saturday = today.replace(day=14)

        monday = date.DayOfWeek(1)
        saturday = date.DayOfWeek(6)
        sunday = date.DayOfWeek(7)

        f = filter.NearDatetimeFilter({"<=": ("foo", saturday)})
        frozen_time.move_to(on_saturday)
        assert await f(FakePR({"foo": saturday})) == on_sunday
        frozen_time.move_to(on_sunday)
        assert await f(FakePR({"foo": sunday})) == next_saturday
        frozen_time.move_to(on_monday)
        assert await f(FakePR({"foo": monday})) == on_saturday
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"<": ("foo", saturday)})
        frozen_time.move_to(on_saturday)
        assert await f(FakePR({"foo": saturday})) == next_saturday
        frozen_time.move_to(on_sunday)
        assert await f(FakePR({"foo": sunday})) == next_saturday
        frozen_time.move_to(on_monday)
        assert await f(FakePR({"foo": monday})) == on_saturday
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">=": ("foo", saturday)})
        frozen_time.move_to(on_saturday)
        assert await f(FakePR({"foo": saturday})) == on_sunday
        frozen_time.move_to(on_sunday)
        assert await f(FakePR({"foo": sunday})) == next_saturday
        frozen_time.move_to(on_monday)
        assert await f(FakePR({"foo": monday})) == on_saturday
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">": ("foo", saturday)})
        frozen_time.move_to(on_saturday)
        assert await f(FakePR({"foo": saturday})) == next_saturday
        frozen_time.move_to(on_sunday)
        assert await f(FakePR({"foo": sunday})) == next_saturday
        frozen_time.move_to(on_monday)
        assert await f(FakePR({"foo": monday})) == on_saturday
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"=": ("foo", saturday)})
        frozen_time.move_to(on_saturday)
        assert await f(FakePR({"foo": saturday})) == on_sunday
        frozen_time.move_to(on_sunday)
        assert await f(FakePR({"foo": sunday})) == next_saturday
        frozen_time.move_to(on_monday)
        assert await f(FakePR({"foo": monday})) == on_saturday
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"!=": ("foo", saturday)})
        frozen_time.move_to(on_saturday)
        assert await f(FakePR({"foo": saturday})) == on_sunday
        frozen_time.move_to(on_sunday)
        assert await f(FakePR({"foo": sunday})) == next_saturday
        frozen_time.move_to(on_monday)
        assert await f(FakePR({"foo": monday})) == on_saturday
        assert await f(FakePR({"foo": None})) == date.DT_MAX


async def test_month_near_datetime() -> None:
    with freeze_time("2012-06-06T12:15:00", tz_offset=0) as frozen_time:
        today = frozen_time().replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC
        )
        in_june = today.replace(month=today.month, day=1)
        in_july = today.replace(month=today.month + 1, day=1)
        next_year_in_january = today.replace(year=today.year + 1, month=1, day=1)
        next_year_in_june = in_june.replace(year=in_june.year + 1, month=6, day=1)

        f = filter.NearDatetimeFilter({"<=": ("foo", date.Month(6))})
        frozen_time.move_to(in_june.replace(month=6))
        assert await f(FakePR({"foo": date.Month(6)})) == in_july
        frozen_time.move_to(in_june.replace(month=7))
        assert await f(FakePR({"foo": date.Month(7)})) == next_year_in_january
        frozen_time.move_to(in_june.replace(month=1))
        assert await f(FakePR({"foo": date.Month(1)})) == in_june
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"<": ("foo", date.Month(6))})
        frozen_time.move_to(in_june.replace(month=6))
        assert await f(FakePR({"foo": date.Month(6)})) == next_year_in_january
        frozen_time.move_to(in_june.replace(month=7))
        assert await f(FakePR({"foo": date.Month(7)})) == next_year_in_january
        frozen_time.move_to(in_june.replace(month=1))
        assert await f(FakePR({"foo": date.Month(1)})) == in_june
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">=": ("foo", date.Month(6))})
        frozen_time.move_to(in_june.replace(month=6))
        assert await f(FakePR({"foo": date.Month(6)})) == in_july
        frozen_time.move_to(in_june.replace(month=7))
        assert await f(FakePR({"foo": date.Month(7)})) == next_year_in_january
        frozen_time.move_to(in_june.replace(month=1))
        assert await f(FakePR({"foo": date.Month(1)})) == in_june
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">": ("foo", date.Month(6))})
        frozen_time.move_to(in_june.replace(month=6))
        assert await f(FakePR({"foo": date.Month(6)})) == next_year_in_january
        frozen_time.move_to(in_june.replace(month=7))
        assert await f(FakePR({"foo": date.Month(7)})) == next_year_in_january
        frozen_time.move_to(in_june.replace(month=1))
        assert await f(FakePR({"foo": date.Month(1)})) == in_june
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"=": ("foo", date.Month(6))})
        frozen_time.move_to(in_june.replace(month=6))
        assert await f(FakePR({"foo": date.Month(6)})) == in_july
        frozen_time.move_to(in_june.replace(month=7))
        assert await f(FakePR({"foo": date.Month(7)})) == next_year_in_june
        frozen_time.move_to(in_june.replace(month=1))
        assert await f(FakePR({"foo": date.Month(1)})) == in_june
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"!=": ("foo", date.Month(6))})
        frozen_time.move_to(in_june.replace(month=6))
        assert await f(FakePR({"foo": date.Month(6)})) == in_july
        frozen_time.move_to(in_june.replace(month=7))
        assert await f(FakePR({"foo": date.Month(7)})) == next_year_in_june
        frozen_time.move_to(in_june.replace(month=1))
        assert await f(FakePR({"foo": date.Month(1)})) == in_june
        assert await f(FakePR({"foo": None})) == date.DT_MAX


async def test_year_near_datetime() -> None:
    with freeze_time("2012-01-14T12:15:00", tz_offset=0) as frozen_time:
        today = frozen_time().replace(tzinfo=UTC)
        in_2016 = datetime.datetime(2016, 1, 1, 0, 0, 0, 0, tzinfo=UTC)
        in_2017 = datetime.datetime(2017, 1, 1, 0, 0, 0, 0, tzinfo=UTC)

        f = filter.NearDatetimeFilter({"<=": ("foo", date.Year(2016))})
        frozen_time.move_to(today.replace(year=2016))
        assert await f(FakePR({"foo": date.Year(2016)})) == in_2017
        frozen_time.move_to(today.replace(year=2017))
        assert await f(FakePR({"foo": date.Year(2017)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2011))
        assert await f(FakePR({"foo": date.Year(2011)})) == in_2016
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"<": ("foo", date.Year(2016))})
        frozen_time.move_to(today.replace(year=2016))
        assert await f(FakePR({"foo": date.Year(2016)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2017))
        assert await f(FakePR({"foo": date.Year(2017)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2011))
        assert await f(FakePR({"foo": date.Year(2011)})) == in_2016
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">=": ("foo", date.Year(2016))})
        frozen_time.move_to(today.replace(year=2016))
        assert await f(FakePR({"foo": date.Year(2016)})) == in_2017
        frozen_time.move_to(today.replace(year=2017))
        assert await f(FakePR({"foo": date.Year(2017)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2011))
        assert await f(FakePR({"foo": date.Year(2011)})) == in_2016
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">": ("foo", date.Year(2016))})
        frozen_time.move_to(today.replace(year=2016))
        assert await f(FakePR({"foo": date.Year(2016)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2017))
        assert await f(FakePR({"foo": date.Year(2017)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2011))
        assert await f(FakePR({"foo": date.Year(2011)})) == in_2016
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"=": ("foo", date.Year(2016))})
        frozen_time.move_to(today.replace(year=2016))
        assert await f(FakePR({"foo": date.Year(2016)})) == in_2017
        frozen_time.move_to(today.replace(year=2017))
        assert await f(FakePR({"foo": date.Year(2017)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2011))
        assert await f(FakePR({"foo": date.Year(2011)})) == in_2016
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"!=": ("foo", date.Year(2016))})
        frozen_time.move_to(today.replace(year=2016))
        assert await f(FakePR({"foo": date.Year(2016)})) == in_2017
        frozen_time.move_to(today.replace(year=2017))
        assert await f(FakePR({"foo": date.Year(2017)})) == date.DT_MAX
        frozen_time.move_to(today.replace(year=2011))
        assert await f(FakePR({"foo": date.Year(2011)})) == in_2016
        assert await f(FakePR({"foo": None})) == date.DT_MAX


async def test_time_near_datetime() -> None:
    with freeze_time("2012-01-06T05:08:00", tz_offset=0) as frozen_time:
        tzinfo = UTC
        now = frozen_time().replace(tzinfo=tzinfo)
        time_now = date.Time(now.hour, now.minute, tzinfo)
        atmidnight = now.replace(
            day=now.day + 1,
            hour=0,
            second=0,
            minute=0,
            microsecond=0,
        )
        nextday = now.replace(day=now.day + 1)
        soon = now.replace(minute=now.minute + 1)

        f = filter.NearDatetimeFilter({"<=": ("foo", time_now)})
        frozen_time.move_to(now)
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == soon
        frozen_time.move_to(now.replace(hour=2, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=5, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=6, minute=2))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        frozen_time.move_to(now.replace(hour=8, minute=9))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"<": ("foo", time_now)})
        frozen_time.move_to(now)
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        frozen_time.move_to(now.replace(hour=2, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=5, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=6, minute=2))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        frozen_time.move_to(now.replace(hour=8, minute=9))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">=": ("foo", time_now)})
        frozen_time.move_to(now)
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == soon
        frozen_time.move_to(now.replace(hour=2, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=5, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=6, minute=2))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        frozen_time.move_to(now.replace(hour=8, minute=9))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({">": ("foo", time_now)})
        frozen_time.move_to(now)
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        frozen_time.move_to(now.replace(hour=2, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=5, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=6, minute=2))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        frozen_time.move_to(now.replace(hour=8, minute=9))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == atmidnight
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"=": ("foo", time_now)})
        frozen_time.move_to(now)
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == soon
        frozen_time.move_to(now.replace(hour=2, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=5, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=6, minute=2))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == nextday
        frozen_time.move_to(now.replace(hour=8, minute=9))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == nextday
        assert await f(FakePR({"foo": None})) == date.DT_MAX

        f = filter.NearDatetimeFilter({"!=": ("foo", time_now)})
        frozen_time.move_to(now)
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == soon
        frozen_time.move_to(now.replace(hour=2, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=5, minute=1))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
        frozen_time.move_to(now.replace(hour=6, minute=2))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == nextday
        frozen_time.move_to(now.replace(hour=8, minute=9))
        assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == nextday
        assert await f(FakePR({"foo": None})) == date.DT_MAX


@freeze_time("2012-01-14T05:08:00")
async def test_datetime_near_datetime() -> None:
    # Condition on past date
    for op in ("<=", ">=", ">", "<", "=", "!="):
        f = filter.NearDatetimeFilter(
            typing.cast(filter.TreeT, {op: ("updated-at", dtime(8))})
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
            typing.cast(filter.TreeT, {op: ("updated-at", dtime(16))})
        )
        assert await f(FakePR({"updated-at": dtime(2)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(10)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(14)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(18)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(16)})) == dtime(16).replace(
            minute=9
        ), str(f)
        assert await f(FakePR({"updated-at": None})) == date.DT_MAX

    # Condition on future date without equality
    for op in ("<", ">"):
        f = filter.NearDatetimeFilter(
            typing.cast(filter.TreeT, {op: ("updated-at", dtime(16))})
        )
        assert await f(FakePR({"updated-at": dtime(2)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(10)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(14)})) == dtime(16), str(f)
        assert await f(FakePR({"updated-at": dtime(18)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": dtime(16)})) == date.DT_MAX, str(f)
        assert await f(FakePR({"updated-at": None})) == date.DT_MAX


def rdtime(day: int) -> date.RelativeDatetime:
    return date.RelativeDatetime(dtime(day))


@freeze_time("2012-01-14T05:08:00")
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
    with freeze_time("2012-01-10T05:08:00", tz_offset=0) as frozen_time:
        tzinfo = UTC
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


async def test_multiple_near_datetime() -> None:
    with freeze_time("2012-01-14T05:08:00", tz_offset=0) as frozen_time:
        tzinfo = UTC
        now = frozen_time().replace(tzinfo=tzinfo)
        now_time = date.Time(now.hour, now.minute, tzinfo)
        atmidnight = now.replace(
            day=now.day + 1,
            hour=0,
            second=0,
            minute=0,
            microsecond=0,
        )
        in_two_hours = now + datetime.timedelta(hours=2)
        assert tzinfo == in_two_hours.tzinfo
        in_two_hours_time = date.Time(
            in_two_hours.hour, in_two_hours.minute, in_two_hours.tzinfo
        )
        soon = now + datetime.timedelta(minutes=1)

        trees = (
            {
                "or": [
                    {"<=": ("foo", in_two_hours_time)},
                    {"<=": ("foo", now_time)},
                ]
            },
            {
                "and": [
                    {">=": ("foo", in_two_hours_time)},
                    {">=": ("foo", now_time)},
                ]
            },
            {
                "or": [
                    {">=": ("foo", in_two_hours_time)},
                    {">=": ("foo", now_time)},
                ]
            },
            {
                "and": [
                    {"<=": ("foo", in_two_hours_time)},
                    {"<=": ("foo", now_time)},
                ]
            },
            # NOTE(charly): "not" is a just a pass through, the PR is
            # re-evaluated like "and" or "or"
            {
                "not": {
                    "and": [
                        {"<=": ("foo", in_two_hours_time)},
                        {"<=": ("foo", now_time)},
                    ]
                }
            },
        )

        for tree in trees:
            f = filter.NearDatetimeFilter(typing.cast(filter.TreeT, tree))

            frozen_time.move_to(now)
            assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == soon
            frozen_time.move_to(now.replace(hour=2, minute=1))
            assert await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)})) == now
            frozen_time.move_to(now.replace(hour=6, minute=8))
            assert (
                await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)}))
                == in_two_hours
            )
            frozen_time.move_to(now.replace(hour=8, minute=9))
            assert (
                await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)}))
                == atmidnight
            )
            frozen_time.move_to(now.replace(hour=18, minute=9))
            assert (
                await f(FakePR({"foo": frozen_time().replace(tzinfo=UTC)}))
                == atmidnight
            )


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
        filter.BinaryFilter({"or": {"foo": "whar"}})


async def test_not_tree() -> None:
    f1 = {"=": ("foo", 1)}
    f2 = {"=": ("bar", 1)}

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
    f1 = {"=": ("bar", 1)}
    f2 = {"=": ("foo", 1)}
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
        filter.BinaryFilter({"and": ({"=": ("head", "foobar")}, {">": ("#files", 3)})})
    )
    assert string == "(head=foobar and #files>3)"

    string = str(
        filter.BinaryFilter(
            {"not": {"and": ({"=": ("head", "foobar")}, {">": ("#files", 3)})}}
        )
    )
    assert string == "not(head=foobar and #files>3)"


def get_scheduled_pr() -> FakePR:
    return FakePR(
        {
            "current-day-of-week": date.DayOfWeek(date.utcnow().isoweekday()),
            "current-year": date.Year(date.utcnow().year),
            "current-day": date.Day(date.utcnow().day),
            "number": 3433,
            "current-time": date.utcnow(),
            "current-month": date.Month(date.utcnow().month),
        }
    )


async def test_schedule_with_timezone() -> None:
    with freeze_time("2021-10-19T22:01:31.610725", tz_offset=0) as frozen_time:
        tree = parser.parse("schedule=Mon-Fri 09:00-17:30[Europe/Paris]")
        f = filter.NearDatetimeFilter(tree)

        today = frozen_time().replace(tzinfo=UTC)

        next_refreshes = [
            "2021-10-20T07:00:01",
            "2021-10-20T15:30:00",
            "2021-10-20T15:31:00",
            "2021-10-21T07:00:01",
        ]
        for next_refresh in next_refreshes:
            next_refresh_dt = datetime.datetime.fromisoformat(next_refresh).replace(
                tzinfo=datetime.timezone.utc
            )
            assert await f(get_scheduled_pr()) == next_refresh_dt
            frozen_time.move_to(next_refresh_dt)

        frozen_time.move_to(today.replace(day=23))
        assert await f(get_scheduled_pr()) == datetime.datetime(
            2021, 10, 24, 7, 0, 1, tzinfo=datetime.timezone.utc
        )

    with freeze_time("2022-09-16T10:38:18.019100", tz_offset=0) as frozen_time:
        tree = parser.parse("schedule=Mon-Fri 06:00-17:00[America/Los_Angeles]")
        f = filter.NearDatetimeFilter(tree)

        today = frozen_time().replace(tzinfo=UTC)

        next_refreshes = [
            "2022-09-16T13:00:01",
        ]
        for next_refresh in next_refreshes:
            next_refresh_dt = datetime.datetime.fromisoformat(next_refresh).replace(
                tzinfo=datetime.timezone.utc
            )
            assert await f(get_scheduled_pr()) == next_refresh_dt
            frozen_time.move_to(next_refresh_dt)
