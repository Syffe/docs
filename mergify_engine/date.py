import dataclasses
import datetime
import functools
import re
import typing
import zoneinfo


DT_MAX = datetime.datetime.max.replace(tzinfo=datetime.timezone.utc)


@dataclasses.dataclass
class InvalidDate(Exception):
    message: str


TIMEZONES = {f"[{tz}]" for tz in zoneinfo.available_timezones()}


def extract_timezone(
    value: str,
) -> typing.Tuple[str, typing.Union[datetime.timezone, zoneinfo.ZoneInfo]]:
    if value[-1] == "]":
        for timezone in TIMEZONES:
            if value.endswith(timezone):
                return value[: -len(timezone)], zoneinfo.ZoneInfo(timezone[1:-1])
        raise InvalidDate("Invalid timezone")
    return value, datetime.timezone.utc


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def is_datetime_between_time_range(
    time_to_check: datetime.datetime,
    begin_hour: int,
    begin_minute: int,
    end_hour: int,
    end_minute: int,
    strict: bool,
) -> bool:
    d_start = datetime.datetime(
        year=time_to_check.year,
        month=time_to_check.month,
        day=time_to_check.day,
        hour=begin_hour,
        minute=begin_minute,
        tzinfo=time_to_check.tzinfo,
    )

    d_end = datetime.datetime(
        year=time_to_check.year,
        month=time_to_check.month,
        day=time_to_check.day,
        hour=end_hour,
        minute=end_minute,
        tzinfo=time_to_check.tzinfo,
    )

    if strict:
        return d_start < time_to_check < d_end
    else:
        return d_start <= time_to_check <= d_end


@dataclasses.dataclass(order=True)
class PartialDatetime:
    value: int

    def __str__(self) -> str:
        return str(self.value)

    @classmethod
    def from_string(cls, value: str) -> "PartialDatetime":
        try:
            number = int(value)
        except ValueError:
            raise InvalidDate(f"{value} is not a number")
        return cls(number)


class TimedeltaRegexResultT(typing.TypedDict):
    filled: typing.Optional[str]
    days: typing.Optional[str]
    hours: typing.Optional[str]
    minutes: typing.Optional[str]
    seconds: typing.Optional[str]


@dataclasses.dataclass(order=True)
class RelativeDatetime:
    # NOTE(sileht): Like a datetime, but we known it has been computed from `utcnow() + timedelta()`
    value: datetime.datetime

    # PostgreSQL's day-time interval format without seconds and microseconds, e.g. "3 days 04:05"
    _TIMEDELTA_TO_NOW_RE: typing.ClassVar[typing.Pattern[str]] = re.compile(
        r"^"
        r"(?:(?P<days>\d+) (days? ?))?"
        r"(?:"
        r"(?P<hours>\d+):"
        r"(?P<minutes>\d\d)"
        r")? ago$"
    )

    @classmethod
    def from_string(cls, value: str) -> "RelativeDatetime":
        m = cls._TIMEDELTA_TO_NOW_RE.match(value)
        if m is None:
            raise InvalidDate("Invalid relative date")

        kw = typing.cast(TimedeltaRegexResultT, m.groupdict())
        return cls(
            utcnow()
            - datetime.timedelta(
                days=int(kw["days"] or 0),
                hours=int(kw["hours"] or 0),
                minutes=int(kw["minutes"] or 0),
            )
        )

    def __post_init__(self) -> None:
        if self.value.tzinfo is None:
            raise InvalidDate("timezone is missing")


@dataclasses.dataclass
class Year(PartialDatetime):
    def __post_init__(self) -> None:
        if self.value < 2000 or self.value > 9999:
            raise InvalidDate("Year must be between 2000 and 9999")


@dataclasses.dataclass
class Month(PartialDatetime):
    def __post_init__(self) -> None:
        if self.value < 1 or self.value > 12:
            raise InvalidDate("Month must be between 1 and 12")


@dataclasses.dataclass
class Day(PartialDatetime):
    def __post_init__(self) -> None:
        if self.value < 1 or self.value > 31:
            raise InvalidDate("Day must be between 1 and 31")


@functools.total_ordering
@dataclasses.dataclass
class Time:
    hour: int
    minute: int
    tzinfo: datetime.tzinfo

    @classmethod
    def from_string(cls, string: str) -> "Time":
        value, tzinfo = extract_timezone(string)
        hour_str, sep, minute_str = value.partition(":")
        if sep != ":":
            raise InvalidDate("Invalid time")
        try:
            hour = int(hour_str)
        except ValueError:
            raise InvalidDate(f"{hour_str} is not a number")
        try:
            minute = int(minute_str)
        except ValueError:
            raise InvalidDate(f"{minute_str} is not a number")

        return cls(hour=hour, minute=minute, tzinfo=tzinfo)

    def __post_init__(self) -> None:
        if self.hour < 0 or self.hour >= 24:
            raise InvalidDate("Hour must be between 0 and 23")
        elif self.minute < 0 or self.minute >= 60:
            raise InvalidDate("Minute must be between 0 and 59")

    def __str__(self) -> str:
        value = f"{self.hour:02d}:{self.minute:02d}"
        if isinstance(self.tzinfo, zoneinfo.ZoneInfo):
            value += f"[{self.tzinfo.key}]"
        return value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (Time, datetime.datetime)):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        now = utcnow()
        d1 = self._to_dt(self, now)
        d2 = self._to_dt(other, now)
        return d1 == d2

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, (Time, datetime.datetime)):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        now = utcnow()
        d1 = self._to_dt(self, now)
        d2 = self._to_dt(other, now)
        return d1 > d2

    @staticmethod
    def _to_dt(
        obj: typing.Union["Time", datetime.datetime], ref: datetime.datetime
    ) -> datetime.datetime:
        if isinstance(obj, datetime.datetime):
            return obj
        elif isinstance(obj, Time):
            return ref.astimezone(obj.tzinfo).replace(
                minute=obj.minute,
                hour=obj.hour,
                second=0,
                microsecond=0,
            )
        else:
            raise ValueError(f"Unsupport comparaison type: {type(obj)}")


_SHORT_WEEKDAY = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_LONG_WEEKDAY = (
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
)


@dataclasses.dataclass
class DayOfWeek(PartialDatetime):
    @classmethod
    def from_string(cls, string: str) -> "DayOfWeek":
        try:
            return cls(_SHORT_WEEKDAY.index(string.lower()) + 1)
        except ValueError:
            pass
        try:
            return cls(_LONG_WEEKDAY.index(string.lower()) + 1)
        except ValueError:
            pass
        try:
            dow = int(string)
        except ValueError:
            raise InvalidDate(f"{string} is not a number or literal day of the week")
        return cls(dow)

    def __post_init__(self) -> None:
        if self.value < 1 or self.value > 7:
            raise InvalidDate("Day of the week must be between 1 and 7")

    def __str__(self) -> str:
        return _SHORT_WEEKDAY[self.value - 1].capitalize()


@dataclasses.dataclass
class Schedule:
    start_weekday: int
    end_weekday: int
    start_hour: int
    end_hour: int
    start_minute: int
    end_minute: int
    tzinfo: datetime.tzinfo

    @classmethod
    def from_strings(
        cls,
        days: str,
        times: str,
    ) -> "Schedule":
        start_weekday, end_weekday = days.split("-")
        start_weekday_int = DayOfWeek.from_string(start_weekday).value
        end_weekday_int = DayOfWeek.from_string(end_weekday).value

        start_hourminute, end_hourminute = times.split("-")
        start_time_obj = Time.from_string(start_hourminute)
        end_time_obj = Time.from_string(end_hourminute)

        return cls(
            start_weekday=start_weekday_int,
            end_weekday=end_weekday_int,
            start_hour=start_time_obj.hour,
            end_hour=end_time_obj.hour,
            start_minute=start_time_obj.minute,
            end_minute=end_time_obj.minute,
            tzinfo=end_time_obj.tzinfo,
        )

    def __post_init__(self) -> None:
        if self.start_weekday > self.end_weekday:
            raise InvalidDate(
                "Starting weekday of schedule needs to be before the last day"
            )

        if self.start_hour > self.end_hour:
            raise InvalidDate(
                "Starting hour of schedule needs to be less or equal than the ending hour"
            )

        if self.start_hour == self.end_hour and self.start_minute > self.end_minute:
            raise InvalidDate(
                "Starting minute of schedule needs to be less or equal than the ending minute"
            )

        if self.start_hour == self.end_hour and self.start_minute == self.end_minute:
            raise InvalidDate(
                "Cannot have schedule with the same hour+minute as start and end"
            )

    def __str__(self) -> str:
        return (
            f"{_SHORT_WEEKDAY[self.start_weekday - 1].capitalize()}-{_SHORT_WEEKDAY[self.end_weekday - 1].capitalize()}"
            " "
            f"{self.start_hour:02d}:{self.start_minute:02d}-"
            f"{self.end_hour:02d}:{self.end_minute:02d}"
            f"[{self.tzinfo}]"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(
            other,
            (
                Schedule,
                datetime.datetime,
            ),
        ):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        if isinstance(other, Schedule):
            # This is for unit/functional test purposes only
            return (
                self.start_weekday == other.start_weekday
                and self.end_weekday == other.end_weekday
                and self.start_hour == other.start_hour
                and self.start_minute == other.start_minute
                and self.end_hour == other.end_hour
                and self.end_minute == other.end_minute
            )

        # Allow to check if a datetime is in a schedule
        dother = other.astimezone(self.tzinfo)
        return (
            self.start_weekday <= dother.isoweekday() <= self.end_weekday
            and self.is_datetime_between_time_range(dother, strict=False)
        )

    def is_datetime_between_time_range(
        self,
        time_to_check: datetime.datetime,
        strict: bool,
    ) -> bool:
        return is_datetime_between_time_range(
            time_to_check.astimezone(self.tzinfo),
            self.start_hour,
            self.start_minute,
            self.end_hour,
            self.end_minute,
            strict,
        )

    def get_next_valid_time(self, from_time: datetime.datetime) -> datetime.datetime:
        def return_as_origin_timezone(dt: datetime.datetime) -> datetime.datetime:
            return dt.astimezone(from_time.tzinfo)

        from_time_as_tz = from_time.astimezone(self.tzinfo)

        if self.is_datetime_between_time_range(from_time_as_tz, strict=True):
            return return_as_origin_timezone(
                from_time_as_tz.replace(
                    hour=self.end_hour,
                    minute=self.end_minute,
                    second=0,
                )
            )
        elif (
            from_time_as_tz.hour == self.end_hour
            and from_time_as_tz.minute == self.end_minute
        ):
            # 1 minute after the end just to invalidate the summary condition
            return return_as_origin_timezone(
                (from_time_as_tz + datetime.timedelta(minutes=1))
            )

        # Outside of the whole schedule
        if self.start_weekday > from_time_as_tz.isoweekday() > self.end_weekday:
            # Add the number of days missing to go to the starting weekday
            # of the next week
            from_time_as_tz += datetime.timedelta(
                days=self.start_weekday + (7 - from_time_as_tz.isoweekday())
            )
        # Inside day schedule but oustide of hour+minute schedule
        elif from_time_as_tz.isoweekday() == self.end_weekday:
            # Next time is next week at the start of schedule
            from_time_as_tz += datetime.timedelta(
                days=self.start_weekday + (7 - from_time_as_tz.isoweekday())
            )
        elif from_time_as_tz.hour < self.start_hour or (
            from_time_as_tz.hour == self.start_hour
            and from_time_as_tz.minute < self.start_minute
        ):
            # We're in a good day but before start hour + start minute
            # The hour+minute replace is done at the end, this elif is just
            # for clarity.
            pass
        else:
            # Next time is next day at start hour + start minute
            from_time_as_tz += datetime.timedelta(days=1)

        return return_as_origin_timezone(
            from_time_as_tz.replace(
                hour=self.start_hour, minute=self.start_minute, second=1, microsecond=0
            )
        )


def fromisoformat(s: str) -> datetime.datetime:
    """always returns an aware datetime object with UTC timezone"""
    if s[-1] == "Z":
        s = s[:-1]
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=datetime.timezone.utc)
    else:
        return dt.astimezone(datetime.timezone.utc)


def fromisoformat_with_zoneinfo(string: str) -> datetime.datetime:
    value, tzinfo = extract_timezone(string)
    try:
        # TODO(sileht): astimezone doesn't look logic, but keep the
        # same behavior as the old parse for now
        return fromisoformat(value).astimezone(tzinfo)
    except ValueError:
        raise InvalidDate("Invalid timestamp")


def fromtimestamp(timestamp: float) -> datetime.datetime:
    """always returns an aware datetime object with UTC timezone"""
    return datetime.datetime.fromtimestamp(timestamp, datetime.timezone.utc)


def pretty_datetime(dt: datetime.datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M %Z")


def pretty_time(dt: datetime.datetime) -> str:
    return dt.strftime("%H:%M %Z")


_INTERVAL_RE = re.compile(
    r"""
    (?P<filled>
        ((?P<days>[-+]?\d+)\s*d(ays?)? \s* )?
        ((?P<hours>[-+]?\d+)\s*h(ours?)? \s* )?
        ((?P<minutes>[-+]?\d+)\s*m((inutes?|ins?)?)? \s* )?
        ((?P<seconds>[-+]?\d+)\s*s(econds?)? \s* )?
    )
    """,
    re.VERBOSE,
)


def interval_from_string(value: str) -> datetime.timedelta:
    m = _INTERVAL_RE.match(value)
    if m is None:
        raise InvalidDate("Invalid date interval")

    kw = typing.cast(TimedeltaRegexResultT, m.groupdict())
    if not kw or not kw["filled"]:
        raise InvalidDate("Invalid date interval")

    return datetime.timedelta(
        days=int(kw["days"] or 0),
        hours=int(kw["hours"] or 0),
        minutes=int(kw["minutes"] or 0),
        seconds=int(kw["seconds"] or 0),
    )
