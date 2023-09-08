from __future__ import annotations

import dataclasses
import datetime
import functools
import re
import time
import typing
import zoneinfo

import dateutil
import pydantic
import typing_extensions


UTC = zoneinfo.ZoneInfo("UTC")
DT_MAX = datetime.datetime.max.replace(tzinfo=UTC)


@dataclasses.dataclass
class InvalidDate(Exception):
    message: str


@dataclasses.dataclass
class TimezoneNotAllowed(Exception):
    message: str


TIMEZONES = {f"[{tz}]" for tz in zoneinfo.available_timezones()}


def extract_timezone(
    value: str,
) -> tuple[str, zoneinfo.ZoneInfo]:
    if has_timezone(value):
        if value[-1] == "Z":
            return value[:-1], UTC

        for timezone in TIMEZONES:
            if value.endswith(timezone):
                return value[: -len(timezone)], zoneinfo.ZoneInfo(timezone[1:-1])

        raise InvalidDate("Invalid timezone")

    return value, UTC


def has_timezone(value: str) -> bool:
    return value[-1] == "]" or value[-1] == "Z"


def utcnow() -> datetime.datetime:
    return datetime.datetime.now(tz=UTC)


def utcnow_from_clock_realtime() -> datetime.datetime:
    # time.clock_gettime is not mocked by freezegun
    return fromtimestamp(time.clock_gettime(time.CLOCK_REALTIME))


def is_datetime_inside_time_range(
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
    return d_start <= time_to_check <= d_end


class TimedeltaRegexResultT(typing.TypedDict):
    filled: str | None
    days: str | None
    hours: str | None
    minutes: str | None
    seconds: str | None


@dataclasses.dataclass(order=True)
class RelativeDatetime:
    # NOTE(sileht): Like a datetime, but we known it has been computed from `utcnow() + timedelta()`
    value: datetime.datetime

    # PostgreSQL's day-time interval format without seconds and microseconds, e.g. "3 days 04:05"
    _TIMEDELTA_TO_NOW_RE: typing.ClassVar[re.Pattern[str]] = re.compile(
        r"^"
        r"(?:(?P<days>\d+) (days? ?))?"
        r"(?:"
        r"(?P<hours>\d+):"
        r"(?P<minutes>\d\d)"
        r")? ago$"
    )

    @classmethod
    def from_string(cls, value: str) -> RelativeDatetime:
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


@functools.total_ordering
@dataclasses.dataclass
class Time:
    hour: int
    minute: int
    tzinfo: zoneinfo.ZoneInfo

    @classmethod
    def from_string(cls, string: str, timezone_allowed: bool = True) -> Time:
        if not timezone_allowed and has_timezone(string):
            raise TimezoneNotAllowed("Timezone is not allowed")

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
        if self.minute < 0 or self.minute >= 60:
            raise InvalidDate("Minute must be between 0 and 59")

    def __str__(self) -> str:
        value = f"{self.hour:02d}:{self.minute:02d}"
        if isinstance(self.tzinfo, zoneinfo.ZoneInfo) and self.tzinfo != UTC:
            value += f"[{self.tzinfo.key}]"
        return value

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Time | datetime.datetime):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        now = utcnow()
        d1 = self._to_dt(self, now)
        d2 = self._to_dt(other, now)
        return d1 == d2

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Time | datetime.datetime):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        now = utcnow()
        d1 = self._to_dt(self, now)
        d2 = self._to_dt(other, now)
        return d1 > d2

    @staticmethod
    def _to_dt(
        obj: Time | datetime.datetime, ref: datetime.datetime
    ) -> datetime.datetime:
        if isinstance(obj, datetime.datetime):
            return obj.replace(second=0, microsecond=0)
        if isinstance(obj, Time):
            return ref.astimezone(obj.tzinfo).replace(
                minute=obj.minute,
                hour=obj.hour,
                second=0,
                microsecond=0,
            )
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


class DayOfWeek(int):
    @classmethod
    def from_string(cls, string: str) -> DayOfWeek:
        try:
            return cls(_SHORT_WEEKDAY.index(string.lower()) + 1)
        except ValueError:
            pass
        try:
            return cls(_LONG_WEEKDAY.index(string.lower()) + 1)
        except ValueError:
            pass
        try:
            return cls(int(string))
        except ValueError:
            raise InvalidDate(f"{string} is not a number or literal day of the week")


class TimeJSON(typing_extensions.TypedDict):
    hour: int
    minute: int


class TimeRangeJSON(typing_extensions.TypedDict):
    start_at: TimeJSON
    end_at: TimeJSON


class DayJSON(typing_extensions.TypedDict):
    times: list[TimeRangeJSON]


class ScheduleJSON(typing_extensions.TypedDict):
    timezone: str
    days: dict[str, DayJSON]


FULL_DAY: DayJSON = {
    "times": [
        {
            "start_at": {"hour": 0, "minute": 0},
            "end_at": {"hour": 23, "minute": 59},
        },
    ]
}

EMPTY_DAY: DayJSON = {"times": []}


@pydantic.dataclasses.dataclass(config={"arbitrary_types_allowed": True})
class Schedule:
    start_weekday: int
    end_weekday: int
    start_hour: int
    end_hour: int
    start_minute: int
    end_minute: int
    # NOTE(charly): ZoneInfo is an arbitrary type
    tzinfo: zoneinfo.ZoneInfo
    is_only_days: bool = dataclasses.field(default=False, repr=False)
    is_only_times: bool = dataclasses.field(default=False, repr=False)

    class Serialized(typing.TypedDict):
        start_weekday: int
        end_weekday: int
        start_hour: int
        end_hour: int
        start_minute: int
        end_minute: int
        tzinfo: str
        is_only_days: bool
        is_only_times: bool

    @staticmethod
    def get_weekdays_from_string(days: str) -> tuple[int, int]:
        try:
            start_weekday, end_weekday = days.split("-")
        except ValueError:
            raise InvalidDate(f"Invalid schedule: missing separator in '{days}'")

        return (
            DayOfWeek.from_string(start_weekday),
            DayOfWeek.from_string(end_weekday),
        )

    @staticmethod
    def get_start_and_end_time_obj_from_string(times: str) -> tuple[Time, Time]:
        try:
            start_hourminute, end_hourminute = times.split("-")
        except ValueError:
            raise InvalidDate(f"Invalid schedule: missing separator in '{times}'")

        try:
            start_time = Time.from_string(start_hourminute, timezone_allowed=False)
        except TimezoneNotAllowed:
            raise InvalidDate(
                "Invalid schedule: schedule takes 1 timezone but 2 were given"
            )

        end_time = Time.from_string(end_hourminute)

        return (start_time, end_time)

    @classmethod
    def from_days_string(cls, days: str) -> Schedule:
        start_weekday, end_weekday = cls.get_weekdays_from_string(days)
        return cls(
            start_weekday=start_weekday,
            end_weekday=end_weekday,
            start_hour=0,
            end_hour=23,
            start_minute=0,
            end_minute=59,
            # TODO(Greesb): Allow timezone with only day of weeks
            tzinfo=UTC,
            is_only_days=True,
        )

    @classmethod
    def from_times_string(cls, times: str) -> Schedule:
        start_time_obj, end_time_obj = cls.get_start_and_end_time_obj_from_string(times)
        return cls(
            start_weekday=1,
            end_weekday=7,
            start_hour=start_time_obj.hour,
            end_hour=end_time_obj.hour,
            start_minute=start_time_obj.minute,
            end_minute=end_time_obj.minute,
            tzinfo=end_time_obj.tzinfo,
            is_only_times=True,
        )

    @classmethod
    def from_strings(
        cls,
        days: str,
        times: str,
    ) -> Schedule:
        start_weekday, end_weekday = cls.get_weekdays_from_string(days)
        start_time_obj, end_time_obj = cls.get_start_and_end_time_obj_from_string(times)

        return cls(
            start_weekday=start_weekday,
            end_weekday=end_weekday,
            start_hour=start_time_obj.hour,
            end_hour=end_time_obj.hour,
            start_minute=start_time_obj.minute,
            end_minute=end_time_obj.minute,
            tzinfo=end_time_obj.tzinfo,
        )

    @classmethod
    def from_string(cls, string: str) -> Schedule:
        days, has_times, times = string.partition(" ")
        if not has_times or not times:
            try:
                # Only days
                return cls.from_days_string(days)
            except InvalidDate:
                # Only hours+minutes
                return cls.from_times_string(days)
        else:
            # Days + Times
            return cls.from_strings(days, times)

    def __post_init__(self) -> None:
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
            Schedule | datetime.datetime,
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
                and self.tzinfo.key == other.tzinfo.key
            )

        # Allow to check if a datetime is in a schedule
        dother = other.astimezone(self.tzinfo)
        if self.start_weekday <= self.end_weekday:
            return (
                self.start_weekday <= dother.isoweekday() <= self.end_weekday
                and self.is_datetime_inside_time_schedule(dother, strict=False)
            )
        return (
            self.end_weekday <= dother.isoweekday()
            or dother.isoweekday() <= self.start_weekday
        ) and self.is_datetime_inside_time_schedule(dother, strict=False)

    def is_datetime_inside_day_schedule(self, time_to_check: datetime.datetime) -> bool:
        time_to_check_as_tz = time_to_check.astimezone(self.tzinfo)
        return (
            self.start_weekday > self.end_weekday
            and (
                self.start_weekday <= time_to_check_as_tz.isoweekday()
                or time_to_check_as_tz.isoweekday() <= self.end_weekday
            )
        ) or (
            self.start_weekday <= self.end_weekday
            and (
                self.start_weekday
                <= time_to_check_as_tz.isoweekday()
                <= self.end_weekday
            )
        )

    def is_datetime_inside_time_schedule(
        self,
        time_to_check: datetime.datetime,
        strict: bool,
    ) -> bool:
        return is_datetime_inside_time_range(
            time_to_check.astimezone(self.tzinfo),
            self.start_hour,
            self.start_minute,
            self.end_hour,
            self.end_minute,
            strict,
        )

    def get_next_datetime(self, from_time: datetime.datetime) -> datetime.datetime:
        """
        * If the `from_time` is out of the schedule,
          returns the next earliest datetime, from `from_time`, at which this `Schedule` will match.
        * If `from_time` is inside the schedule,
          returns a datetime.datetime 1 minute after the end of the schedule.
        """

        def return_as_origin_timezone(dt: datetime.datetime) -> datetime.datetime:
            return dt.astimezone(from_time.tzinfo)

        from_time_as_tz = from_time.astimezone(self.tzinfo)

        # Outside of the day schedule
        if not self.is_datetime_inside_day_schedule(from_time_as_tz):
            if (
                self.start_weekday > self.end_weekday
                or from_time_as_tz.isoweekday() < self.start_weekday
            ):
                # Next time is this week at the start of schedule
                from_time_as_tz += datetime.timedelta(
                    days=self.start_weekday - from_time_as_tz.isoweekday()
                )

            # self.start_weekday <= self.end_weekday and from_time_as_tz.isoweekday() >= self.end_weekday
            else:
                # Add the number of days missing to go to the starting weekday
                # of the next week
                from_time_as_tz += datetime.timedelta(
                    days=self.start_weekday + (7 - from_time_as_tz.isoweekday())
                )

            if self.is_only_days:
                return return_as_origin_timezone(
                    from_time_as_tz.replace(
                        hour=0,
                        minute=0,
                        second=0,
                        microsecond=0,
                    )
                )
        # Inside of day schedule and is only a day schedule
        elif self.is_only_days:
            # Next time is outside of day schedule
            if (
                self.start_weekday <= self.end_weekday
                or from_time_as_tz.isoweekday() < self.start_weekday
            ):
                # Next time is this week at the end of the schedule
                from_time_as_tz += datetime.timedelta(
                    days=(self.end_weekday + 1) - from_time_as_tz.isoweekday()
                )
            # self.start_weekday > self.end_weekday and from_time_as_tz.isoweekday() >= self.start_weekday
            else:
                # Next time is next week at the end of the schedule
                from_time_as_tz += datetime.timedelta(
                    days=7 - (from_time_as_tz.isoweekday() - (self.end_weekday + 1))
                )
            return return_as_origin_timezone(
                from_time_as_tz.replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
            )
        # Inside day+time schedule
        elif self.is_datetime_inside_time_schedule(from_time_as_tz, strict=False):
            # We are between the correct date+time range,
            # next try is 1 minute after the end of the schedule.
            return return_as_origin_timezone(
                from_time_as_tz.replace(
                    hour=self.end_hour,
                    minute=self.end_minute,
                    second=0,
                )
                + datetime.timedelta(minutes=1)
            )
        # Inside day schedule but oustide of hour+minute schedule
        elif from_time_as_tz.hour < self.start_hour or (
            from_time_as_tz.hour == self.start_hour
            and from_time_as_tz.minute < self.start_minute
        ):
            # We're in a good day but before start hour + start minute
            # The hour+minute replace is done at the end, this elif is just
            # for clarity.
            pass
        # Outside of hour+minute schedule and last day of schedule
        elif from_time_as_tz.isoweekday() == self.end_weekday:
            if self.start_weekday > self.end_weekday:
                # Next time is this week at the start of schedule
                from_time_as_tz += datetime.timedelta(
                    days=self.start_weekday - from_time_as_tz.isoweekday()
                )

            else:
                # Next time is next week at the start of the schedule
                from_time_as_tz += datetime.timedelta(
                    days=self.start_weekday + (7 - from_time_as_tz.isoweekday())
                )
        else:
            # Next time is next day at start hour + start minute
            from_time_as_tz += datetime.timedelta(days=1)

        return return_as_origin_timezone(
            from_time_as_tz.replace(
                hour=self.start_hour, minute=self.start_minute, second=1, microsecond=0
            )
        )

    def serialized(self) -> Schedule.Serialized:
        return {
            "start_weekday": self.start_weekday,
            "end_weekday": self.end_weekday,
            "start_hour": self.start_hour,
            "end_hour": self.end_hour,
            "start_minute": self.start_minute,
            "end_minute": self.end_minute,
            "tzinfo": self.tzinfo.key,
            "is_only_days": self.is_only_days,
            "is_only_times": self.is_only_times,
        }

    @classmethod
    def deserialize(cls, data: Schedule.Serialized) -> Schedule:
        return cls(
            start_weekday=data["start_weekday"],
            end_weekday=data["end_weekday"],
            start_hour=data["start_hour"],
            end_hour=data["end_hour"],
            start_minute=data["start_minute"],
            end_minute=data["end_minute"],
            tzinfo=zoneinfo.ZoneInfo(data["tzinfo"]),
            is_only_days=data["is_only_days"],
            is_only_times=data["is_only_times"],
        )

    def as_json_dict(self, reverse: bool = False) -> ScheduleJSON:
        return {
            "timezone": str(self.tzinfo),
            "days": {
                day: self._day_as_json_dict(DayOfWeek.from_string(day), reverse=reverse)
                for day in _LONG_WEEKDAY
            },
        }

    def _day_as_json_dict(self, day: DayOfWeek, reverse: bool) -> DayJSON:
        if self._is_day_in_schedule(day):
            if self.is_only_days:
                return EMPTY_DAY if reverse else FULL_DAY

            if reverse:
                return {"times": self._timeranges_as_json_reversed()}
            return {"times": self._timeranges_as_json()}

        return FULL_DAY if reverse else EMPTY_DAY

    def _is_day_in_schedule(self, day: DayOfWeek) -> bool:
        if self.start_weekday <= self.end_weekday:
            return self.start_weekday <= day <= self.end_weekday
        return day <= self.end_weekday or day >= self.start_weekday

    def _timeranges_as_json(self) -> list[TimeRangeJSON]:
        return [
            {
                "start_at": self._time_as_json_dict(self.start_hour, self.start_minute),
                "end_at": self._time_as_json_dict(self.end_hour, self.end_minute),
            }
        ]

    def _timeranges_as_json_reversed(self) -> list[TimeRangeJSON]:
        result: list[TimeRangeJSON] = []
        if (self.start_hour, self.start_minute) != (0, 0):
            result.append(
                {
                    "start_at": self._time_as_json_dict(0, 0),
                    "end_at": self._time_as_json_dict(
                        self.start_hour, self.start_minute
                    ),
                }
            )
        if (self.end_hour, self.end_minute) != (23, 59):
            result.append(
                {
                    "start_at": self._time_as_json_dict(self.end_hour, self.end_minute),
                    "end_at": self._time_as_json_dict(23, 59),
                }
            )
        return result

    def _time_as_json_dict(self, hour: int, minute: int) -> TimeJSON:
        return {"hour": hour, "minute": minute}


class UncertainDatePart:
    def __eq__(self, other: object) -> bool:
        # Needed for unit tests
        return isinstance(other, UncertainDatePart)


REGEX_DATETIME_WITH_UNCERTAIN_DIGITS = re.compile(
    # We don't need to match the timezone since it will be already split from
    # the date value when this regex will be used
    r"^(?P<year>\d{4}|X{4})-(?P<month>\d{2}|X{2})-(?P<day>\d{2}|X{2})[T ]?(?P<hour>[\d]{2}):(?P<minute>[\d]{2})"
)


@dataclasses.dataclass
class UncertainDate:
    year: int | UncertainDatePart
    month: int | UncertainDatePart
    day: int | UncertainDatePart
    hour: int
    minute: int
    tzinfo: zoneinfo.ZoneInfo = dataclasses.field(default=UTC)

    def __post_init__(self) -> None:
        if (
            isinstance(self.year, UncertainDatePart)
            and isinstance(self.month, UncertainDatePart)
            and isinstance(self.day, UncertainDatePart)
        ):
            raise InvalidDate(
                "Cannot have year, month and day as uncertain, use `schedule` condition instead"
            )

        if (
            isinstance(self.year, int)
            and isinstance(self.day, UncertainDatePart)
            and isinstance(self.month, UncertainDatePart)
        ):
            # This forbid the following case: 2023-XX-XX
            # NOTE(Greesb): It is forbidden at the moment because it will require
            # quite a bit of code to get working and i'm not sure this use case
            # make sense or will be used at all. So until a client requires it,
            # it will be forbidden.
            raise InvalidDate("Cannot have both month and day as uncertain")

    @classmethod
    def fromisoformat(cls, string: str, tzinfo: zoneinfo.ZoneInfo) -> UncertainDate:
        match = REGEX_DATETIME_WITH_UNCERTAIN_DIGITS.search(string)
        if not match:
            raise InvalidDate("The datetime format is invalid")

        values: dict[str, int | UncertainDatePart] = {}
        for group_name in ("year", "month", "day"):
            raw_value = match.group(group_name)
            if "X" in raw_value:
                values[group_name] = UncertainDatePart()
            else:
                try:
                    values[group_name] = int(raw_value)
                except ValueError:
                    raise InvalidDate("The datetime format is invalid")

        for group_name in ("hour", "minute"):
            # hour and minute cannot have uncertain digits
            raw_value = match.group(group_name)
            if raw_value is not None:
                values[group_name] = int(raw_value)

        return cls(**values, tzinfo=tzinfo)  # type: ignore[arg-type]

    def __eq__(self, other: object) -> bool:
        if isinstance(other, UncertainDate):
            return (
                self.year == other.year
                and self.month == other.month
                and self.day == other.day
                and (self.hour is None or other.hour is None or self.hour == other.hour)
                and (
                    self.minute is None
                    or other.minute is None
                    or self.minute == other.minute
                )
                and self.tzinfo.key == other.tzinfo.key
            )

        if isinstance(other, datetime.datetime):
            dt_as_utc = other.astimezone(self.tzinfo)
            return (
                self.year == dt_as_utc.year
                and self.month == dt_as_utc.month
                and self.day == dt_as_utc.day
                and (self.hour is None or self.hour == dt_as_utc.hour)
                and (self.minute is None or self.minute == dt_as_utc.minute)
            )

        raise ValueError(f"Unsupported comparison type: {type(other)}")

    def _as_datetime(self, other: datetime.datetime) -> datetime.datetime:
        dt_values = {}
        other = other.astimezone(self.tzinfo)
        for attr in ("year", "month", "day", "hour", "minute"):
            # Build a datetime with the `UncertainDatePart`
            # replaced by the values of the `other` datetime
            if isinstance(getattr(self, attr), UncertainDatePart):
                dt_values[attr] = getattr(other, attr)
            else:
                dt_values[attr] = getattr(self, attr)

        out_of_range = True
        dt: datetime.datetime
        while out_of_range:
            try:
                dt = datetime.datetime(**dt_values, tzinfo=self.tzinfo)
            except ValueError as e:
                if "day is out of range for month" not in str(e):
                    raise

                # We want to keep the correct day, so go try and find it
                # in the next months if the current month doesn't have the
                # day we want.
                # (eg, `31st` day in february doesn't exist, we will find the next `31st`
                # in march)
                dt_values["month"] += 1
                if dt_values["month"] > 12:
                    dt_values["month"] = 0
                    dt_values["year"] += 1
            else:
                break

        return dt

    def isoformat(self) -> str:
        return self._as_datetime(datetime.datetime.now(tz=self.tzinfo)).isoformat()

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, datetime.datetime):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        return self._as_datetime(other) >= other.astimezone(self.tzinfo)

    def __le__(self, other: object) -> bool:
        if not isinstance(other, datetime.datetime):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        return self._as_datetime(other) <= other.astimezone(self.tzinfo)

    def get_next_datetime(self, from_time: datetime.datetime) -> datetime.datetime:
        as_dt = self._as_datetime(from_time)

        if as_dt >= from_time:
            if isinstance(self.day, UncertainDatePart):
                return as_dt.replace(day=1)
            return as_dt

        # as_dt < from_time
        if isinstance(self.month, UncertainDatePart):
            # We want a specific day each month, if we are here that means
            # we already passed the day of the current month.
            # So we need to increment months to find the next specific day
            # in the following months.
            # We add a while loop just to make sure we land on the correct day,
            # for example if we want the 31st of each month, then we won't have a 31st for each month.
            as_dt += dateutil.relativedelta.relativedelta(months=1)
            while as_dt.day != self.day:
                as_dt += dateutil.relativedelta.relativedelta(months=1)

            return as_dt

        if isinstance(self.year, UncertainDatePart) and isinstance(
            self.day, UncertainDatePart
        ):
            return as_dt.replace(day=1) + dateutil.relativedelta.relativedelta(
                months=12
            )

        # Only `year` is `UncertainDatePart`
        return as_dt + dateutil.relativedelta.relativedelta(years=1)


def fromisoformat(s: str) -> datetime.datetime:
    """always returns an aware datetime object with UTC timezone"""
    if s[-1] == "Z":
        s = s[:-1]
    dt = datetime.datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def fromisoformat_with_zoneinfo(
    string: str,
) -> datetime.datetime:
    value, tzinfo = extract_timezone(string)

    try:
        return fromisoformat(value).replace(tzinfo=tzinfo)
    except ValueError:
        raise InvalidDate("Invalid timestamp")


def fromtimestamp(timestamp: float) -> datetime.datetime:
    """always returns an aware datetime object with UTC timezone"""
    return datetime.datetime.fromtimestamp(timestamp, UTC)


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


@dataclasses.dataclass
class DateTimeRange:
    start: datetime.datetime | UncertainDate
    end: datetime.datetime | UncertainDate

    def __eq__(self, other: object) -> bool:
        if not isinstance(
            other,
            DateTimeRange | datetime.datetime,
        ):
            raise ValueError(f"Unsupported comparison type: {type(other)}")

        if isinstance(other, DateTimeRange):
            return self.start == other.start and self.end == other.end

        return self.start <= other <= self.end

    def as_github_date_query(self) -> str:
        # https://docs.github.com/en/search-github/getting-started-with-searching-on-github/understanding-the-search-syntax#query-for-dates
        return f"{self.start.isoformat()}..{self.end.isoformat()}"

    @classmethod
    def _fromisoformat_or_uncertain_date(
        cls, string: str, tzinfo: zoneinfo.ZoneInfo
    ) -> datetime.datetime | UncertainDate:
        if "X" in string:
            return UncertainDate.fromisoformat(string, tzinfo)

        try:
            return fromisoformat(string).replace(tzinfo=tzinfo)
        except ValueError:
            raise InvalidDate("Invalid date/time range")

    @classmethod
    def fromisoformat_with_zoneinfo(cls, string: str) -> DateTimeRange:
        # NOTE(charly): Search for the position of the "/" that separate two
        # dates
        separator_match = re.search(r"/(?:\d{4}|X{4})-", string)
        if separator_match is None:
            raise InvalidDate("Invalid date/time range")

        start_str = string[: separator_match.start()]
        start_iso_str, start_tz = extract_timezone(start_str)

        end_str = string[separator_match.start() + 1 :]
        end_iso_str, end_tz = extract_timezone(end_str)

        # NOTE(charly): if the first TZ isn't set, we use the second (e.g.
        # "2007-03-01T13:00/2008-05-11T15:30[Europe/Paris]")
        if not has_timezone(start_str):
            start_tz = end_tz

        start = cls._fromisoformat_or_uncertain_date(start_iso_str, start_tz)
        end = cls._fromisoformat_or_uncertain_date(end_iso_str, end_tz)

        return cls(start, end)

    def get_next_datetime(self, from_time: datetime.datetime) -> datetime.datetime:
        if from_time <= self.start:
            if isinstance(self.start, datetime.datetime):
                return self.start
            return self.start.get_next_datetime(from_time)

        if from_time <= self.end:
            if isinstance(self.end, datetime.datetime):
                return self.end + datetime.timedelta(minutes=1)
            return self.end.get_next_datetime(from_time) + datetime.timedelta(minutes=1)

        # from_time > self.end > self.start
        if isinstance(self.start, UncertainDate):
            return self.start.get_next_datetime(from_time)

        return DT_MAX
