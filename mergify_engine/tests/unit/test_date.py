import datetime
import zoneinfo

from freezegun import freeze_time
import pytest

from mergify_engine import date


TZ_PARIS = zoneinfo.ZoneInfo("Europe/Paris")


@pytest.mark.parametrize(
    "value, expected",
    (
        (
            "2021-06-01",
            datetime.datetime(2021, 6, 1, tzinfo=date.UTC),
        ),
        (
            "2021-06-01 18:41:39Z",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=date.UTC),
        ),
        (
            "2021-06-01H18:41:39Z",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=date.UTC),
        ),
        (
            "2021-06-01T18:41:39",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=date.UTC),
        ),
        (
            "2021-06-01T18:41:39+00:00",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=date.UTC),
        ),
        (
            "2022-01-01T01:01:01+00:00",
            datetime.datetime(2022, 1, 1, 1, 1, 1, tzinfo=date.UTC),
        ),
        (
            "2022-01-01T01:01:01-02:00",
            datetime.datetime(2022, 1, 1, 3, 1, 1, tzinfo=date.UTC),
        ),
        (
            "2022-01-01T01:01:01+02:00",
            datetime.datetime(2021, 12, 31, 23, 1, 1, tzinfo=date.UTC),
        ),
    ),
)
def test_fromisoformat(value: str, expected: datetime.datetime) -> None:
    assert date.fromisoformat(value) == expected


@pytest.mark.parametrize(
    "dt,expected_string",
    [
        (
            datetime.datetime(2021, 6, 1, tzinfo=date.UTC),
            "2021-06-01 00:00 UTC",
        ),
        (
            datetime.datetime(2021, 12, 31, 23, 1, 1, tzinfo=date.UTC),
            "2021-12-31 23:01 UTC",
        ),
        (
            datetime.datetime(2021, 12, 31, 23, 1, 0, 999, tzinfo=date.UTC),
            "2021-12-31 23:01 UTC",
        ),
        (
            datetime.datetime(2021, 12, 31, 23, 0, 0, 999, tzinfo=date.UTC),
            "2021-12-31 23:00 UTC",
        ),
    ],
)
def test_pretty_datetime(dt: datetime.datetime, expected_string: str) -> None:
    assert date.pretty_datetime(dt) == expected_string


def test_time_compare() -> None:
    utc = date.UTC
    with freeze_time("2021-09-22T08:00:05", tz_offset=0):
        assert datetime.datetime(2021, 9, 22, 8, 0, 5, tzinfo=utc) >= date.Time(
            8, 0, utc
        )

    with freeze_time("2012-01-14T12:15:00", tz_offset=0):
        assert date.Time(12, 0, utc) < date.utcnow()
        assert date.Time(15, 45, utc) > date.utcnow()
        assert date.Time(12, 15, utc) == date.utcnow()
        assert date.utcnow() > date.Time(12, 0, utc)
        assert date.utcnow() < date.Time(15, 45, utc)
        assert date.utcnow() == date.Time(12, 15, utc)
        assert date.Time(13, 15, utc) == date.Time(13, 15, utc)
        assert date.Time(13, 15, utc) < date.Time(15, 15, utc)
        assert date.Time(15, 0, utc) > date.Time(5, 0, utc)

        # TZ that endup the same day
        zone = zoneinfo.ZoneInfo("Europe/Paris")
        assert date.Time(10, 0, zone) < date.utcnow()
        assert date.Time(18, 45, zone) > date.utcnow()
        assert date.Time(13, 15, zone) == date.utcnow()
        assert date.utcnow() > date.Time(10, 0, zone)
        assert date.utcnow() < date.Time(18, 45, zone)
        assert date.utcnow() == date.Time(13, 15, zone)
        assert date.Time(13, 15, zone) == date.Time(13, 15, zone)
        assert date.Time(13, 15, zone) < date.Time(15, 15, zone)
        assert date.Time(15, 0, zone) > date.Time(5, 0, zone)

        # TZ that endup the next day GMT + 13
        zone = zoneinfo.ZoneInfo("Pacific/Auckland")
        assert date.Time(0, 2, zone) < date.utcnow()
        assert date.Time(2, 9, zone) > date.utcnow()
        assert date.Time(1, 15, zone) == date.utcnow()
        assert date.utcnow() > date.Time(0, 2, zone)
        assert date.utcnow() < date.Time(2, 9, zone)
        assert date.utcnow() == date.Time(1, 15, zone)
        assert date.Time(13, 15, zone) == date.Time(13, 15, zone)
        assert date.Time(13, 15, zone) < date.Time(15, 15, zone)
        assert date.Time(15, 0, zone) > date.Time(5, 0, zone)

        assert date.utcnow() == date.utcnow()
        assert (date.utcnow() > date.utcnow()) is False


@pytest.mark.parametrize(
    "dow,expected_int",
    [
        ("MON", 1),
        ("wed", 3),
        ("Sun", 7),
        ("FRI", 5),
        ("monday", 1),
        ("tuesday", 2),
        ("WEDNESDAY", 3),
        ("thursday", 4),
        ("fRiday", 5),
        ("SATURDAY", 6),
        ("sunday", 7),
    ],
)
def test_day_of_week_from_string(dow: str, expected_int: int) -> None:
    assert date.DayOfWeek.from_string(dow) == expected_int


@pytest.mark.parametrize(
    "string,expected_value",
    [
        ("7 days ago", "2021-09-15T08:00:05"),
        ("7 days 2:05 ago", "2021-09-15T05:55:05"),
        ("2:05 ago", "2021-09-22T05:55:05"),
    ],
)
def test_relative_datetime_from_string(string: str, expected_value: str) -> None:
    with freeze_time("2021-09-22T08:00:05", tz_offset=0):
        dt = date.RelativeDatetime.from_string(string)
        assert dt.value == date.fromisoformat(expected_value)


def test_relative_datetime_without_timezone() -> None:
    with pytest.raises(date.InvalidDate):
        date.RelativeDatetime(datetime.datetime.utcnow())


@pytest.mark.parametrize(
    "time,expected_hour,expected_minute,expected_tzinfo",
    [
        ("10:00", 10, 0, date.UTC),
        ("11:22[Europe/Paris]", 11, 22, zoneinfo.ZoneInfo("Europe/Paris")),
    ],
)
def test_time_from_string(
    time: str,
    expected_hour: int,
    expected_minute: int,
    expected_tzinfo: zoneinfo.ZoneInfo,
) -> None:
    t = date.Time.from_string(time)
    assert t.hour == expected_hour
    assert t.minute == expected_minute
    assert t.tzinfo == expected_tzinfo


@pytest.mark.parametrize(
    "date_type,value,expected_message",
    [
        (date.Time, "10:20[Invalid]", "Invalid timezone"),
        (date.Time, "36:20", "Hour must be between 0 and 23"),
        (date.Time, "16:120", "Minute must be between 0 and 59"),
        (date.Time, "36", "Invalid time"),
        (date.RelativeDatetime, "36 ago", "Invalid relative date"),
        (date.RelativeDatetime, "36 days", "Invalid relative date"),
        (date.RelativeDatetime, "10:20", "Invalid relative date"),
    ],
)
def test_invalid_date_string(
    date_type: type[date.RelativeDatetime | date.Time],
    value: str,
    expected_message: str,
) -> None:
    with pytest.raises(date.InvalidDate) as exc:
        date_type.from_string(value)

    assert exc.value.message == expected_message


@pytest.mark.parametrize(
    "value,expected_interval",
    [
        ("1 days", datetime.timedelta(days=1)),
        ("1 day", datetime.timedelta(days=1)),
        ("1 d", datetime.timedelta(days=1)),
        ("1 hours", datetime.timedelta(hours=1)),
        ("1 hour", datetime.timedelta(hours=1)),
        ("1 h", datetime.timedelta(hours=1)),
        ("1 minutes", datetime.timedelta(minutes=1)),
        ("1 minute", datetime.timedelta(minutes=1)),
        ("1 m", datetime.timedelta(minutes=1)),
        ("1 seconds", datetime.timedelta(seconds=1)),
        ("1 second", datetime.timedelta(seconds=1)),
        ("1 s", datetime.timedelta(seconds=1)),
        ("1s", datetime.timedelta(seconds=1)),
        (
            "1 days 15 hours 6 minutes 42 seconds",
            datetime.timedelta(days=1, hours=15, minutes=6, seconds=42),
        ),
        (
            "1days 15hours 6min 42s",
            datetime.timedelta(days=1, hours=15, minutes=6, seconds=42),
        ),
        (
            "1 d +15 hour 6 m 42 seconds",
            datetime.timedelta(days=1, hours=15, minutes=6, seconds=42),
        ),
        (
            "1 d 15 h +6 m 42 s",
            datetime.timedelta(days=1, hours=15, minutes=6, seconds=42),
        ),
        (
            "1 d 15 hour 42 seconds",
            datetime.timedelta(days=1, hours=15, minutes=0, seconds=42),
        ),
        (
            "1 d 15 hour 6 m",
            datetime.timedelta(days=1, hours=15, minutes=6, seconds=0),
        ),
        (
            "1 d +6 minute 42 s",
            datetime.timedelta(days=1, hours=0, minutes=6, seconds=42),
        ),
        (
            "1 d 6 m 42 seconds",
            datetime.timedelta(days=1, hours=0, minutes=6, seconds=42),
        ),
        (
            "-1 d -6 m 42 seconds",
            datetime.timedelta(days=-1, hours=0, minutes=-6, seconds=42),
        ),
        (
            "1 d -6 m 42 seconds",
            datetime.timedelta(days=1, hours=0, minutes=-6, seconds=42),
        ),
        ("whater", None),
        ("1 foo 2 bar", None),
    ],
)
def test_interval_from_string(
    value: str, expected_interval: datetime.timedelta | None
) -> None:
    if expected_interval is None:
        with pytest.raises(date.InvalidDate):
            date.interval_from_string(value)
    else:
        assert date.interval_from_string(value) == expected_interval


@pytest.mark.parametrize(
    "time_to_check,begin_hour,begin_minute,end_hour,end_minute,strict,result",
    (
        (datetime.datetime(2022, 1, 1, 20, 10, 1), 20, 10, 21, 0, False, True),
        (datetime.datetime(2022, 1, 1, 20, 10, 0), 20, 10, 21, 0, False, True),
        (datetime.datetime(2022, 1, 1, 20, 10, 0), 20, 10, 21, 0, True, False),
        (datetime.datetime(2022, 1, 1, 20, 10, 1), 20, 10, 21, 0, True, True),
        (
            datetime.datetime(
                2022, 1, 1, 20, 10, 1, tzinfo=zoneinfo.ZoneInfo("Pacific/Auckland")
            ),
            20,
            10,
            21,
            0,
            False,
            True,
        ),
    ),
)
def test_datetime_inside_time_range(
    time_to_check: datetime.datetime,
    begin_hour: int,
    begin_minute: int,
    end_hour: int,
    end_minute: int,
    strict: bool,
    result: bool,
) -> None:
    assert (
        date.is_datetime_inside_time_range(
            time_to_check, begin_hour, begin_minute, end_hour, end_minute, strict
        )
        == result
    )


@pytest.mark.parametrize(
    "schedule,from_time,expected",
    (
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Friday, 15:00 UTC
            datetime.datetime(2022, 11, 11, 15, tzinfo=date.UTC),
            # Friday, 15:00:01 UTC
            datetime.datetime(2022, 11, 11, 15, 0, 1, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Monday, 16:00 UTC
            datetime.datetime(2022, 11, 7, 16, tzinfo=date.UTC),
            # Tuesday, 7:00 UTC
            datetime.datetime(2022, 11, 8, 7, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Friday, 16:00 UTC
            datetime.datetime(2022, 11, 11, 16, tzinfo=date.UTC),
            # Saturday, 7:00 UTC
            datetime.datetime(2022, 11, 12, 7, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Monday, 16:00 UTC
            datetime.datetime(2022, 11, 7, 16, tzinfo=date.UTC),
            # Friday, 7:00:01 UTC
            datetime.datetime(2022, 11, 11, 7, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Saturday, 14:00 UTC
            datetime.datetime(2022, 11, 5, 14, tzinfo=date.UTC),
            # Monday, 7:00 UTC
            datetime.datetime(2022, 11, 7, 7, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Saturday, 16:00 UTC
            datetime.datetime(2022, 11, 5, 16, tzinfo=date.UTC),
            # Monday, 7:00 UTC
            datetime.datetime(2022, 11, 7, 7, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("MON-FRI"),
            # Saturday, 16:00 UTC
            datetime.datetime(2022, 11, 5, 16, tzinfo=date.UTC),
            # Monday, 00:00 UTC
            datetime.datetime(2022, 11, 7, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("TUE-FRI"),
            # Monday, 16:00 UTC
            datetime.datetime(2022, 11, 7, 16, tzinfo=date.UTC),
            # Tuesday, 00:00 UTC
            datetime.datetime(2022, 11, 8, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("MON-FRI"),
            # Friday, 15:00 UTC
            datetime.datetime(2022, 11, 4, 15, tzinfo=date.UTC),
            # Saturday, 00:00 UTC
            datetime.datetime(2022, 11, 5, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("MON-FRI"),
            # Tuesday, 15:00 UTC
            datetime.datetime(2022, 11, 1, 15, tzinfo=date.UTC),
            # Saturday, 00:00 UTC
            datetime.datetime(2022, 11, 5, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("FRI-MON"),
            # Saturday, 16:00 UTC
            datetime.datetime(2022, 11, 5, 16, tzinfo=date.UTC),
            # Tuesday, 00:00 UTC
            datetime.datetime(2022, 11, 8, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("FRI-MON"),
            # Monday, 15:00 UTC
            datetime.datetime(2022, 11, 7, 15, tzinfo=date.UTC),
            # Tuesday, 00:00 UTC
            datetime.datetime(2022, 11, 8, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("FRI-TUE"),
            # Sunday, 15:00 UTC
            datetime.datetime(2022, 11, 6, 15, tzinfo=date.UTC),
            # Wednesday, 00:00 UTC
            datetime.datetime(2022, 11, 9, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("FRI-TUE"),
            # Monday, 15:00 UTC
            datetime.datetime(2022, 11, 7, 15, tzinfo=date.UTC),
            # Wednesday, 00:00 UTC
            datetime.datetime(2022, 11, 9, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("08:10-17:30"),
            # Monday, 15:00 UTC
            datetime.datetime(2022, 11, 7, 15, tzinfo=date.UTC),
            # Monday, 17:30:01 UTC
            datetime.datetime(2022, 11, 7, 17, 30, 1, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("08:10-17:30"),
            # Monday, 18:00 UTC
            datetime.datetime(2022, 11, 7, 18, tzinfo=date.UTC),
            # Tuesday, 08:10:00 UTC
            datetime.datetime(2022, 11, 8, 8, 10, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("08:10-17:30"),
            # Sunday, 18:00 UTC
            datetime.datetime(2022, 11, 6, 18, tzinfo=date.UTC),
            # Monday, 08:10:00 UTC
            datetime.datetime(2022, 11, 7, 8, 10, tzinfo=date.UTC),
        ),
        (
            date.Schedule.from_string("08:10-17:30"),
            # Monday, 00:00 UTC
            datetime.datetime(2022, 11, 6, tzinfo=date.UTC),
            # Monday, 08:10:00 UTC
            datetime.datetime(2022, 11, 6, 8, 10, tzinfo=date.UTC),
        ),
    ),
)
def test_schedule_next_datetime(
    schedule: date.Schedule, from_time: datetime.datetime, expected: datetime.datetime
) -> None:
    assert schedule.get_next_datetime(from_time) == expected


@pytest.mark.parametrize(
    "schedule1,schedule2",
    (
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            date.Schedule.from_string("monday-friday 7:00-15:00"),
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            date.Schedule.from_string("MON-FRI 7:00-15:00[UTC]"),
        ),
        # We don't support timezone alias equality yet, but here is the tests
        # (
        #     date.Schedule.from_string("MON-FRI 7:00-15:00[Etc/Universal]"),
        #     date.Schedule.from_string("MON-FRI 7:00-15:00[UTC]"),
        # ),
        # (
        #     date.Schedule.from_string("MON-FRI 7:00-15:00[Europe/Rome]"),
        #     date.Schedule.from_string("MON-FRI 7:00-15:00[Europe/Vatican]"),
        # ),
    ),
)
def test_schedule_equality(schedule1: date.Schedule, schedule2: date.Schedule) -> None:
    # In winter
    with freeze_time("2023-01-01T00:00:00", tz_offset=0):
        assert schedule1 == schedule2
    # In summer
    with freeze_time("2023-07-01T00:00:00", tz_offset=0):
        assert schedule1 == schedule2


@pytest.mark.parametrize(
    "schedule1,schedule2",
    (
        (
            date.Schedule.from_string("MON-FRI"),
            date.Schedule.from_string("MON-SAT"),
        ),
        (
            date.Schedule.from_string("11:00-12:00"),
            date.Schedule.from_string("11:00-12:01"),
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00[Europe/London]"),
            date.Schedule.from_string("MON-FRI 7:00-15:00[UTC]"),
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00[Europe/London]"),
            date.Schedule.from_string("MON-FRI 7:00-15:00[Africa/Abidjan]"),
        ),
    ),
)
def test_schedule_inequality(
    schedule1: date.Schedule, schedule2: date.Schedule
) -> None:
    # In winter
    with freeze_time("2023-01-01T00:00:00", tz_offset=0):
        assert schedule1 != schedule2
    # In summer
    with freeze_time("2023-07-01T00:00:00", tz_offset=0):
        assert schedule1 != schedule2


def test_schedule_equality_after_clear_cache() -> None:
    schedule1 = date.Schedule.from_string("MON-FRI 7:00-15:00[UTC]")
    zoneinfo.ZoneInfo.clear_cache()
    schedule2 = date.Schedule.from_string("MON-FRI 7:00-15:00[UTC]")
    assert schedule1 == schedule2


@pytest.mark.parametrize(
    "schedule,date_to_test,expected",
    (
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Friday, 15:00 UTC
            datetime.datetime(2022, 11, 11, 15, tzinfo=date.UTC),
            True,
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Friday, 15:00:01 UTC
            datetime.datetime(2022, 11, 11, 15, 0, 1, tzinfo=date.UTC),
            False,
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Sunday, 14:00 UTC
            datetime.datetime(2022, 11, 12, 14, tzinfo=date.UTC),
            False,
        ),
        (
            date.Schedule.from_string("MON-FRI 7:00-15:00"),
            # Sunday, 16:00 UTC
            datetime.datetime(2022, 11, 12, 16, tzinfo=date.UTC),
            False,
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Monday, 8:00 UTC
            datetime.datetime(2022, 11, 14, 8, tzinfo=date.UTC),
            True,
        ),
        (
            date.Schedule.from_string("FRI-TUE 7:00-15:00"),
            # Monday, 8:00 UTC
            datetime.datetime(2022, 11, 14, 8, tzinfo=date.UTC),
            True,
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Tuesday, 8:00 UTC
            datetime.datetime(2022, 11, 15, 8, tzinfo=date.UTC),
            True,
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Friday, 15:00 UTC
            datetime.datetime(2022, 11, 11, 15, tzinfo=date.UTC),
            True,
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Friday, 16:00 UTC
            datetime.datetime(2022, 11, 11, 16, tzinfo=date.UTC),
            False,
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Saturday, 14:00 UTC
            datetime.datetime(2022, 11, 12, 14, tzinfo=date.UTC),
            True,
        ),
        (
            date.Schedule.from_string("FRI-MON 7:00-15:00"),
            # Saturday, 16:00 UTC
            datetime.datetime(2022, 11, 12, 16, tzinfo=date.UTC),
            False,
        ),
        (
            date.Schedule.from_string("FRI-FRI"),
            # Saturday, 16:00 UTC
            datetime.datetime(2022, 11, 12, 16, tzinfo=date.UTC),
            False,
        ),
        (
            date.Schedule.from_string("SAT-SAT"),
            # Saturday, 16:00 UTC
            datetime.datetime(2022, 11, 12, 16, tzinfo=date.UTC),
            True,
        ),
    ),
)
def test_schedule_eq_with_datetime(
    schedule: date.Schedule, date_to_test: datetime.datetime, expected: bool
) -> None:
    assert expected == (date_to_test == schedule)
    assert not expected == (date_to_test != schedule)


def test_schedule_as_json_dict_only_days() -> None:
    schedule = date.Schedule.from_string("MON-FRI")
    assert schedule.as_json_dict() == {
        "timezone": "UTC",
        "days": {
            "monday": date.FULL_DAY,
            "tuesday": date.FULL_DAY,
            "wednesday": date.FULL_DAY,
            "thursday": date.FULL_DAY,
            "friday": date.FULL_DAY,
            "saturday": date.EMPTY_DAY,
            "sunday": date.EMPTY_DAY,
        },
    }
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "UTC",
        "days": {
            "monday": date.EMPTY_DAY,
            "tuesday": date.EMPTY_DAY,
            "wednesday": date.EMPTY_DAY,
            "thursday": date.EMPTY_DAY,
            "friday": date.EMPTY_DAY,
            "saturday": date.FULL_DAY,
            "sunday": date.FULL_DAY,
        },
    }


def test_schedule_as_json_dict_only_days_reversed() -> None:
    schedule = date.Schedule.from_string("FRI-MON")
    assert schedule.as_json_dict() == {
        "timezone": "UTC",
        "days": {
            "monday": date.FULL_DAY,
            "tuesday": date.EMPTY_DAY,
            "wednesday": date.EMPTY_DAY,
            "thursday": date.EMPTY_DAY,
            "friday": date.FULL_DAY,
            "saturday": date.FULL_DAY,
            "sunday": date.FULL_DAY,
        },
    }
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "UTC",
        "days": {
            "monday": date.EMPTY_DAY,
            "tuesday": date.FULL_DAY,
            "wednesday": date.FULL_DAY,
            "thursday": date.FULL_DAY,
            "friday": date.EMPTY_DAY,
            "saturday": date.EMPTY_DAY,
            "sunday": date.EMPTY_DAY,
        },
    }


def test_schedule_as_json_dict_days_and_times() -> None:
    schedule = date.Schedule.from_string("MON-FRI 11:00-15:30")
    expected_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 11, "minute": 0},
                "end_at": {"hour": 15, "minute": 30},
            },
        ]
    }
    assert schedule.as_json_dict() == {
        "timezone": "UTC",
        "days": {
            "monday": expected_day,
            "tuesday": expected_day,
            "wednesday": expected_day,
            "thursday": expected_day,
            "friday": expected_day,
            "saturday": date.EMPTY_DAY,
            "sunday": date.EMPTY_DAY,
        },
    }

    expected_reversed_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 0, "minute": 0},
                "end_at": {"hour": 11, "minute": 0},
            },
            {
                "start_at": {"hour": 15, "minute": 30},
                "end_at": {"hour": 23, "minute": 59},
            },
        ]
    }
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "UTC",
        "days": {
            "monday": expected_reversed_day,
            "tuesday": expected_reversed_day,
            "wednesday": expected_reversed_day,
            "thursday": expected_reversed_day,
            "friday": expected_reversed_day,
            "saturday": date.FULL_DAY,
            "sunday": date.FULL_DAY,
        },
    }


def test_schedule_as_json_dict_only_times() -> None:
    schedule = date.Schedule.from_string("9:00-16:32")
    expected_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 9, "minute": 0},
                "end_at": {"hour": 16, "minute": 32},
            },
        ]
    }
    assert schedule.as_json_dict() == {
        "timezone": "UTC",
        "days": {
            "monday": expected_day,
            "tuesday": expected_day,
            "wednesday": expected_day,
            "thursday": expected_day,
            "friday": expected_day,
            "saturday": expected_day,
            "sunday": expected_day,
        },
    }

    expected_reversed_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 0, "minute": 0},
                "end_at": {"hour": 9, "minute": 0},
            },
            {
                "start_at": {"hour": 16, "minute": 32},
                "end_at": {"hour": 23, "minute": 59},
            },
        ]
    }
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "UTC",
        "days": {
            "monday": expected_reversed_day,
            "tuesday": expected_reversed_day,
            "wednesday": expected_reversed_day,
            "thursday": expected_reversed_day,
            "friday": expected_reversed_day,
            "saturday": expected_reversed_day,
            "sunday": expected_reversed_day,
        },
    }


def test_schedule_as_json_dict_only_times_with_timezone() -> None:
    schedule = date.Schedule.from_string("10:02-22:35[Europe/Paris]")
    expected_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 10, "minute": 2},
                "end_at": {"hour": 22, "minute": 35},
            },
        ]
    }
    assert schedule.as_json_dict() == {
        "timezone": "Europe/Paris",
        "days": {
            "monday": expected_day,
            "tuesday": expected_day,
            "wednesday": expected_day,
            "thursday": expected_day,
            "friday": expected_day,
            "saturday": expected_day,
            "sunday": expected_day,
        },
    }

    expected_reversed_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 0, "minute": 0},
                "end_at": {"hour": 10, "minute": 2},
            },
            {
                "start_at": {"hour": 22, "minute": 35},
                "end_at": {"hour": 23, "minute": 59},
            },
        ]
    }
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "Europe/Paris",
        "days": {
            "monday": expected_reversed_day,
            "tuesday": expected_reversed_day,
            "wednesday": expected_reversed_day,
            "thursday": expected_reversed_day,
            "friday": expected_reversed_day,
            "saturday": expected_reversed_day,
            "sunday": expected_reversed_day,
        },
    }


def test_schedule_as_json_reversed_edge_cases() -> None:
    schedule = date.Schedule.from_string("00:00-12:00")
    expected_reversed_day: date.DayJSON = {
        "times": [
            {
                "start_at": {"hour": 12, "minute": 0},
                "end_at": {"hour": 23, "minute": 59},
            },
        ]
    }
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "UTC",
        "days": {
            "monday": expected_reversed_day,
            "tuesday": expected_reversed_day,
            "wednesday": expected_reversed_day,
            "thursday": expected_reversed_day,
            "friday": expected_reversed_day,
            "saturday": expected_reversed_day,
            "sunday": expected_reversed_day,
        },
    }

    schedule = date.Schedule.from_string("12:00-23:59")
    expected_reversed_day = {
        "times": [
            {
                "start_at": {"hour": 0, "minute": 0},
                "end_at": {"hour": 12, "minute": 0},
            },
        ]
    }
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "UTC",
        "days": {
            "monday": expected_reversed_day,
            "tuesday": expected_reversed_day,
            "wednesday": expected_reversed_day,
            "thursday": expected_reversed_day,
            "friday": expected_reversed_day,
            "saturday": expected_reversed_day,
            "sunday": expected_reversed_day,
        },
    }

    schedule = date.Schedule.from_string("00:00-23:59")
    assert schedule.as_json_dict(reverse=True) == {
        "timezone": "UTC",
        "days": {
            "monday": date.EMPTY_DAY,
            "tuesday": date.EMPTY_DAY,
            "wednesday": date.EMPTY_DAY,
            "thursday": date.EMPTY_DAY,
            "friday": date.EMPTY_DAY,
            "saturday": date.EMPTY_DAY,
            "sunday": date.EMPTY_DAY,
        },
    }


def test_schedule_dict_serialization() -> None:
    schedule = date.Schedule.from_string("MON-FRI 10:02-22:35[Europe/Paris]")
    expected_dict = {
        "start_weekday": 1,
        "end_weekday": 5,
        "start_hour": 10,
        "end_hour": 22,
        "start_minute": 2,
        "end_minute": 35,
        "tzinfo": "Europe/Paris",
        "is_only_days": False,
        "is_only_times": False,
    }

    assert schedule.serialized() == expected_dict
    assert schedule.deserialize(schedule.serialized()) == schedule


def test_datetimerange_as_github_date_query() -> None:
    r = date.DateTimeRange(
        datetime.datetime(2023, 5, 30, 15, 30, tzinfo=datetime.UTC),
        datetime.datetime(2023, 5, 30, 16, 0, tzinfo=datetime.UTC),
    )
    assert (
        r.as_github_date_query()
        == "2023-05-30T15:30:00+00:00..2023-05-30T16:00:00+00:00"
    )


@pytest.mark.parametrize(
    "isoformat_datetime,expected_datetime",
    (
        (
            "2023-07-13",
            datetime.datetime(2023, 7, 13, tzinfo=date.UTC),
        ),
        (
            "2023-07-13T14:00",
            datetime.datetime(2023, 7, 13, 14, tzinfo=date.UTC),
        ),
        (
            "2023-07-13T14:00Z",
            datetime.datetime(2023, 7, 13, 14, tzinfo=date.UTC),
        ),
        (
            "2023-07-13T14:00+02:00",
            datetime.datetime(2023, 7, 13, 12, tzinfo=date.UTC),
        ),
        (
            "2023-07-13T14:00+02",
            datetime.datetime(2023, 7, 13, 12, tzinfo=date.UTC),
        ),
        (
            "2023-07-13T14:00[Europe/Paris]",
            datetime.datetime(
                2023, 7, 13, 14, tzinfo=zoneinfo.ZoneInfo("Europe/Paris")
            ),
        ),
    ),
)
def test_fromisoformat_with_zoneinfo(
    isoformat_datetime: str, expected_datetime: datetime.datetime
) -> None:
    assert date.fromisoformat_with_zoneinfo(isoformat_datetime) == expected_datetime


@pytest.mark.parametrize(
    "isoformat_datetime",
    (
        "2023-07",
        "2023-07-12-",
        "2023-07-13T14:00[Europe/Paris",
        "2023-07-13T14:00Europe/Paris]",
        "2023-07-13T14:00[WTF]",
        "2023-07-13T14:00+25",
    ),
)
def test_fromisoformat_with_zoneinfo_invalid(isoformat_datetime: str) -> None:
    with pytest.raises(date.InvalidDate):
        assert date.fromisoformat_with_zoneinfo(isoformat_datetime)


@pytest.mark.parametrize(
    "isoformat_dtr,expected_dtr",
    (
        (
            "2007-03-01T13:00/2008-05-11T15:30",
            date.DateTimeRange(
                datetime.datetime(2007, 3, 1, 13, tzinfo=date.UTC),
                datetime.datetime(2008, 5, 11, 15, 30, tzinfo=date.UTC),
            ),
        ),
        (
            "2007-03-01T13:00:00Z/2008-05-11T15:30:00Z",
            date.DateTimeRange(
                datetime.datetime(2007, 3, 1, 13, tzinfo=date.UTC),
                datetime.datetime(2008, 5, 11, 15, 30, tzinfo=date.UTC),
            ),
        ),
        (
            "2007-03-01T13:00:00[Europe/Paris]/2008-05-11T15:30:00[Europe/Paris]",
            date.DateTimeRange(
                datetime.datetime(2007, 3, 1, 13, tzinfo=TZ_PARIS),
                datetime.datetime(2008, 5, 11, 15, 30, tzinfo=TZ_PARIS),
            ),
        ),
        (
            "2007-03-01T13:00:00Z/2008-05-11T15:30:00[Europe/Paris]",
            date.DateTimeRange(
                datetime.datetime(2007, 3, 1, 13, tzinfo=date.UTC),
                datetime.datetime(2008, 5, 11, 15, 30, tzinfo=TZ_PARIS),
            ),
        ),
        (
            "2007-03-01T13:00:00[UTC]/2008-05-11T15:30:00[Europe/Paris]",
            date.DateTimeRange(
                datetime.datetime(2007, 3, 1, 13, tzinfo=date.UTC),
                datetime.datetime(2008, 5, 11, 15, 30, tzinfo=TZ_PARIS),
            ),
        ),
        (
            "2007-03-01T13:00:00/2008-05-11T15:30:00[Europe/Paris]",
            date.DateTimeRange(
                datetime.datetime(2007, 3, 1, 13, tzinfo=TZ_PARIS),
                datetime.datetime(2008, 5, 11, 15, 30, tzinfo=TZ_PARIS),
            ),
        ),
    ),
)
def test_datetimerange_fromisoformat_with_zoneinfo(
    isoformat_dtr: str, expected_dtr: date.DateTimeRange
) -> None:
    assert date.DateTimeRange.fromisoformat_with_zoneinfo(isoformat_dtr) == expected_dtr


@pytest.mark.parametrize(
    "isoformat_dtr",
    (
        "2023-07-12",
        "2023-07-13T14:00:00Z",
        "2023-07-13T14:00[Europe/Paris]",
        "2007-03-01T13:00/wtf",
        "2007-03-01T13:00/2008-wtf",
        "2007-03-01T13:00/2008-05-11T15:30[WTF]",
        "2007-03-01T13:00/15:30",
    ),
)
def test_datetimerange_fromisoformat_with_zoneinfo_invalid(isoformat_dtr: str) -> None:
    with pytest.raises(date.InvalidDate):
        assert date.DateTimeRange.fromisoformat_with_zoneinfo(isoformat_dtr)


@pytest.mark.parametrize(
    "actual,other",
    (
        (
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
        ),
        (
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
            datetime.datetime(2007, 3, 1, 13, tzinfo=date.UTC),
        ),
        (
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
            datetime.datetime(2008, 5, 11, 15, 30, tzinfo=date.UTC),
        ),
    ),
)
def test_datetimerange_equality(actual: date.DateTimeRange, other: object) -> None:
    assert actual == other


@pytest.mark.parametrize(
    "actual,other",
    (
        (
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2007-05-11T15:30"
            ),
        ),
        (
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2006-03-01T13:00/2008-05-11T15:30"
            ),
        ),
        (
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
            datetime.datetime(2007, 3, 1, 12, tzinfo=date.UTC),
        ),
        (
            date.DateTimeRange.fromisoformat_with_zoneinfo(
                "2007-03-01T13:00/2008-05-11T15:30"
            ),
            datetime.datetime(2008, 5, 11, 15, 31, tzinfo=date.UTC),
        ),
    ),
)
def test_datetimerange_inequality(actual: date.DateTimeRange, other: object) -> None:
    assert actual != other


@pytest.mark.parametrize(
    "current_date,months,years,expected_date",
    (
        (
            "2023-10-16",
            1,
            0,
            "2023-11-16",
        ),
        (
            "2023-10-16",
            0,
            1,
            "2024-10-16",
        ),
        (
            "2023-10-16",
            0,
            -1,
            "2022-10-16",
        ),
        (
            "2023-10-16",
            -1,
            0,
            "2023-09-16",
        ),
        (
            "2023-10-16",
            -1,
            -1,
            "2022-09-16",
        ),
        (
            "2023-10-16",
            -6,
            1,
            "2024-04-16",
        ),
        (
            "2023-01-31",
            2,
            0,
            "2023-03-31",
        ),
        (
            "2023-01-31",
            1,
            0,
            "2023-02-28",
        ),
        (
            "2023-03-31",
            -2,
            0,
            "2023-01-31",
        ),
        (
            "2023-03-31",
            -1,
            0,
            "2023-02-28",
        ),
        (
            "2023-03-31",
            0,
            0,
            "2023-03-31",
        ),
    ),
)
def test_relativedelta(
    current_date: str, months: int, years: int, expected_date: str
) -> None:
    assert date.relativedelta(
        date.fromisoformat(current_date), months=months, years=years
    ) == date.fromisoformat(expected_date)
