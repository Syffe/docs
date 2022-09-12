import datetime
import typing
import zoneinfo

from freezegun import freeze_time
import pytest

from mergify_engine import date


@pytest.mark.parametrize(
    "value, expected",
    (
        (
            "2021-06-01",
            datetime.datetime(2021, 6, 1, tzinfo=datetime.timezone.utc),
        ),
        (
            "2021-06-01 18:41:39Z",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=datetime.timezone.utc),
        ),
        (
            "2021-06-01H18:41:39Z",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=datetime.timezone.utc),
        ),
        (
            "2021-06-01T18:41:39",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=datetime.timezone.utc),
        ),
        (
            "2021-06-01T18:41:39+00:00",
            datetime.datetime(2021, 6, 1, 18, 41, 39, tzinfo=datetime.timezone.utc),
        ),
        (
            "2022-01-01T01:01:01+00:00",
            datetime.datetime(2022, 1, 1, 1, 1, 1, tzinfo=datetime.timezone.utc),
        ),
        (
            "2022-01-01T01:01:01-02:00",
            datetime.datetime(2022, 1, 1, 3, 1, 1, tzinfo=datetime.timezone.utc),
        ),
        (
            "2022-01-01T01:01:01+02:00",
            datetime.datetime(2021, 12, 31, 23, 1, 1, tzinfo=datetime.timezone.utc),
        ),
    ),
)
def test_fromisoformat(value: str, expected: datetime.datetime) -> None:
    assert date.fromisoformat(value) == expected


@pytest.mark.parametrize(
    "dt,expected_string",
    [
        (
            datetime.datetime(2021, 6, 1, tzinfo=datetime.timezone.utc),
            "2021-06-01 00:00 UTC",
        ),
        (
            datetime.datetime(2021, 12, 31, 23, 1, 1, tzinfo=datetime.timezone.utc),
            "2021-12-31 23:01 UTC",
        ),
        (
            datetime.datetime(
                2021, 12, 31, 23, 1, 0, 999, tzinfo=datetime.timezone.utc
            ),
            "2021-12-31 23:01 UTC",
        ),
        (
            datetime.datetime(
                2021, 12, 31, 23, 0, 0, 999, tzinfo=datetime.timezone.utc
            ),
            "2021-12-31 23:00 UTC",
        ),
    ],
)
def test_pretty_datetime(dt: datetime.datetime, expected_string: str) -> None:
    assert date.pretty_datetime(dt) == expected_string


def test_time_compare() -> None:
    utc = datetime.timezone.utc
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
    "dow,expected_string",
    [
        ("MON", "Mon"),
        ("wed", "Wed"),
        ("Sun", "Sun"),
        ("FRI", "Fri"),
        ("monday", "Mon"),
        ("tuesday", "Tue"),
        ("WEDNESDAY", "Wed"),
        ("thursday", "Thu"),
        ("fRiday", "Fri"),
        ("SATURDAY", "Sat"),
        ("sunday", "Sun"),
    ],
)
def test_day_of_week_from_string(dow: str, expected_string: str) -> None:
    assert str(date.DayOfWeek.from_string(dow)) == expected_string


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
        ("10:00", 10, 0, datetime.timezone.utc),
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
        (date.Day, "foobar", "foobar is not a number"),
        (date.Month, "foobar", "foobar is not a number"),
        (date.Year, "foobar", "foobar is not a number"),
        (date.DayOfWeek, "foobar", "foobar is not a number or literal day of the week"),
        (date.Day, "64", "Day must be between 1 and 31"),
        (date.Month, "34", "Month must be between 1 and 12"),
        (date.Year, "1500", "Year must be between 2000 and 9999"),
        (date.DayOfWeek, "9", "Day of the week must be between 1 and 7"),
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
    date_type: typing.Type[date.PartialDatetime], value: str, expected_message: str
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
    value: str, expected_interval: typing.Optional[datetime.timedelta]
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
def test_datetime_between_range(
    time_to_check: datetime.datetime,
    begin_hour: int,
    begin_minute: int,
    end_hour: int,
    end_minute: int,
    strict: bool,
    result: bool,
) -> None:
    assert (
        date.is_datetime_between_time_range(
            time_to_check, begin_hour, begin_minute, end_hour, end_minute, strict
        )
        == result
    )
