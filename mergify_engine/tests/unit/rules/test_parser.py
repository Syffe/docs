import datetime
import re
import typing
import zoneinfo

from freezegun import freeze_time
import pytest

from mergify_engine import date
from mergify_engine import github_types
from mergify_engine.rules import filter
from mergify_engine.rules import parser


now = datetime.datetime.fromisoformat("2012-01-14T20:32:00+00:00")


@pytest.mark.parametrize(
    "line, result",
    (
        (
            "schedule!=Mon-Fri 12:00-23:59[Europe/Paris]",
            {
                "@": (
                    "schedule",
                    {
                        "!=": (
                            "current-time",
                            date.Schedule(
                                start_weekday=1,
                                end_weekday=5,
                                start_hour=12,
                                end_hour=23,
                                start_minute=0,
                                end_minute=59,
                                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
                            ),
                        )
                    },
                )
            },
        ),
        (
            "schedule!=Mon-Fri 12:00-23:59",
            {
                "@": (
                    "schedule",
                    {
                        "!=": (
                            "current-time",
                            date.Schedule(
                                start_weekday=1,
                                end_weekday=5,
                                start_hour=12,
                                end_hour=23,
                                start_minute=0,
                                end_minute=59,
                                tzinfo=date.UTC,
                            ),
                        )
                    },
                )
            },
        ),
        ("base:main", {"=": ("base", "main")}),
        ("base!=main", {"!=": ("base", "main")}),
        ("base~=^stable/", {"~=": ("base", "^stable/")}),
        ("-base:foobar", {"-": {"=": ("base", "foobar")}}),
        ("-author~=jd", {"-": {"~=": ("author", "jd")}}),
        ("¬author~=jd", {"-": {"~=": ("author", "jd")}}),
        ("conflict", {"=": ("conflict", True)}),
        (
            "current-time>=10:00[PST8PDT]",
            {
                ">=": (
                    "current-time",
                    date.Time(10, 0, tzinfo=zoneinfo.ZoneInfo("PST8PDT")),
                )
            },
        ),
        (
            "current-time>=10:00",
            {
                ">=": (
                    "current-time",
                    date.Time(10, 0, tzinfo=date.UTC),
                )
            },
        ),
        ("current-day=4", {"=": ("current-day", date.Day(4))}),
        ("current-month=5", {"=": ("current-month", date.Month(5))}),
        ("current-year=2000", {"=": ("current-year", date.Year(2000))}),
        ("current-day-of-week=4", {"=": ("current-day-of-week", date.DayOfWeek(4))}),
        ("current-day-of-week=MON", {"=": ("current-day-of-week", date.DayOfWeek(1))}),
        (
            "current-day-of-week=WednesDay",
            {"=": ("current-day-of-week", date.DayOfWeek(3))},
        ),
        ("current-day-of-week=sun", {"=": ("current-day-of-week", date.DayOfWeek(7))}),
        (
            "schedule: MON-FRI 08:00-17:00",
            {
                "@": (
                    "schedule",
                    {
                        "=": (
                            "current-time",
                            date.Schedule(
                                start_weekday=1,
                                end_weekday=5,
                                start_hour=8,
                                end_hour=17,
                                start_minute=0,
                                end_minute=0,
                                tzinfo=date.UTC,
                            ),
                        )
                    },
                )
            },
        ),
        (
            "schedule=MON-friday 10:02-22:35",
            {
                "@": (
                    "schedule",
                    {
                        "=": (
                            "current-time",
                            date.Schedule(
                                start_weekday=1,
                                end_weekday=5,
                                start_hour=10,
                                end_hour=22,
                                start_minute=2,
                                end_minute=35,
                                tzinfo=date.UTC,
                            ),
                        )
                    },
                )
            },
        ),
        (
            "schedule: MON-friday",
            {
                "@": (
                    "schedule",
                    {
                        "=": (
                            "current-time",
                            date.Schedule(
                                start_weekday=1,
                                end_weekday=5,
                                start_hour=0,
                                end_hour=23,
                                start_minute=0,
                                end_minute=59,
                                tzinfo=date.UTC,
                            ),
                        )
                    },
                )
            },
        ),
        (
            "schedule=10:02-22:35",
            {
                "@": (
                    "schedule",
                    {
                        "=": (
                            "current-time",
                            date.Schedule(
                                start_weekday=1,
                                end_weekday=7,
                                start_hour=10,
                                end_hour=22,
                                start_minute=2,
                                end_minute=35,
                                tzinfo=date.UTC,
                            ),
                        )
                    },
                )
            },
        ),
        (
            "schedule=10:02-22:35[Europe/Paris]",
            {
                "@": (
                    "schedule",
                    {
                        "=": (
                            "current-time",
                            date.Schedule(
                                start_weekday=1,
                                end_weekday=7,
                                start_hour=10,
                                end_hour=22,
                                start_minute=2,
                                end_minute=35,
                                tzinfo=zoneinfo.ZoneInfo("Europe/Paris"),
                            ),
                        )
                    },
                )
            },
        ),
        ("locked", {"=": ("locked", True)}),
        ("#commits=2", {"=": ("#commits", 2)}),
        ("#commits-behind>2", {">": ("#commits-behind", 2)}),
        (
            "commits[0].date_committer < 1 day ago",
            {
                "<": (
                    "commits[0].date_committer-relative",
                    date.RelativeDatetime(now - datetime.timedelta(days=1)),
                )
            },
        ),
        (
            "commits[-1].author~=hellothere",
            {
                "~=": (
                    "commits[-1].author",
                    "hellothere",
                )
            },
        ),
        (
            f"merged-at<={now.isoformat()}",
            {"<=": ("merged-at", now)},
        ),
        (
            f"closed-at<={now.isoformat()}",
            {"<=": ("closed-at", now)},
        ),
        (
            f"merged-at<={now.isoformat()}",
            {"<=": ("merged-at", now)},
        ),
        (
            f"queued-at<={now.isoformat()}",
            {"<=": ("queued-at", now)},
        ),
        (
            f"updated-at<={now.isoformat()}",
            {"<=": ("updated-at", now)},
        ),
        (
            f"queue-merge-started-at<={now.isoformat()}",
            {"<=": ("queue-merge-started-at", now)},
        ),
        (
            "closed-at>=18:02 ago",
            {
                ">=": (
                    "closed-at-relative",
                    date.RelativeDatetime(
                        now - datetime.timedelta(hours=18, minutes=2)
                    ),
                )
            },
        ),
        (
            "merged-at>18:02 ago",
            {
                ">": (
                    "merged-at-relative",
                    date.RelativeDatetime(
                        now - datetime.timedelta(hours=18, minutes=2)
                    ),
                )
            },
        ),
        (
            "created-at<18:02 ago",
            {
                "<": (
                    "created-at-relative",
                    date.RelativeDatetime(
                        now - datetime.timedelta(hours=18, minutes=2)
                    ),
                ),
            },
        ),
        (
            "updated-at>=18:02 ago",
            {
                ">=": (
                    "updated-at-relative",
                    date.RelativeDatetime(
                        now - datetime.timedelta(hours=18, minutes=2)
                    ),
                )
            },
        ),
        (
            "updated-at<=7 days ago",
            {
                "<=": (
                    "updated-at-relative",
                    date.RelativeDatetime(now - datetime.timedelta(days=7)),
                )
            },
        ),
        (
            "updated-at>7 days 18:02 ago",
            {
                ">": (
                    "updated-at-relative",
                    date.RelativeDatetime(
                        now - datetime.timedelta(days=7, hours=18, minutes=2),
                    ),
                )
            },
        ),
        (
            f"current-timestamp<={now.isoformat()}",
            {"<=": ("current-timestamp", now)},
        ),
        ("-linear-history", {"-": {"=": ("linear-history", True)}}),
        ("-locked", {"-": {"=": ("locked", True)}}),
        ("queue-position>=0", {">=": ("queue-position", 0)}),
        ("queue-position=-1", {"=": ("queue-position", -1)}),
        ("assignee:sileht", {"=": ("assignee", "sileht")}),
        ("assignee: sileht", {"=": ("assignee", "sileht")}),
        ("assignee: sileht ", {"=": ("assignee", "sileht")}),
        ("assignee : sileht ", {"=": ("assignee", "sileht")}),
        ("#assignee=3", {"=": ("#assignee", 3)}),
        ("# assignee = 3", {"=": ("#assignee", 3)}),
        ("#assignee>1", {">": ("#assignee", 1)}),
        (" # assignee > 1", {">": ("#assignee", 1)}),
        ("author=jd", {"=": ("author", "jd")}),
        ("author=jd_", {"=": ("author", "jd_")}),
        ("author=mergify[bot]", {"=": ("author", "mergify[bot]")}),
        ("author=foo-bar", {"=": ("author", "foo-bar")}),
        ("#assignee>=2", {">=": ("#assignee", 2)}),
        ("number>=2", {">=": ("number", 2)}),
        ("assignee=@org/team", {"=": ("assignee", "@org/team")}),
        (
            "status-success=my ci has spaces",
            {"=": ("status-success", "my ci has spaces")},
        ),
        ("status-success='my quoted ci'", {"=": ("status-success", "my quoted ci")}),
        (
            'status-success="my double quoted ci"',
            {"=": ("status-success", "my double quoted ci")},
        ),
        (
            "check-success=my ci has spaces",
            {"=": ("check-success", "my ci has spaces")},
        ),
        ("check-success='my quoted ci'", {"=": ("check-success", "my quoted ci")}),
        (
            'check-success="my double quoted ci"',
            {"=": ("check-success", "my double quoted ci")},
        ),
        ("check-failure='my quoted ci'", {"=": ("check-failure", "my quoted ci")}),
        (
            'check-failure="my double quoted ci"',
            {"=": ("check-failure", "my double quoted ci")},
        ),
        ("check-neutral='my quoted ci'", {"=": ("check-neutral", "my quoted ci")}),
        (
            'check-neutral="my double quoted ci"',
            {"=": ("check-neutral", "my double quoted ci")},
        ),
        ("check-skipped='my quoted ci'", {"=": ("check-skipped", "my quoted ci")}),
        (
            'check-skipped="my double quoted ci"',
            {"=": ("check-skipped", "my double quoted ci")},
        ),
        ("check-pending='my quoted ci'", {"=": ("check-pending", "my quoted ci")}),
        (
            'check-pending="my double quoted ci"',
            {"=": ("check-pending", "my double quoted ci")},
        ),
        ("check-stale='my quoted ci'", {"=": ("check-stale", "my quoted ci")}),
        (
            'check-stale="my double quoted ci"',
            {"=": ("check-stale", "my double quoted ci")},
        ),
        ("body=b", {"=": ("body", "b")}),
        ("body=bb", {"=": ("body", "bb")}),
        ("author=", {"=": ("author", "")}),
        (
            "sender-permission=admin",
            {
                "=": (
                    "sender-permission",
                    github_types.GitHubRepositoryPermission.ADMIN,
                )
            },
        ),
        (
            "sender-permission>=write",
            {
                ">=": (
                    "sender-permission",
                    github_types.GitHubRepositoryPermission.WRITE,
                )
            },
        ),
        (
            "sender-permission!=read",
            {
                "!=": (
                    "sender-permission",
                    github_types.GitHubRepositoryPermission.READ,
                )
            },
        ),
    ),
)
@freeze_time(now)
def test_parse(line: str, result: typing.Any) -> None:
    assert result == parser.parse(line, allow_command_attributes=True)


@pytest.mark.parametrize(
    "condition,expected_attribute,expected_operator,expected_rendering",
    (
        ("sender={{ author }}", "sender", "=", "foo"),
        ("body='hello {{ author }}'", "body", "=", "hello foo"),
        ("body~=hello {{ author }}", "body", "~=", "hello foo"),
    ),
)
def test_parse_jinja_template(
    condition: str,
    expected_attribute: str,
    expected_operator: str,
    expected_rendering: str,
) -> None:
    result = parser.parse(condition, allow_command_attributes=True)
    assert expected_operator in result
    attribute_name, template = result[expected_operator]
    assert attribute_name == expected_attribute
    assert isinstance(template, filter.JinjaTemplateWrapper)
    assert template.render({"author": "foo"}) == expected_rendering


@pytest.mark.parametrize(
    "line, expected_error",
    (
        ("arf", "Invalid attribute"),
        ("-heyo", "Invalid attribute"),
        ("locked=1", "Operators are invalid for Boolean attribute: `locked`"),
        ("#conflict", "`#` modifier is invalid for attribute: `conflict`"),
        ("++head=main", "Invalid attribute"),
        ("foo=bar", "Invalid attribute"),
        ("#foo=bar", "Invalid attribute"),
        ("number=foo", "foo is not a number"),
        ("author=%foobar", "Invalid GitHub login"),
        ("current-time<foobar", "Invalid time"),
        ("current-time=10:00", "Invalid operator"),
        (
            "-current-time>=10:00",
            "`-` modifier is invalid for attribute: `current-time`",
        ),
        ("current-day-of-week=100", "Day of the week must be between 1 and 7"),
        ("current-month=100", "Month must be between 1 and 12"),
        ("current-year=0", "Year must be between 2000 and 9999"),
        ("current-day=100", "Day must be between 1 and 31"),
        ("current-day>100", "Day must be between 1 and 31"),
        ("updated-at=7 days 18:00", "Invalid operator"),
        ("updated-at>=100", "Invalid timestamp"),
        ("current-timestamp>=100", "Invalid timestamp"),
        ("current-time>=10:00[InvalidTZ]", "Invalid timezone"),
        ("schedule=MON-friday 10:02-22:35[InvalidTZ]", "Invalid timezone"),
        (
            "-schedule=MON-friday 10:02-22:35",
            "`-` modifier is invalid for attribute: `schedule`",
        ),
        ("foobar", "Invalid attribute"),
        ("schedule=", "Invalid schedule"),
        ('schedule="Mon 12:00-17:00"', "Invalid schedule"),
        ('schedule="Mon-Fri 12:00"', "Invalid schedule"),
        ("#schedule=bar", "`#` modifier is invalid for attribute: `schedule`"),
        ("#title=bar", "bar is not a number"),
        ('body="b', "Unbalanced quotes"),
        ('body=b"', "Unbalanced quotes"),
        ("body='b", "Unbalanced quotes"),
        ("body=b'", "Unbalanced quotes"),
        ("", "Condition empty"),
        ("-", "Incomplete condition"),
        ("#", "Incomplete condition"),
        ("-#", "Incomplete condition"),
        ("#-", "Invalid attribute"),
        ("commits-behind", "`#` modifier is required for attribute: `commits-behind`"),
        ("sender-permission=foo", "Permission must be one of"),
        ("author=foo bar", "Invalid GitHub login"),
        ("author=@team/foo/bar", "Invalid GitHub team name"),
        ("author=foo/bar", "Invalid GitHub login"),
        ("sender={{ author", "Invalid template"),
        (r"sender={ author }", "Invalid GitHub login"),
        ("commits[0].test", "`test` is not a valid sub-attribute for `commits`"),
        ("commits[abc].author", "Invalid operator"),
        ("commits[0]", "Invalid operator"),
    ),
)
def test_parse_invalid(line: str, expected_error: str) -> None:
    with pytest.raises(parser.ConditionParsingError, match=re.escape(expected_error)):
        parser.parse(line, allow_command_attributes=True)


@pytest.mark.parametrize(
    "line, expected_error",
    (
        ("sender=foobar", "Attribute only allowed in commands_restrictions section"),
        (
            "sender-permissions=write",
            "Attribute only allowed in commands_restrictions section",
        ),
    ),
)
def test_parse_invalid_not_commands(line: str, expected_error: str) -> None:
    with pytest.raises(parser.ConditionParsingError, match=re.escape(expected_error)):
        parser.parse(line, allow_command_attributes=False)


def test_is_github_team_name() -> None:
    assert parser.is_github_team_name("@foo")
    assert parser.is_github_team_name("@foo/bar")
    assert not parser.is_github_team_name("foo")


@pytest.mark.parametrize(
    ("value",),
    (("@foo/bar/baz",), ("@foo@bar",), ("@foo bar",), ("@é",)),
)
def test_validate_github_team_name(value: str) -> None:
    with pytest.raises(
        parser.ConditionParsingError, match=re.escape("Invalid GitHub team name")
    ):
        parser.validate_github_team_name(value)


@pytest.mark.parametrize(
    ("value",),
    (("@foo",), ("foo/bar",), ("foo bar",), ("é",)),
)
def test_validate_github_login(value: str) -> None:
    with pytest.raises(
        parser.ConditionParsingError, match=re.escape("Invalid GitHub login")
    ):
        parser.validate_github_login(value)


@pytest.mark.parametrize(
    ("value", "expected_error"),
    (
        (r"{{ author", "Invalid template"),
        (r"{{ foo bar }}", "Invalid template"),
        (r"{% for x in my_list %}{{x.foo}}{% wtf %}", "Invalid template"),
        (r"{% for x in my_list %}{{x.foo}}{% endfor %", "Invalid template"),
        (r"{# comment", "Invalid template"),
        (
            r"{{ unknown_attr }}",
            r"Invalid template, unexpected variables {'unknown_attr'}",
        ),
    ),
)
def test_validate_jinja_template(value: str, expected_error: str) -> None:
    with pytest.raises(parser.ConditionParsingError, match=re.escape(expected_error)):
        parser.validate_jinja_template(value)
