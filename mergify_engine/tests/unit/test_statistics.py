from freezegun import freeze_time

from mergify_engine import date
from mergify_engine.web.api import statistics as api_stats


def test_is_timestamp_in_future() -> None:
    with freeze_time("2022-10-14T10:00:00"):
        now_ts = int(date.utcnow().timestamp())
        assert not api_stats.is_timestamp_in_future(now_ts)
        assert api_stats.is_timestamp_in_future(now_ts + 1)
        assert not api_stats.is_timestamp_in_future(now_ts - 1)
