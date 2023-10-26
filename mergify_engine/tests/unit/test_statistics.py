from mergify_engine import date
from mergify_engine.tests.tardis import time_travel
from mergify_engine.web.api.statistics import utils as web_stat_utils


def test_is_timestamp_in_future() -> None:
    with time_travel("2022-10-14T10:00:00"):
        now_ts = int(date.utcnow().timestamp())
        assert not web_stat_utils.is_timestamp_in_future(now_ts)
        assert web_stat_utils.is_timestamp_in_future(now_ts + 1)
        assert not web_stat_utils.is_timestamp_in_future(now_ts - 1)
