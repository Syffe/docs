import datetime
import typing

from freezegun import freeze_time
import pytest

from mergify_engine import delayed_refresh
from mergify_engine import rules
from mergify_engine.tests import utils
from mergify_engine.tests.unit import conftest


@freeze_time("2021-09-22T08:00:05", tz_offset=0)
@pytest.mark.parametrize(
    "pull, expected_refresh",
    (
        ({}, datetime.datetime(2021, 9, 22, 15, 0, tzinfo=datetime.timezone.utc)),
        ({"closed_at": "2021-09-10T08:00:05Z"}, None),
        (
            {"updated_at": "2021-09-22T08:02:05Z"},
            datetime.datetime(2021, 9, 22, 9, 2, 5, tzinfo=datetime.timezone.utc),
        ),
    ),
)
async def test_delay_refresh_without_time(
    context_getter: conftest.ContextGetterFixture,
    pull: dict[str, typing.Any],
    expected_refresh: datetime.datetime | None,
) -> None:
    config = await utils.load_mergify_config(
        """
pull_request_rules:
  - name: so rules
    conditions:
    - and:
      - head~=^foo/
      - current-time>=08:00[Europe/Madrid]
      - current-time<=17:00[Europe/Madrid]
      - updated-at<01:00 ago
    actions:
      label:
        add:
          - conflict
"""
    )
    ctxt = await context_getter(0, **pull)
    rule = typing.cast(list[rules.EvaluatedRule], config["pull_request_rules"].rules)
    await delayed_refresh.plan_next_refresh(ctxt, rule, ctxt.pull_request)

    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"]
    )
    assert when == expected_refresh
