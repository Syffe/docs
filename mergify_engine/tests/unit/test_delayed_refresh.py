import datetime
import typing

from freezegun import freeze_time
import pytest

from mergify_engine import delayed_refresh
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.tests import utils
from mergify_engine.tests.unit import conftest


@freeze_time("2021-09-22T08:00:05", tz_offset=0)
@pytest.mark.parametrize(
    "pull, expected_refresh",
    (
        ({}, datetime.datetime(2021, 9, 22, 15, 1, tzinfo=datetime.UTC)),
        ({"closed_at": "2021-09-10T08:00:05Z"}, None),
        (
            {"updated_at": "2021-09-22T08:02:05Z"},
            datetime.datetime(2021, 9, 22, 9, 2, 5, tzinfo=datetime.UTC),
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
      - schedule=08:00-17:00[Europe/Madrid]
      - updated-at<01:00 ago
    actions:
      label:
        add:
          - conflict
"""
    )
    ctxt = await context_getter(0, **pull)
    rule = typing.cast(
        list[prr_config.EvaluatedPullRequestRule], config["pull_request_rules"].rules
    )
    await delayed_refresh.plan_next_refresh(ctxt, rule, ctxt.pull_request)

    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"]
    )
    assert when == expected_refresh


@freeze_time("2021-09-22T08:00:05", tz_offset=0)
async def test_delay_refresh_only_if_earlier(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    config = await utils.load_mergify_config(
        """
pull_request_rules:
  - name: so rules
    conditions:
    - and:
      - head~=^foo/
      - schedule=08:00-17:00[Europe/Madrid]
    actions:
      label:
        add:
          - conflict
"""
    )

    expected_refresh_due_to_rules = datetime.datetime(
        2021, 9, 22, 15, 1, tzinfo=datetime.UTC
    )

    ctxt = await context_getter(0)
    rule = typing.cast(
        list[prr_config.EvaluatedPullRequestRule], config["pull_request_rules"].rules
    )

    # No delay refresh yet
    await delayed_refresh.plan_next_refresh(
        ctxt, rule, ctxt.pull_request, only_if_earlier=True
    )
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"]
    )
    assert when == expected_refresh_due_to_rules

    # hardcode a date in the future and ensure it's overriden
    future = datetime.datetime(2021, 9, 23, 00, 0, tzinfo=datetime.UTC)
    await delayed_refresh._set_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"], future
    )
    await delayed_refresh.plan_next_refresh(
        ctxt, rule, ctxt.pull_request, only_if_earlier=True
    )
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"]
    )
    assert when == expected_refresh_due_to_rules

    # hardcode a date in the past and ensure it's not overriden
    past = datetime.datetime(2021, 9, 22, 00, 0, tzinfo=datetime.UTC)
    await delayed_refresh._set_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"], past
    )
    await delayed_refresh.plan_next_refresh(
        ctxt, rule, ctxt.pull_request, only_if_earlier=True
    )
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"]
    )
    assert when == past

    # Don't use only_if_earlier and it's overriden
    past = datetime.datetime(2021, 9, 22, 00, 0, tzinfo=datetime.UTC)
    await delayed_refresh._set_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"], past
    )
    await delayed_refresh.plan_next_refresh(ctxt, rule, ctxt.pull_request)
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository, ctxt.pull["number"]
    )
    assert when == expected_refresh_due_to_rules
