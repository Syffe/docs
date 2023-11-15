import datetime
import typing

import pytest

from mergify_engine import condition_value_querier
from mergify_engine import delayed_refresh
from mergify_engine.rules.config import pull_request_rules as prr_config
from mergify_engine.tests import utils
from mergify_engine.tests.tardis import time_travel
from mergify_engine.tests.unit import conftest


@time_travel("2021-09-22T08:00:05Z")
@pytest.mark.parametrize(
    "pull, expected_refresh",
    (
        ({}, datetime.datetime(2021, 9, 22, 15, 0, 1, tzinfo=datetime.UTC)),
        ({"closed_at": "2021-09-10T08:00:05Z"}, None),
        (
            {"updated_at": "2021-09-22T08:02:05Z"},
            datetime.datetime(2021, 9, 22, 9, 2, 5, tzinfo=datetime.UTC),
        ),
    ),
)
async def test_delayed_refresh_without_time(
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
""",
    )
    ctxt = await context_getter(0, **pull)
    rule = typing.cast(
        list[prr_config.EvaluatedPullRequestRule],
        config["pull_request_rules"].rules,
    )
    await delayed_refresh.plan_next_refresh(
        ctxt,
        rule,
        condition_value_querier.PullRequest(ctxt),
    )

    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
    )
    assert when == expected_refresh


@pytest.mark.parametrize(
    "start_datetime, expected_refresh",
    (
        # [Europe/Paris] == UTC+2
        # This is a Monday
        (
            datetime.datetime(2023, 7, 24, 12, 10, tzinfo=datetime.UTC),
            datetime.datetime(2023, 7, 24, 12, 30, tzinfo=datetime.UTC),
        ),
        # This is a Monday
        (
            datetime.datetime(2023, 7, 24, 12, 31, 0, tzinfo=datetime.UTC),
            datetime.datetime(2023, 7, 24, 12, 50, 1, tzinfo=datetime.UTC),
        ),
        # This is a Friday
        (
            datetime.datetime(2023, 7, 28, 6, 0, 0, tzinfo=datetime.UTC),
            datetime.datetime(2023, 7, 28, 7, 0, 0, tzinfo=datetime.UTC),
        ),
        # This is a Friday
        (
            datetime.datetime(2023, 7, 28, 7, 2, 0, tzinfo=datetime.UTC),
            datetime.datetime(2023, 7, 28, 10, 0, 1, tzinfo=datetime.UTC),
        ),
    ),
)
async def test_delayed_refresh_with_more_success_conditions(
    context_getter: conftest.ContextGetterFixture,
    start_datetime: datetime.datetime,
    expected_refresh: datetime.datetime | None,
) -> None:
    config = await utils.load_mergify_config(
        """
pull_request_rules:
  - name: post check with success conditions
    conditions:
      - "base=main"

    actions:
      post_check:
        success_conditions:
          - or:
            - schedule=Mon-Thu 14:30-14:50[Europe/Paris]
            - schedule=Mon-Thu 15:30-15:50[Europe/Paris]
            - schedule=Fri-Fri 09:00-12:00[Europe/Paris]
        title: Best post check ever
        summary: |
          {% if not check_succeed %}
          We are not in working hours, try another time
          {% else %}
          We are in working hours, well done
          {% endif %}
""",
    )
    with time_travel(start_datetime, tick=False):
        ctxt = await context_getter(0)
        rule = typing.cast(
            list[prr_config.EvaluatedPullRequestRule],
            config["pull_request_rules"].rules,
        )
        await delayed_refresh.plan_next_refresh(
            ctxt,
            rule,
            condition_value_querier.PullRequest(ctxt),
        )

        when = await delayed_refresh._get_current_refresh_datetime(
            ctxt.repository,
            ctxt.pull["number"],
        )
        assert when == expected_refresh


@pytest.mark.parametrize(
    "start_datetime, pull, expected_refresh",
    (
        # [Europe/Madrid] == UTC+2
        # This is a Monday
        (
            datetime.datetime(2023, 7, 24, 11, 10, tzinfo=datetime.UTC),
            {"updated_at": "2023-07-24T9:02:00Z"},
            datetime.datetime(2023, 7, 24, 13, 20, 0, tzinfo=datetime.UTC),
        ),
        # This is a Monday
        (
            datetime.datetime(2023, 7, 24, 10, 10, tzinfo=datetime.UTC),
            {},
            datetime.datetime(2023, 7, 24, 13, 20, 0, tzinfo=datetime.UTC),
        ),
        # This is a Monday
        (
            datetime.datetime(2023, 7, 24, 10, 10, tzinfo=datetime.UTC),
            {"closed_at": "2023-07-10T06:50:05Z"},
            None,
        ),
        # This is a Friday
        (
            datetime.datetime(2023, 7, 21, 6, 10, tzinfo=datetime.UTC),
            {},
            datetime.datetime(2023, 7, 21, 7, 0, 0, tzinfo=datetime.UTC),
        ),
    ),
)
async def test_delayed_refresh_with_success_conditions(
    context_getter: conftest.ContextGetterFixture,
    start_datetime: datetime.datetime,
    pull: dict[str, typing.Any],
    expected_refresh: datetime.datetime | None,
) -> None:
    config = await utils.load_mergify_config(
        """
pull_request_rules:
  - name: post check with success conditions
    conditions:
      - "base=main"

    actions:
      post_check:
        success_conditions:
          - or:
            - schedule=Mon-Thu 15:20-18:00[Europe/Madrid]
            - schedule=Fri-Fri 09:00-12:00[Europe/Madrid]
        title: Best post check ever
        summary: |
          {% if not check_succeed %}
          We are not in working hours, try another time
          {% else %}
          We are in working hours, well done
          {% endif %}
""",
    )
    with time_travel(start_datetime, tick=True):
        ctxt = await context_getter(0, **pull)
        rule = typing.cast(
            list[prr_config.EvaluatedPullRequestRule],
            config["pull_request_rules"].rules,
        )
        await delayed_refresh.plan_next_refresh(
            ctxt,
            rule,
            condition_value_querier.PullRequest(ctxt),
        )

        when = await delayed_refresh._get_current_refresh_datetime(
            ctxt.repository,
            ctxt.pull["number"],
        )
        assert when == expected_refresh


@time_travel("2021-09-22T08:00:05Z")
async def test_delayed_refresh_only_if_earlier(
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
""",
    )

    expected_refresh_due_to_rules = datetime.datetime(
        2021,
        9,
        22,
        15,
        0,
        1,
        tzinfo=datetime.UTC,
    )

    ctxt = await context_getter(0)
    rule = typing.cast(
        list[prr_config.EvaluatedPullRequestRule],
        config["pull_request_rules"].rules,
    )

    # No delay refresh yet
    await delayed_refresh.plan_next_refresh(
        ctxt,
        rule,
        condition_value_querier.PullRequest(ctxt),
        only_if_earlier=True,
    )
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
    )
    assert when == expected_refresh_due_to_rules

    # hardcode a date in the future and ensure it's overriden
    future = datetime.datetime(2021, 9, 23, 00, 0, tzinfo=datetime.UTC)
    await delayed_refresh._set_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
        future,
    )
    await delayed_refresh.plan_next_refresh(
        ctxt,
        rule,
        condition_value_querier.PullRequest(ctxt),
        only_if_earlier=True,
    )
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
    )
    assert when == expected_refresh_due_to_rules

    # hardcode a date in the past and ensure it's not overriden
    past = datetime.datetime(2021, 9, 22, 00, 0, tzinfo=datetime.UTC)
    await delayed_refresh._set_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
        past,
    )
    await delayed_refresh.plan_next_refresh(
        ctxt,
        rule,
        condition_value_querier.PullRequest(ctxt),
        only_if_earlier=True,
    )
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
    )
    assert when == past

    # Don't use only_if_earlier and it's overriden
    past = datetime.datetime(2021, 9, 22, 00, 0, tzinfo=datetime.UTC)
    await delayed_refresh._set_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
        past,
    )
    await delayed_refresh.plan_next_refresh(
        ctxt,
        rule,
        condition_value_querier.PullRequest(ctxt),
    )
    when = await delayed_refresh._get_current_refresh_datetime(
        ctxt.repository,
        ctxt.pull["number"],
    )
    assert when == expected_refresh_due_to_rules
