from unittest import mock

import pytest

from mergify_engine import github_types
from mergify_engine import signals
from mergify_engine import subscription
from mergify_engine.tests.unit import conftest


async def test_signals(
    context_getter: conftest.ContextGetterFixture, request: pytest.FixtureRequest
) -> None:
    signals.register()
    request.addfinalizer(signals.unregister)
    assert len(signals.SIGNALS) == 6

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    with mock.patch("mergify_engine.signals.NoopSignal.__call__") as signal_method:
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.label",
            signals.EventLabelMetadata({"added": [], "removed": ["bar"]}),
            "Rule: awesome rule",
        )
        signal_method.assert_called_once_with(
            ctxt.repository,
            ctxt.pull["number"],
            "action.label",
            {"added": [], "removed": ["bar"]},
            "Rule: awesome rule",
        )


async def test_copy_signal(
    context_getter: conftest.ContextGetterFixture, request: pytest.FixtureRequest
) -> None:
    signals.register()
    request.addfinalizer(signals.unregister)
    assert len(signals.SIGNALS) == 6

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    with mock.patch("mergify_engine.signals.NoopSignal.__call__") as signal_method:
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.copy",
            signals.EventCopyMetadata(
                {"to": "test_branch", "pull_request_number": 123, "conflicts": False}
            ),
            "Rule: awesome rule",
        )
        signal_method.assert_called_once_with(
            ctxt.repository,
            ctxt.pull["number"],
            "action.copy",
            {"to": "test_branch", "pull_request_number": 123, "conflicts": False},
            "Rule: awesome rule",
        )


async def test_datadog(
    context_getter: conftest.ContextGetterFixture, request: pytest.FixtureRequest
) -> None:
    signals.register()
    request.addfinalizer(signals.unregister)
    assert len(signals.SIGNALS) == 6

    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))

    with mock.patch("datadog.statsd.increment") as increment:
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.label",
            {"added": [], "removed": ["bar"]},
            "Rule: awesome rule",
        )
        increment.assert_called_once_with(
            "engine.signals.action.count", tags=["event:label"]
        )

    ctxt.repository.installation.subscription.features = frozenset(
        [
            subscription.Features.PUBLIC_REPOSITORY,
            subscription.Features.PRIVATE_REPOSITORY,
        ]
    )

    with mock.patch("datadog.statsd.increment") as increment:
        await signals.send(
            ctxt.repository,
            ctxt.pull["number"],
            "action.label",
            {"added": [], "removed": ["bar"]},
            "Rule: awesome rule",
        )
        increment.assert_called_once_with(
            "engine.signals.action.count",
            tags=["event:label", "github_login:Mergifyio"],
        )
