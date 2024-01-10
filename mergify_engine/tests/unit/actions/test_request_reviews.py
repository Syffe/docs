import typing
from unittest import mock

import pytest
import voluptuous

from mergify_engine import actions
from mergify_engine import check_api
from mergify_engine import github_types
from mergify_engine.actions import request_reviews
from mergify_engine.clients import http
from mergify_engine.tests.unit import conftest


@pytest.mark.parametrize(
    "config",
    (
        {},
        {
            "users": ["hello"],
        },
        {
            "teams": ["hello", "@foobar"],
        },
    ),
)
def test_config(config: dict[str, list[str]]) -> None:
    request_reviews.RequestReviewsAction(config)


async def test_random_reviewers(context_getter: conftest.ContextGetterFixture) -> None:
    action = request_reviews.RequestReviewsAction(
        {
            "teams": {
                "foobar": 2,
                "foobaz": 1,
            },
            "users": {
                "jd": 2,
                "sileht": 1,
            },
        },
    )

    client = mock.MagicMock()
    client.get = mock.AsyncMock(return_value={})
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    await action.load_context(ctxt, mock.Mock())
    executor = typing.cast(request_reviews.RequestReviewsExecutor, action.executor)

    reviewers = executor._get_random_reviewers(2, 123, "jd")
    assert reviewers == {"@foobar", "sileht"}
    reviewers = executor._get_random_reviewers(2, 124, "sileht")
    assert reviewers == {"jd", "@foobar"}
    reviewers = executor._get_random_reviewers(2, 124, "jd")
    assert reviewers == {"@foobaz", "@foobar"}


async def test_random_reviewers_no_weight(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    action = request_reviews.RequestReviewsAction(
        {
            "teams": {
                "foobar": 2,
                "foobaz": 1,
            },
            "users": ["jd", "sileht"],
        },
    )

    client = mock.MagicMock()
    client.get = mock.AsyncMock(return_value={})
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    await action.load_context(ctxt, mock.Mock())
    executor = typing.cast(request_reviews.RequestReviewsExecutor, action.executor)

    reviewers = executor._get_random_reviewers(2, 123, "another-jd")
    assert reviewers == {"sileht", "jd"}
    reviewers = executor._get_random_reviewers(2, 124, "another-jd")
    assert reviewers == {"sileht", "@foobar"}
    reviewers = executor._get_random_reviewers(2, 124, "sileht")
    assert reviewers == {"@foobaz", "@foobar"}


async def test_random_reviewers_count_bigger(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    action = request_reviews.RequestReviewsAction(
        {
            "teams": {
                "foobar": 2,
                "foobaz": 1,
            },
            "users": {
                "jd": 2,
                "sileht": 45,
            },
        },
    )

    client = mock.MagicMock()
    client.get = mock.AsyncMock(return_value={})
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    await action.load_context(ctxt, mock.Mock())
    executor = typing.cast(request_reviews.RequestReviewsExecutor, action.executor)

    reviewers = executor._get_random_reviewers(15, 123, "foobar")
    assert reviewers == {"@foobar", "@foobaz", "jd", "sileht"}
    reviewers = executor._get_random_reviewers(15, 124, "another-jd")
    assert reviewers == {"@foobar", "@foobaz", "jd", "sileht"}
    reviewers = executor._get_random_reviewers(15, 124, "jd")
    assert reviewers == {"@foobar", "@foobaz", "sileht"}


def test_random_config_too_much_count() -> None:
    with pytest.raises(voluptuous.MultipleInvalid) as p:
        request_reviews.RequestReviewsAction(
            {
                "random_count": 20,
                "teams": {
                    "foobar": 2,
                    "foobaz": 1,
                },
                "users": {
                    "foobar": 2,
                    "foobaz": 1,
                },
            },
        )
    assert (
        str(p.value)
        == "value must be at most 15 for dictionary value @ data['random_count']"
    )


async def test_get_reviewers(context_getter: conftest.ContextGetterFixture) -> None:
    action = request_reviews.RequestReviewsAction(
        {
            "random_count": 2,
            "teams": {
                "foobar": 2,
                "foobaz": 1,
            },
            "users": {
                "jd": 2,
                "sileht": 1,
            },
        },
    )

    client = mock.MagicMock()
    client.get = mock.AsyncMock(return_value={})
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    await action.load_context(ctxt, mock.Mock())
    executor = typing.cast(request_reviews.RequestReviewsExecutor, action.executor)

    reviewers = executor._get_reviewers(843, set(), "another-jd")
    assert reviewers == ({"jd", "sileht"}, set())
    reviewers = executor._get_reviewers(844, set(), "another-jd")
    assert reviewers == ({"jd"}, {"foobar"})
    reviewers = executor._get_reviewers(845, set(), "another-jd")
    assert reviewers == ({"sileht"}, {"foobar"})
    reviewers = executor._get_reviewers(845, {"sileht"}, "another-jd")
    assert reviewers == (set(), {"foobar"})
    reviewers = executor._get_reviewers(845, {"jd"}, "another-jd")
    assert reviewers == ({"sileht"}, {"foobar"})
    reviewers = executor._get_reviewers(845, set(), "SILEHT")
    assert reviewers == ({"jd"}, {"foobar"})


async def test_team_permissions_missing(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    action = request_reviews.RequestReviewsAction(
        {
            "random_count": 2,
            "teams": {
                "foobar": 2,
                "@other/foobaz": 1,
            },
            "users": {
                "jd": 2,
                "sileht": 1,
            },
        },
    )
    client = mock.MagicMock()
    client.get = mock.AsyncMock(
        side_effect=http.HTTPNotFoundError(
            message="not found",
            response=mock.ANY,
            request=mock.ANY,
        ),
    )
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    with pytest.raises(actions.InvalidDynamicActionConfigurationError) as excinfo:
        await action.load_context(ctxt, mock.Mock())

    assert excinfo.value.reason == "Invalid requested teams"
    for error in (
        "Team `foobar` does not exist or has not access to this repository",
        "Team `@other/foobaz` is not part of the organization `Mergifyio`",
    ):
        assert error in excinfo.value.details


async def test_team_permissions_ok(
    context_getter: conftest.ContextGetterFixture,
) -> None:
    action = request_reviews.RequestReviewsAction(
        {
            "random_count": 2,
            "teams": {
                "foobar": 2,
                "foobaz": 1,
            },
            "users": {
                "jd": 2,
                "sileht": 1,
            },
        },
    )
    client = mock.MagicMock()
    client.get = mock.AsyncMock(return_value={})
    ctxt = await context_getter(github_types.GitHubPullRequestNumber(1))
    ctxt.repository.installation.client = client
    await action.load_context(ctxt, mock.Mock())
    result = await action.executor.run()
    assert not result.summary
    assert result.title == "No new reviewers to request"
    assert result.conclusion == check_api.Conclusion.SUCCESS
