import pytest

from mergify_engine import debug
from mergify_engine import github_types


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/mergifyio",
        "https://github.com//mergifyio",
        "https://github.com//mergifyio//",
    ],
)
def test_url_parser_with_owner_ok(url: str) -> None:
    assert debug._url_parser(url) == ("mergifyio", None, None)


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/mergifyio/mergify-engine",
        "https://github.com//mergifyio//mergify-engine//",
    ],
)
def test_url_parser_with_repo_ok(url: str) -> None:
    assert debug._url_parser(url) == ("mergifyio", "mergify-engine", None)


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/mergifyio/mergify-engine/pull/123",
        "https://github.com/mergifyio/mergify-engine/pull/123#",
        "https://github.com/mergifyio/mergify-engine/pull/123#",
        "https://github.com/mergifyio/mergify-engine/pull/123#42",
        "https://github.com/mergifyio/mergify-engine/pull/123?foo=345",
        "https://github.com/mergifyio/mergify-engine/pull/123?foo=456&bar=567c",
        "https://github.com/mergifyio/mergify-engine/pull/123?foo",
        "https://github.com//mergifyio/mergify-engine/pull/123",
        "https://github.com/mergifyio//mergify-engine/pull/123",
        "https://github.com//mergifyio/mergify-engine//pull/123",
        "https://github.com//mergifyio/mergify-engine/pull//123",
    ],
)
def test_url_parser_with_pr_ok(url: str) -> None:
    assert debug._url_parser(url) == (
        github_types.GitHubLogin("mergifyio"),
        github_types.GitHubRepositoryName("mergify-engine"),
        github_types.GitHubPullRequestNumber(123),
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/",
        "https://github.com/mergifyio/mergify-engine/123",
        "https://github.com/mergifyio/mergify-engine/foobar/pull/123",
        "https://github.com/mergifyio/mergify-engine/foobar/pull/123/foobar/pull/123/",
    ],
)
def test_url_parser_fail(url: str) -> None:
    with pytest.raises(ValueError):
        debug._url_parser(url)
