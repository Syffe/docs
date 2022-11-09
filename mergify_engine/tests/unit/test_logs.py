import pytest

from mergify_engine import logs


@pytest.mark.parametrize(
    "url,url_expected",
    (
        ("redis://foobar/db?args", "redis://foobar/db?args"),
        ("redis://foo@bar/db?args", "redis://*****@bar/db?args"),
        ("redis://foobar@/db?args", "redis://*****@/db?args"),
        ("redis://foo:bar@/db?args", "redis://*****@/db?args"),
        ("redis://foo:bar@example/db?args", "redis://*****@example/db?args"),
        ("redis://foo:bar@example:80/db?args", "redis://*****@example:80/db?args"),
        ("redis://foo:bar@example:0/db?args", "redis://*****@example:0/db?args"),
        ("redis://foo:bar@:2/db?args", "redis://*****@:2/db?args"),
        ("redis://foo@:0/db?args", "redis://*****@:0/db?args"),
        ("redis://:bar@:2/db?args", "redis://*****@:2/db?args"),
    ),
)
def test_strip_url_credentials(url: str, url_expected: str) -> None:
    assert logs.strip_url_credentials(url) == url_expected
