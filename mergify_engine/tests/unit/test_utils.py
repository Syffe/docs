import datetime
import typing

from freezegun import freeze_time
import pytest

from mergify_engine import github_types
from mergify_engine import redis_utils
from mergify_engine import utils


@pytest.mark.parametrize(
    "position, length,placeholder,expected",
    (
        ("end", 0, "", ""),
        ("end", 1, "", "h"),
        ("end", 2, "", "h"),
        ("end", 3, "", "hé"),
        ("end", 4, "", "hé "),
        ("end", 10, "", "hé ho! ho"),
        ("end", 18, "", "hé ho! how are yo"),
        ("end", 19, "", "hé ho! how are you"),
        ("end", 20, "", "hé ho! how are you"),
        ("end", 21, "", "hé ho! how are you"),
        ("end", 22, "", "hé ho! how are you√"),
        ("end", 23, "", "hé ho! how are you√2"),
        ("end", 50, "", "hé ho! how are you√2?"),
        # ellipsis
        ("end", 0, "…", None),
        ("end", 1, "…", None),
        ("end", 2, "…", None),
        ("end", 3, "…", "…"),
        ("end", 4, "…", "h…"),
        ("end", 5, "…", "h…"),
        ("end", 6, "…", "hé…"),
        ("end", 7, "…", "hé …"),
        ("end", 13, "…", "hé ho! ho…"),
        ("end", 21, "…", "hé ho! how are yo…"),
        ("end", 22, "…", "hé ho! how are you…"),
        ("end", 23, "…", "hé ho! how are you…"),
        ("end", 24, "…", "hé ho! how are you√2?"),
        ("end", 50, "…", "hé ho! how are you√2?"),
        ("end", 21, "😎", "hé ho! how are y😎"),
        ("end", 22, "😎", "hé ho! how are yo😎"),
        ("end", 23, "😎", "hé ho! how are you😎"),
        ("end", 24, "😎", "hé ho! how are you√2?"),
        ("end", 50, "😎", "hé ho! how are you√2?"),
        ("end", 3, "😎", None),
        ("end", 4, "😎", "😎"),
        ("end", 5, "😎", "h😎"),
        ("end", 6, "😎", "h😎"),
        ("end", 7, "😎", "hé😎"),
        # middle
        ("middle", 0, "", ""),
        ("middle", 1, "", "h"),
        ("middle", 2, "", "h?"),
        ("middle", 3, "", "h?"),
        ("middle", 4, "", "h2?"),
        ("middle", 10, "", "hé h√2?"),
        ("middle", 18, "", "hé ho! h you√2?"),
        ("middle", 19, "", "hé ho! ho you√2?"),
        ("middle", 20, "", "hé ho! hoe you√2?"),
        ("middle", 21, "", "hé ho! howe you√2?"),
        ("middle", 22, "", "hé ho! howre you√2?"),
        ("middle", 23, "", "hé ho! how re you√2?"),
        ("middle", 50, "", "hé ho! how are you√2?"),
        # ellipsis
        ("middle", 0, "…", None),
        ("middle", 1, "…", None),
        ("middle", 2, "…", None),
        ("middle", 3, "…", "…"),
        ("middle", 4, "…", "h…"),
        ("middle", 5, "…", "h…?"),
        ("middle", 6, "…", "h…?"),
        ("middle", 7, "…", "h…2?"),
        ("middle", 13, "…", "hé h…√2?"),
        ("middle", 21, "…", "hé ho! h… you√2?"),
        ("middle", 22, "…", "hé ho! ho… you√2?"),
        ("middle", 23, "…", "hé ho! ho…e you√2?"),
        ("middle", 24, "…", "hé ho! how are you√2?"),
        ("middle", 50, "…", "hé ho! how are you√2?"),
        ("middle", 21, "😎", "hé ho! h😎you√2?"),
        ("middle", 22, "😎", "hé ho! h😎 you√2?"),
        ("middle", 23, "😎", "hé ho! ho😎 you√2?"),
        ("middle", 24, "😎", "hé ho! how are you√2?"),
        ("middle", 50, "😎", "hé ho! how are you√2?"),
        ("middle", 3, "😎", None),
        ("middle", 4, "😎", "😎"),
        ("middle", 5, "😎", "h😎"),
        ("middle", 6, "😎", "h😎?"),
        ("middle", 7, "😎", "h😎?"),
    ),
)
def test_unicode_truncate(
    position: typing.Literal["end", "middle"],
    length: int,
    placeholder: str,
    expected: str | None,
) -> None:
    s = "hé ho! how are you√2?"
    if expected is None:
        with pytest.raises(ValueError):
            utils.unicode_truncate(s, length, placeholder, position)
    else:
        result = utils.unicode_truncate(s, length, placeholder, position)
        assert len(result.encode()) <= length
        assert result == expected


def test_get_random_choices() -> None:
    choices = {
        "jd": 10,
        "sileht": 1,
        "foobar": 3,
    }
    assert utils.get_random_choices(0, choices, 1) == {"foobar"}
    assert utils.get_random_choices(1, choices, 1) == {"foobar"}
    assert utils.get_random_choices(2, choices, 1) == {"foobar"}
    assert utils.get_random_choices(3, choices, 1) == {"jd"}
    assert utils.get_random_choices(4, choices, 1) == {"jd"}
    assert utils.get_random_choices(11, choices, 1) == {"jd"}
    assert utils.get_random_choices(12, choices, 1) == {"jd"}
    assert utils.get_random_choices(13, choices, 1) == {"sileht"}
    assert utils.get_random_choices(14, choices, 1) == {"foobar"}
    assert utils.get_random_choices(15, choices, 1) == {"foobar"}
    assert utils.get_random_choices(16, choices, 1) == {"foobar"}
    assert utils.get_random_choices(17, choices, 1) == {"jd"}
    assert utils.get_random_choices(18, choices, 1) == {"jd"}
    assert utils.get_random_choices(19, choices, 1) == {"jd"}
    assert utils.get_random_choices(20, choices, 1) == {"jd"}
    assert utils.get_random_choices(21, choices, 1) == {"jd"}
    assert utils.get_random_choices(22, choices, 1) == {"jd"}
    assert utils.get_random_choices(23, choices, 1) == {"jd"}
    assert utils.get_random_choices(24, choices, 1) == {"jd"}
    assert utils.get_random_choices(25, choices, 1) == {"jd"}
    assert utils.get_random_choices(26, choices, 1) == {"jd"}
    assert utils.get_random_choices(27, choices, 1) == {"sileht"}
    assert utils.get_random_choices(28, choices, 1) == {"foobar"}
    assert utils.get_random_choices(29, choices, 1) == {"foobar"}
    assert utils.get_random_choices(30, choices, 1) == {"foobar"}
    assert utils.get_random_choices(31, choices, 1) == {"jd"}
    assert utils.get_random_choices(32, choices, 1) == {"jd"}
    assert utils.get_random_choices(23, choices, 2) == {"sileht", "jd"}
    assert utils.get_random_choices(2, choices, 2) == {"jd", "foobar"}
    assert utils.get_random_choices(4, choices, 2) == {"jd", "foobar"}
    assert utils.get_random_choices(0, choices, 3) == {"jd", "sileht", "foobar"}
    with pytest.raises(ValueError):
        assert utils.get_random_choices(4, choices, 4) == {"jd", "sileht"}


def test_to_ordinal_numeric() -> None:
    with pytest.raises(ValueError):
        utils.to_ordinal_numeric(-1)

    assert utils.to_ordinal_numeric(0) == "0th"
    assert utils.to_ordinal_numeric(100) == "100th"
    assert utils.to_ordinal_numeric(1) == "1st"
    assert utils.to_ordinal_numeric(11) == "11th"
    assert utils.to_ordinal_numeric(12) == "12th"
    assert utils.to_ordinal_numeric(13) == "13th"
    assert utils.to_ordinal_numeric(2) == "2nd"
    assert utils.to_ordinal_numeric(111) == "111th"
    assert utils.to_ordinal_numeric(112) == "112th"
    assert utils.to_ordinal_numeric(113) == "113th"
    assert utils.to_ordinal_numeric(42) == "42nd"
    assert utils.to_ordinal_numeric(6543512) == "6543512th"
    assert utils.to_ordinal_numeric(6543522) == "6543522nd"
    assert utils.to_ordinal_numeric(3) == "3rd"
    assert utils.to_ordinal_numeric(5743) == "5743rd"
    for i in range(4, 10):
        assert utils.to_ordinal_numeric(i) == f"{i}th"

    assert utils.to_ordinal_numeric(4567) == "4567th"
    assert utils.to_ordinal_numeric(5743) == "5743rd"


def test_split_list() -> None:
    assert list(utils.split_list([1, 2, 3, 4, 5, 6, 7, 8, 9], 2)) == [
        [1, 2, 3, 4, 5],
        [6, 7, 8, 9],
    ]
    assert list(utils.split_list([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)) == [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
    ]
    assert list(utils.split_list([1, 2, 3, 4, 5, 6, 7, 8, 9], 4)) == [
        [1, 2, 3],
        [4, 5, 6],
        [7, 8, 9],
    ]
    assert list(utils.split_list([1, 2], 4)) == [
        [1],
        [2],
    ]
    assert list(utils.split_list([1, 2], 2)) == [
        [1],
        [2],
    ]
    assert list(utils.split_list([1], 2)) == [
        [1],
    ]


def test_payload_dumper() -> None:
    expected_data = {"data": True}
    payload = utils.get_mergify_payload(expected_data)
    message = f"somecontent\n{payload}\nwhatever"
    data = utils.get_hidden_payload_from_comment_body(message)
    assert data == expected_data


@freeze_time("2022-08-03T15:43:50.478Z")
def test_get_retention_minid() -> None:
    retention = datetime.timedelta(days=1)
    expected_minid = 1659455030478  # 2022-08-02T15:43:50.478Z
    assert redis_utils.get_expiration_minid(retention) == expected_minid


@pytest.mark.parametrize(
    "string,expected",
    (
        ("n", False),
        ("no", False),
        ("false", False),
        ("f", False),
        ("0", False),
        ("off", False),
        ("y", True),
        ("yes", True),
        ("t", True),
        ("on", True),
        ("1", True),
    ),
)
def test_strtobool(string: str, expected: bool) -> None:
    assert utils.strtobool(string) == expected


def test_strtobool_exc() -> None:
    with pytest.raises(ValueError):
        utils.strtobool("test")


@pytest.mark.parametrize(
    "string,expected",
    (
        ("<!-- test1 -->", "test1"),
        ("<!-- test2      -->", "test2"),
        ("<!--        test 3 -->", "test 3"),
    ),
)
def test_stip_comment_tags(string: str, expected: str) -> None:
    assert utils.strip_comment_tags(string) == expected


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/mergifyio",
        "https://github.com//mergifyio",
        "https://github.com//mergifyio//",
    ],
)
def test_url_parser_with_owner_ok(url: str) -> None:
    assert utils.github_url_parser(url) == ("mergifyio", None, None, None)


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/mergifyio/mergify-engine",
        "https://github.com//mergifyio//mergify-engine//",
    ],
)
def test_url_parser_with_repo_ok(url: str) -> None:
    assert utils.github_url_parser(url) == ("mergifyio", "mergify-engine", None, None)


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
    assert utils.github_url_parser(url) == (
        github_types.GitHubLogin("mergifyio"),
        github_types.GitHubRepositoryName("mergify-engine"),
        github_types.GitHubPullRequestNumber(123),
        None,
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://github.com/mergifyio/mergify-engine/branch/main",
        "https://github.com/mergifyio/mergify-engine/branch/main#",
        "https://github.com/mergifyio/mergify-engine/branch/main?",
    ],
)
def test_url_parser_with_branch_ok(url: str) -> None:
    assert utils.github_url_parser(url) == (
        github_types.GitHubLogin("mergifyio"),
        github_types.GitHubRepositoryName("mergify-engine"),
        None,
        github_types.GitHubRefType("main"),
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
        utils.github_url_parser(url)
