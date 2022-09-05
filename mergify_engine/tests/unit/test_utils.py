import datetime
import typing

from freezegun import freeze_time
import pytest

from mergify_engine import redis_utils
from mergify_engine import utils


@pytest.mark.parametrize(
    "length,placeholder,expected",
    (
        (0, "", ""),
        (1, "", "h"),
        (2, "", "h"),
        (3, "", "hé"),
        (4, "", "hé "),
        (10, "", "hé ho! ho"),
        (18, "", "hé ho! how are yo"),
        (19, "", "hé ho! how are you"),
        (20, "", "hé ho! how are you"),
        (21, "", "hé ho! how are you"),
        (22, "", "hé ho! how are you√"),
        (23, "", "hé ho! how are you√2"),
        (50, "", "hé ho! how are you√2?"),
        # ellipsis
        (0, "…", None),
        (1, "…", None),
        (2, "…", None),
        (3, "…", "…"),
        (4, "…", "h…"),
        (5, "…", "h…"),
        (6, "…", "hé…"),
        (7, "…", "hé …"),
        (13, "…", "hé ho! ho…"),
        (21, "…", "hé ho! how are yo…"),
        (22, "…", "hé ho! how are you…"),
        (23, "…", "hé ho! how are you…"),
        (24, "…", "hé ho! how are you√2?"),
        (50, "…", "hé ho! how are you√2?"),
        (21, "😎", "hé ho! how are y😎"),
        (22, "😎", "hé ho! how are yo😎"),
        (23, "😎", "hé ho! how are you😎"),
        (24, "😎", "hé ho! how are you√2?"),
        (50, "😎", "hé ho! how are you√2?"),
        (3, "😎", None),
        (4, "😎", "😎"),
        (5, "😎", "h😎"),
        (6, "😎", "h😎"),
        (7, "😎", "hé😎"),
    ),
)
def test_unicode_truncate(
    length: int,
    placeholder: str,
    expected: typing.Optional[str],
) -> None:
    s = "hé ho! how are you√2?"
    if expected is None:
        with pytest.raises(ValueError):
            utils.unicode_truncate(s, length, placeholder)
    else:
        result = utils.unicode_truncate(s, length, placeholder)
        assert len(result.encode()) <= length
        assert result == expected


def test_process_identifier() -> None:
    assert isinstance(utils._PROCESS_IDENTIFIER, str)


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
