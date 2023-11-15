import pytest

from mergify_engine.rules import filter


@pytest.mark.parametrize(
    "value, expected",
    (
        (True, False),
        (False, True),
        (filter.UnknownOnlyAttribute, filter.UnknownOnlyAttribute),
        (filter.UnknownOrFalseAttribute, filter.UnknownOrTrueAttribute),
        (filter.UnknownOrTrueAttribute, filter.UnknownOrFalseAttribute),
    ),
)
async def test_operator_negate(
    value: filter.TernaryFilterResult,
    expected: filter.TernaryFilterResult,
) -> None:
    result = filter.TernaryFilterOperatorNegate(value)
    assert result == expected


@pytest.mark.parametrize(
    "values, expected",
    (
        ([], False),
        ([False, False], False),
        ([False, True], True),
        ([True, False], True),
        ([True, True], True),
        (
            [filter.UnknownOnlyAttribute, filter.UnknownOnlyAttribute],
            filter.UnknownOnlyAttribute,
        ),
        (
            [filter.UnknownOnlyAttribute, False],
            filter.UnknownOnlyAttribute,
        ),
        (
            [filter.UnknownOrFalseAttribute, False],
            filter.UnknownOnlyAttribute,
        ),
        (
            [filter.UnknownOrTrueAttribute, False],
            filter.UnknownOrTrueAttribute,
        ),
        (
            [filter.UnknownOrTrueAttribute, filter.UnknownOrFalseAttribute],
            filter.UnknownOrTrueAttribute,
        ),
        (
            [
                filter.UnknownOrTrueAttribute,
                filter.UnknownOrFalseAttribute,
                filter.UnknownOnlyAttribute,
            ],
            filter.UnknownOrTrueAttribute,
        ),
        (
            [filter.UnknownOrTrueAttribute, filter.UnknownOrFalseAttribute, False],
            filter.UnknownOrTrueAttribute,
        ),
        (
            [filter.UnknownOnlyAttribute, True],
            True,
        ),
        (
            [filter.UnknownOrFalseAttribute, True],
            True,
        ),
        (
            [filter.UnknownOrTrueAttribute, True],
            True,
        ),
    ),
)
async def test_operator_any(
    values: list[filter.TernaryFilterResult],
    expected: filter.TernaryFilterResult,
) -> None:
    result = filter.TernaryFilterOperatorAny(values)
    assert result == expected


@pytest.mark.parametrize(
    "values, expected",
    (
        ([], True),
        ([False, False], False),
        ([False, True], False),
        ([True, False], False),
        ([True, True], True),
        (
            [filter.UnknownOnlyAttribute, filter.UnknownOnlyAttribute],
            filter.UnknownOnlyAttribute,
        ),
        (
            [filter.UnknownOnlyAttribute, False],
            False,
        ),
        (
            [filter.UnknownOrFalseAttribute, False],
            False,
        ),
        (
            [filter.UnknownOrTrueAttribute, False],
            False,
        ),
        (
            [filter.UnknownOrTrueAttribute, filter.UnknownOrFalseAttribute],
            filter.UnknownOrFalseAttribute,
        ),
        (
            [
                filter.UnknownOrTrueAttribute,
                filter.UnknownOrFalseAttribute,
                filter.UnknownOnlyAttribute,
            ],
            filter.UnknownOrFalseAttribute,
        ),
        (
            [filter.UnknownOrTrueAttribute, filter.UnknownOrFalseAttribute, False],
            False,
        ),
        (
            [filter.UnknownOnlyAttribute, True],
            filter.UnknownOrTrueAttribute,
        ),
        (
            [filter.UnknownOrTrueAttribute, True],
            filter.UnknownOrTrueAttribute,
        ),
        (
            [filter.UnknownOrFalseAttribute, True],
            filter.UnknownOrFalseAttribute,
        ),
    ),
)
async def test_operator_all(
    values: list[filter.TernaryFilterResult],
    expected: filter.TernaryFilterResult,
) -> None:
    result = filter.TernaryFilterOperatorAll(values)
    assert result == expected
