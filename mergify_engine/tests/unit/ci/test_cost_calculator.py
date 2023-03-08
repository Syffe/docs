import datetime

import pytest

from mergify_engine.ci import cost_calculator
from mergify_engine.ci import models


@pytest.mark.parametrize(
    "minutes,system,cores,expected_cost",
    (
        (3000, "Linux", 2, cost_calculator.Money("24.00")),
        (3000, "Linux", 64, cost_calculator.Money("768.00")),
        (1000, "Windows", 2, cost_calculator.Money("16.00")),
        (1000, "macOS", 3, cost_calculator.Money("80.00")),
    ),
)
def test_cost_calculator(
    minutes: int,
    system: models.OperatingSystem,
    cores: int,
    expected_cost: cost_calculator.Money,
) -> None:
    actual_cost = cost_calculator.CostCalculator.calculate(
        datetime.timedelta(minutes=minutes), system, cores
    )
    assert actual_cost == expected_cost
