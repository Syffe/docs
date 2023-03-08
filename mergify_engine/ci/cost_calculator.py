from __future__ import annotations

import datetime
import decimal
import math

from mergify_engine.ci import models


class Money(decimal.Decimal):
    def __add__(self, __other: Money | decimal.Decimal | int) -> Money:
        return self.__class__(super().__add__(__other))

    @classmethod
    def zero(cls) -> "Money":
        return cls(0)


# https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions#per-minute-rates
PER_MINUTE_RATES: dict[tuple[models.OperatingSystem, int], Money] = {
    ("Linux", 2): Money("0.008"),
    ("macOS", 3): Money("0.08"),
    ("Windows", 2): Money("0.016"),
    ("Linux", 4): Money("0.016"),
    ("Linux", 8): Money("0.032"),
    ("Linux", 16): Money("0.064"),
    ("Linux", 32): Money("0.128"),
    ("Linux", 64): Money("0.256"),
    ("Windows", 8): Money("0.064"),
    ("Windows", 16): Money("0.128"),
    ("Windows", 32): Money("0.256"),
    ("Windows", 64): Money("0.512"),
}


class CostCalculator:
    @staticmethod
    def calculate(
        timing: datetime.timedelta,
        operating_system: models.OperatingSystem,
        cores: int,
    ) -> Money:
        # https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions#calculating-minute-and-storage-spending
        minutes = math.ceil(timing / datetime.timedelta(minutes=1))

        return Money(minutes * PER_MINUTE_RATES[(operating_system, cores)])
