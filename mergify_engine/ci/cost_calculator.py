from __future__ import annotations

import datetime
import decimal
import math

from mergify_engine.ci import models


class MoneyAmount(decimal.Decimal):
    def __add__(self, __other: MoneyAmount | decimal.Decimal | int) -> MoneyAmount:
        return self.__class__(super().__add__(__other))

    def __sub__(self, __other: MoneyAmount | decimal.Decimal | int) -> MoneyAmount:
        return self.__class__(super().__sub__(__other))

    @classmethod
    def zero(cls) -> MoneyAmount:
        return cls(0)


# https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions#per-minute-rates
PER_MINUTE_RATES: dict[tuple[models.OperatingSystem, int], MoneyAmount] = {
    ("Linux", 2): MoneyAmount("0.008"),
    ("macOS", 3): MoneyAmount("0.08"),
    ("Windows", 2): MoneyAmount("0.016"),
    ("Linux", 4): MoneyAmount("0.016"),
    ("Linux", 8): MoneyAmount("0.032"),
    ("Linux", 16): MoneyAmount("0.064"),
    ("Linux", 32): MoneyAmount("0.128"),
    ("Linux", 64): MoneyAmount("0.256"),
    ("Windows", 8): MoneyAmount("0.064"),
    ("Windows", 16): MoneyAmount("0.128"),
    ("Windows", 32): MoneyAmount("0.256"),
    ("Windows", 64): MoneyAmount("0.512"),
}


class CostCalculator:
    @staticmethod
    def calculate(
        timing: datetime.timedelta,
        operating_system: models.OperatingSystem,
        cores: int,
    ) -> MoneyAmount:
        # https://docs.github.com/en/billing/managing-billing-for-github-actions/about-billing-for-github-actions#calculating-minute-and-storage-spending
        minutes = math.ceil(timing / datetime.timedelta(minutes=1))

        return MoneyAmount(minutes * PER_MINUTE_RATES[(operating_system, cores)])
