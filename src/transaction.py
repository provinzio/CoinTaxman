# CoinTaxman
# Copyright (C) 2021  Carsten Docktor <https://github.com/provinzio>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations

import dataclasses
import datetime
import decimal
import itertools
import typing
from copy import copy
from pathlib import Path
from typing import ClassVar, Iterator, Optional

import log_config
import misc

log = log_config.getLogger(__name__)


# TODO Implementation might be cleaner, when we add a class AbstractOperation
# which gets inherited by Fee and Operation
# Currently it might be possible for fees to have fees, which is unwanted.


@dataclasses.dataclass
class Operation:
    utc_time: datetime.datetime
    platform: str
    change: decimal.Decimal
    coin: str
    line: list[int]
    file_path: Path
    fees: "Optional[list[Fee]]" = None

    @classmethod
    def type_name_c(cls) -> str:
        return cls.__name__

    @property
    def type_name(self) -> str:
        return self.type_name_c()

    identical_columns: ClassVar[list[str]] = [
        "type_name",
        "utc_time",
        "platform",
        "coin",
    ]

    def __post_init__(self):
        assert self.validate_types()

        if self.change < 0:
            raise ValueError("Operation.change must be positive.")

    def validate_types(self) -> bool:
        ret = True
        for field in dataclasses.fields(self):
            if isinstance(field.type, typing._SpecialForm):
                # No check for typing.Any, typing.Union, typing.ClassVar
                # (without parameters)
                continue

            actual_value = getattr(self, field.name)

            if field.name == "fees":
                # BUG currently kind of ignored, would be nice when
                # implemented correctly.
                assert actual_value is None
                continue

            actual_type = typing.get_origin(field.type) or field.type

            if isinstance(actual_type, typing._SpecialForm):
                actual_type = field.type.__args__
            elif isinstance(actual_type, str):
                while isinstance(actual_type, str):
                    # BUG row:list[int] value gets only checked for list.
                    # not as list[int]
                    if actual_type.startswith("list["):
                        actual_type = list
                    else:
                        actual_type = eval(actual_type)

            if not isinstance(actual_value, actual_type):
                log.warning(
                    f"\t{field.name}: '{type(actual_value)}' "
                    f"instead of '{field.type}'"
                )
                ret = False
        return ret

    def identical_to(self, op: Operation) -> bool:
        identical_to = all(
            getattr(self, i) == getattr(op, i) for i in self.identical_columns
        )

        if identical_to:
            assert (
                self.file_path == op.file_path
            ), "Identical operations should also be in the same file."

        return identical_to

    @staticmethod
    def merge(*operations: Operation) -> Operation:
        assert len(operations) > 0, "There have to be operations to be merged."
        assert all(
            op1.identical_to(op2) for op1, op2 in itertools.combinations(operations, 2)
        ), "Operations have to be identical to be merged"

        # Select arbitray operation from list.
        o = copy(operations[0])
        # Update this operation with merged entries.
        o.change = misc.dsum(op.change for op in operations)
        o.line = list(itertools.chain(*(op.line for op in operations)))
        if not all(op.fees is None for op in operations):
            raise NotImplementedError(
                "merging operations with fees is currently not supported"
            )
        return o


class Fee(Operation):
    pass


class CoinLend(Operation):
    pass


class CoinLendEnd(Operation):
    pass


class Staking(Operation):
    """Cold Staking or Proof Of Stake (not for mining)"""

    pass


class StakingEnd(Operation):
    """Cold Staking or Proof Of Stake (not for mining)"""

    pass


class Transaction(Operation):
    pass


class Buy(Transaction):
    pass


class Sell(Transaction):
    pass


class CoinLendInterest(Transaction):
    pass


class StakingInterest(Transaction):
    """Cold Staking or Proof Of Stake (not for mining)"""

    pass


class Airdrop(Transaction):
    pass


class Commission(Transaction):
    pass


class Deposit(Transaction):
    link: Optional[Withdrawal] = None


class Withdrawal(Transaction):
    withdrawn_coins: Optional[list[SoldCoin]]

    def partial_withdrawn_coins(self, percent: decimal.Decimal) -> list[SoldCoin]:
        assert self.withdrawn_coins
        withdrawn_coins = [wc.partial(percent) for wc in self.withdrawn_coins]
        assert self.change == misc.dsum(
            (wsc.sold for wsc in withdrawn_coins)
        ), "Withdrawn coins total must be equal to the sum if the single coins."
        return withdrawn_coins


# Helping variables


@dataclasses.dataclass
class SoldCoin:
    op: Operation
    sold: decimal.Decimal

    def partial(self, percent: decimal.Decimal) -> SoldCoin:
        sc = copy(self)
        sc.sold *= percent
        sc.op.change *= percent
        return sc


@dataclasses.dataclass
class TaxReportEntry:
    event_type = "virtual"

    first_platform: Optional[str] = None
    second_platform: Optional[str] = None

    amount: Optional[decimal.Decimal] = None
    coin: Optional[str] = None

    first_utc_time: Optional[datetime.datetime] = None
    second_utc_time: Optional[datetime.datetime] = None

    # Fee might be paid in multiple coin types (e.g. Binance BNB)
    first_fee_amount: Optional[decimal.Decimal] = None
    first_fee_coin: Optional[str] = None
    first_fee_in_fiat: Optional[decimal.Decimal] = None
    #
    second_fee_amount: Optional[decimal.Decimal] = None
    second_fee_coin: Optional[str] = None
    second_fee_in_fiat: Optional[decimal.Decimal] = None

    first_value_in_fiat: Optional[decimal.Decimal] = None
    second_value_in_fiat: Optional[decimal.Decimal] = None
    total_fee_in_fiat: Optional[decimal.Decimal] = dataclasses.field(init=False)

    @property
    def _total_fee_in_fiat(self) -> Optional[decimal.Decimal]:
        if self.first_fee_in_fiat is None and self.second_fee_in_fiat is None:
            return None
        return misc.dsum(
            map(
                # TODO Report mypy bug
                misc.cdecimal,
                (self.first_fee_in_fiat, self.second_fee_in_fiat),
            )
        )

    gain_in_fiat: Optional[decimal.Decimal] = dataclasses.field(init=False)

    @property
    def _gain_in_fiat(self) -> Optional[decimal.Decimal]:
        if (
            self.first_value_in_fiat is None
            and self.second_value_in_fiat is None
            and self._total_fee_in_fiat is None
        ):
            return None
        return (
            misc.cdecimal(self.first_value_in_fiat)
            - misc.cdecimal(self.second_value_in_fiat)
            - misc.cdecimal(self._total_fee_in_fiat)
        )

    is_taxable: Optional[bool] = None
    taxation_type: Optional[str] = None
    remark: Optional[str] = None

    @property
    def taxable_gain(self) -> decimal.Decimal:
        if self.is_taxable and self._gain_in_fiat:
            return self._gain_in_fiat
        return decimal.Decimal()

    # Copy-paste template for subclasses.
    # def __init__(
    #     self,
    #     first_platform: str,
    #     second_platform: str,
    #     amount: decimal.Decimal,
    #     coin: str,
    #     first_utc_time: datetime.datetime,
    #     second_utc_time: datetime.datetime,
    #     first_fee_amount: decimal.Decimal,
    #     first_fee_coin: str,
    #     first_fee_in_fiat: decimal.Decimal,
    #     second_fee_amount: decimal.Decimal,
    #     second_fee_coin: str,
    #     second_fee_in_fiat: decimal.Decimal,
    #     first_value_in_fiat: decimal.Decimal,
    #     second_value_in_fiat: decimal.Decimal,
    #     is_taxable: bool,
    #     taxation_type: str,
    #     remark: str,
    # ) -> None:
    #     super().__init__(
    #         first_platform=first_platform,
    #         second_platform=second_platform,
    #         amount=amount,
    #         coin=coin,
    #         first_utc_time=first_utc_time,
    #         second_utc_time=second_utc_time,
    #         first_fee_amount=first_fee_amount,
    #         first_fee_coin=first_fee_coin,
    #         first_fee_in_fiat=first_fee_in_fiat,
    #         second_fee_amount=second_fee_amount,
    #         second_fee_coin=second_fee_coin,
    #         second_fee_in_fiat=second_fee_in_fiat,
    #         first_value_in_fiat=first_value_in_fiat,
    #         second_value_in_fiat=second_value_in_fiat,
    #         is_taxable=is_taxable,
    #         taxation_type=taxation_type,
    #         remark=remark,
    #     )

    def __post_init__(self) -> None:
        """Validate that all required fields (label != '-') are given."""
        missing_field_values = [
            field_name
            for label, field_name in zip(self.labels(), self.field_names())
            if label != "-" and getattr(self, field_name) is None
        ]
        assert not missing_field_values, (
            f"{self=} : missing values for fields " f"{', '.join(missing_field_values)}"
        )

    @classmethod
    def field_names(cls) -> Iterator[str]:
        return (field.name for field in dataclasses.fields(cls))

    @classmethod
    def _labels(cls) -> list[str]:
        return list(cls.field_names())

    @classmethod
    def labels(cls) -> list[str]:
        labels = cls._labels()
        assert len(labels) == len(dataclasses.fields(cls))
        return labels

    def values(self) -> Iterator:
        return (getattr(self, f) for f in self.field_names())


# Bypass dataclass machinery, add a custom property function to a dataclass field.
TaxReportEntry.total_fee_in_fiat = TaxReportEntry._total_fee_in_fiat  # type:ignore
TaxReportEntry.gain_in_fiat = TaxReportEntry._gain_in_fiat  # type:ignore


class LendingReportEntry(TaxReportEntry):
    event_type = "Coin-Lending Zeitraum"

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Börse",
            "-",
            #
            "Anzahl",
            "Währung",
            #
            "Wiedererhalten am",
            "Verliehen am",
            #
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            #
            "-",
            "-",
            "-",
            #
            "Gewinn/Verlust in EUR",
            "davon steuerbar",
            "Einkunftsart",
            "Bemerkung",
        ]


class StakingReportEntry(LendingReportEntry):
    event_type = "Staking Zeitaraum"


class SellReportEntry(TaxReportEntry):
    event_type = "Verkauf"

    def __init__(
        self,
        sell_platform: str,
        buy_platform: str,
        amount: decimal.Decimal,
        coin: str,
        sell_utc_time: datetime.datetime,
        buy_utc_time: datetime.datetime,
        first_fee_amount: decimal.Decimal,
        first_fee_coin: str,
        first_fee_in_fiat: decimal.Decimal,
        second_fee_amount: decimal.Decimal,
        second_fee_coin: str,
        second_fee_in_fiat: decimal.Decimal,
        sell_value_in_fiat: decimal.Decimal,
        buy_value_in_fiat: decimal.Decimal,
        is_taxable: bool,
        taxation_type: str,
        remark: str,
    ) -> None:
        super().__init__(
            first_platform=sell_platform,
            second_platform=buy_platform,
            amount=amount,
            coin=coin,
            first_utc_time=sell_utc_time,
            second_utc_time=buy_utc_time,
            first_fee_amount=first_fee_amount,
            first_fee_coin=first_fee_coin,
            first_fee_in_fiat=first_fee_in_fiat,
            second_fee_amount=second_fee_amount,
            second_fee_coin=second_fee_coin,
            second_fee_in_fiat=second_fee_in_fiat,
            first_value_in_fiat=sell_value_in_fiat,
            second_value_in_fiat=buy_value_in_fiat,
            is_taxable=is_taxable,
            taxation_type=taxation_type,
            remark=remark,
        )

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Verkauf auf Börse",
            "Erworben von Börse",
            #
            "Anzahl",
            "Währung",
            #
            "Verkaufsdatum",
            "Erwerbsdatum",
            #
            "(1) Anzahl Transaktionsgebühr",
            "(1) Währung Transaktionsgebühr",
            "(1) Transaktionsgebühr in EUR",
            "(2) Anzahl Transaktionsgebühr",
            "(2) Währung Transaktionsgebühr",
            "(2) Transaktionsgebühr in EUR",
            #
            "Veräußerungserlös in EUR",
            "Anschaffungskosten in EUR",
            "Gesamt Transaktionsgebühr in EUR",
            #
            "Gewinn/Verlust in EUR",
            "davon steuerbar",
            "Einkunftsart",
            "Bemerkung",
        ]


class UnrealizedSellReportEntry(SellReportEntry):
    event_type = "Offene Positionen"

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Virtueller Verkauf auf Börse",
            "Erworben von Börse",
            #
            "Anzahl",
            "Währung",
            #
            "Virtuelles Verkaufsdatum",
            "Erwerbsdatum",
            #
            "(1) Anzahl Transaktionsgebühr",
            "(1) Währung Transaktionsgebühr",
            "(1) Transaktionsgebühr in EUR",
            "(2) Anzahl Transaktionsgebühr",
            "(2) Währung Transaktionsgebühr",
            "(2) Transaktionsgebühr in EUR",
            #
            "Virtueller Veräußerungserlös in EUR",
            "Virtuelle Anschaffungskosten in EUR",
            "Virtuelle Gesamt Transaktionsgebühr in EUR",
            #
            "Virtueller Gewinn/Verlust in EUR",
            "davon wären steuerbar",
            "Einkunftsart",
            "Bemerkung",
        ]


class InterestReportEntry(TaxReportEntry):
    event_type = "Zinsen"

    def __init__(
        self,
        platform: str,
        amount: decimal.Decimal,
        utc_time: datetime.datetime,
        coin: str,
        interest_in_fiat: decimal.Decimal,
        taxation_type: str,
        remark: str,
    ) -> None:
        super().__init__(
            first_platform=platform,
            amount=amount,
            first_utc_time=utc_time,
            coin=coin,
            first_value_in_fiat=interest_in_fiat,
            is_taxable=True,
            taxation_type=taxation_type,
            remark=remark,
        )

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Börse",
            "-",
            #
            "Anzahl",
            "Währung",
            #
            "Erhalten am",
            "-",
            #
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            #
            "Wert in EUR",
            "-",
            "-",
            #
            "Gewinn/Verlust in EUR",
            "davon steuerbar",
            "Einkunftsart",
            "Bemerkung",
        ]


class LendingInterestReportEntry(InterestReportEntry):
    event_type = "Coin-Lending"


class StakingInterestReportEntry(InterestReportEntry):
    event_type = "Staking"


class AirdropReportEntry(TaxReportEntry):
    event_type = "Airdrop"

    def __init__(
        self,
        platform: str,
        amount: decimal.Decimal,
        coin: str,
        utc_time: datetime.datetime,
        in_fiat: decimal.Decimal,
        taxation_type: str,
        remark: str,
    ) -> None:
        super().__init__(
            first_platform=platform,
            amount=amount,
            coin=coin,
            first_utc_time=utc_time,
            first_value_in_fiat=in_fiat,
            is_taxable=True,
            taxation_type=taxation_type,
            remark=remark,
        )

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Börse",
            "-",
            #
            "Anzahl",
            "Währung",
            #
            "Erhalten am",
            "-",
            #
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            #
            "Wert in EUR",
            "-",
            "-",
            #
            "Gewinn/Verlust in EUR",
            "davon steuerbar",
            "Einkunftsart",
            "Bemerkung",
        ]


class CommissionReportEntry(AirdropReportEntry):
    event_type = "Kommission"  # TODO gibt es eine bessere Bezeichnung?


class TransferReportEntry(TaxReportEntry):
    event_type = "Transfer von Kryptowährung"

    def __init__(
        self,
        first_platform: str,
        second_platform: str,
        amount: decimal.Decimal,
        coin: str,
        first_utc_time: datetime.datetime,
        second_utc_time: datetime.datetime,
        first_fee_amount: decimal.Decimal,
        first_fee_coin: str,
        first_fee_in_fiat: decimal.Decimal,
        remark: str,
    ) -> None:
        super().__init__(
            first_platform=first_platform,
            second_platform=second_platform,
            amount=amount,
            coin=coin,
            first_utc_time=first_utc_time,
            second_utc_time=second_utc_time,
            first_fee_amount=first_fee_amount,
            first_fee_coin=first_fee_coin,
            first_fee_in_fiat=first_fee_in_fiat,
            remark=remark,
        )

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Eingang auf Börse",
            "Ausgang von Börse",
            #
            "Anzahl",
            "Währung",
            #
            "Eingangsdatum",
            "Ausgangsdatum",
            #
            "(1) Anzahl Transaktionsgebühr",
            "(1) Währung Transaktionsgebühr",
            "(1) Transaktionsgebühr in EUR",
            "-",
            "-",
            "-",
            #
            "-",
            "-",
            "Gesamt Transaktionsgebühr in EUR",
            #
            "Gewinn/Verlust in EUR",
            "-",
            "-",
            "Bemerkung",
        ]


gain_operations = [
    CoinLendEnd,
    StakingEnd,
    Buy,
    CoinLendInterest,
    StakingInterest,
    Airdrop,
    Commission,
    Deposit,
]
loss_operations = [
    Fee,
    CoinLend,
    Staking,
    Sell,
    Withdrawal,
]
operations_order = gain_operations + loss_operations


def sort_operations(
    operations: list[Operation],
    keys: typing.Optional[list[str]] = None,
) -> list[Operation]:
    """Sort operations by `operations_order` and arbitrary keys/members.

    If the operation type is missing in `operations_order`. The operation
    will be placed first.

    Args:
        operations (list[Operation]): Operations to be sorted.
        keys (list[str], optional): List of operation members which will be considered
                                    when sorting. Defaults to None.

    Returns:
        list[Operation]: Sorted operations by `operations_order` and specific keys.
    """

    def key_function(op: Operation) -> tuple:
        try:
            idx = operations_order.index(type(op))
        except ValueError:
            idx = 0
        return tuple(([getattr(op, key) for key in keys] if keys else []) + [idx])

    return sorted(operations, key=key_function)
