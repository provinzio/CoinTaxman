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

import config
import log_config
import misc

log = log_config.getLogger(__name__)


# TODO Implementation might be cleaner, when we add a class AbstractOperation
#      which gets inherited by Fee and Operation
#      Currently it might be possible for fees to have fees, which is unwanted.


@dataclasses.dataclass
class Operation:
    utc_time: datetime.datetime
    platform: str
    change: decimal.Decimal
    coin: str
    line: list[int]
    file_path: Path
    fees: "Optional[list[Fee]]" = None
    remarks: list[str] = dataclasses.field(default_factory=list)

    @property
    def remark(self) -> str:
        return ", ".join(self.remarks)

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
                # TODO currently kind of ignored, would be nice when
                #      implemented correctly.
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
    link: Optional[Sell] = None
    buying_cost: Optional[decimal.Decimal] = None


class Sell(Transaction):
    link: Optional[Buy] = None
    selling_value: Optional[decimal.Decimal] = None


class CoinLendInterest(Transaction):
    pass


class StakingInterest(Transaction):
    """Cold Staking or Proof Of Stake (not for mining)"""

    pass


class Airdrop(Transaction):
    taxation_type: Optional[str] = None

class AirdropGift(Airdrop):
    """AirdropGift is used for gifts that are non-taxable"""

    taxation_type: Optional[str] = "Schenkung"

class AirdropIncome(Airdrop):
    """AirdropIncome is used for income that is taxable"""

    taxation_type: Optional[str] = "Einkünfte aus sonstigen Leistungen"

class Commission(Transaction):
    pass


class Deposit(Transaction):
    link: Optional[Withdrawal] = None


class Withdrawal(Transaction):
    withdrawn_coins: Optional[list[SoldCoin]] = None
    has_link: bool = False

    def partial_withdrawn_coins(self, percent: decimal.Decimal) -> list[SoldCoin]:
        assert self.withdrawn_coins
        withdrawn_coins = [wc.partial(percent) for wc in self.withdrawn_coins]
        assert percent * self.change == misc.dsum(
            (wsc.sold for wsc in withdrawn_coins)
        ), "Withdrawn coins total must be equal to the sum if the single coins."
        return withdrawn_coins


# Helping variables


@dataclasses.dataclass
class SoldCoin:
    op: Operation
    sold: decimal.Decimal

    def __post_init__(self):
        self.validate()

    def validate(self) -> None:
        assert self.sold <= self.op.change

    def partial(self, percent: decimal.Decimal) -> SoldCoin:
        return SoldCoin(self.op, self.sold * percent)


@dataclasses.dataclass
class TaxReportEntry:
    event_type: ClassVar[str] = "virtual"
    allowed_missing_fields: ClassVar[list[str]] = []
    abs_gain_loss: ClassVar[bool] = False

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
        gain_in_fiat = (
            misc.cdecimal(self.first_value_in_fiat)
            - misc.cdecimal(self.second_value_in_fiat)
            - misc.cdecimal(self._total_fee_in_fiat)
        )
        if self.abs_gain_loss:
            gain_in_fiat = abs(gain_in_fiat)
        return gain_in_fiat

    taxable_gain_in_fiat: decimal.Decimal = dataclasses.field(init=False)

    @property
    def _taxable_gain_in_fiat(self) -> Optional[decimal.Decimal]:
        if self.is_taxable and self._gain_in_fiat:
            return self._gain_in_fiat
        if self.get_excel_label("taxable_gain_in_fiat") == "-":
            return None
        return decimal.Decimal()

    is_taxable: Optional[bool] = None
    taxation_type: Optional[str] = None
    remark: Optional[str] = None

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
            field.name
            for label, field in zip(self.excel_labels(), self.excel_fields())
            if label != "-"
            and getattr(self, field.name) is None
            and field.name not in self.allowed_missing_fields
        ]
        assert not missing_field_values, (
            f"{self=} : missing values for fields " f"{', '.join(missing_field_values)}"
        )
        assert len(self.excel_labels()) == len(self.excel_fields())

    @classmethod
    def fields(cls) -> tuple[dataclasses.Field, ...]:
        return dataclasses.fields(cls)

    @classmethod
    def field_names(cls) -> Iterator[str]:
        return (field.name for field in cls.fields())

    @classmethod
    def _labels(cls) -> list[str]:
        return list(cls.field_names())

    @classmethod
    def labels(cls) -> list[str]:
        labels = cls._labels()
        assert len(labels) == len(dataclasses.fields(cls)) - 1
        return labels

    @classmethod
    def get_excel_label(cls, field_name: str) -> str:
        assert len(cls.excel_labels()) == len(cls.excel_fields())
        for label, field in zip(cls.excel_labels(), cls.excel_fields()):
            if field.name == field_name:
                return label
        raise ValueError(f"{field_name} is not a field of {cls=}")

    def values(self) -> Iterator:
        return (getattr(self, f) for f in self.field_names())

    @staticmethod
    def is_excel_label(label: str) -> bool:
        return label != "is_taxable"

    @classmethod
    def excel_fields(cls) -> tuple[dataclasses.Field, ...]:
        return tuple(field for field in cls.fields() if cls.is_excel_label(field.name))

    @classmethod
    def excel_labels(self) -> list[str]:
        return [label for label in self.labels() if self.is_excel_label(label)]

    @classmethod
    def excel_field_and_width(cls) -> Iterator[tuple[dataclasses.Field, float, bool]]:
        for field in cls.fields():
            if cls.is_excel_label(field.name):
                label = cls.get_excel_label(field.name)
                if label == "-":
                    width = 15.0
                elif field.name == "taxation_type":
                    width = 43.0
                elif field.name == "taxable_gain_in_fiat":
                    width = 13.0
                elif (
                    field.name.endswith("_in_fiat")
                    or "coin" in field.name
                    or "platform" in field.name
                ):
                    width = 15.0
                elif field.type in ("datetime.datetime", "Optional[datetime.datetime]"):
                    width = 18.43
                elif field.type in ("decimal.Decimal", "Optional[decimal.Decimal]"):
                    width = 20.0
                else:
                    width = 18.0
                hidden = label == "-"
                yield field, width, hidden

    def excel_values(self) -> Iterator:
        for field_name in self.field_names():
            if self.is_excel_label(field_name):
                value = getattr(self, field_name)
                label = self.get_excel_label(field_name)
                if label == "-":
                    yield None
                else:
                    if isinstance(value, datetime.datetime):
                        value = value.astimezone(config.LOCAL_TIMEZONE)
                    yield value


# Bypass dataclass machinery, add a custom property function to a dataclass field.
TaxReportEntry.total_fee_in_fiat = TaxReportEntry._total_fee_in_fiat  # type:ignore
TaxReportEntry.gain_in_fiat = TaxReportEntry._gain_in_fiat  # type:ignore
TaxReportEntry.taxable_gain_in_fiat = (
    TaxReportEntry._taxable_gain_in_fiat  # type:ignore
)


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
            "davon steuerbar in EUR",
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
        buy_cost_in_fiat: decimal.Decimal,
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
            second_value_in_fiat=buy_cost_in_fiat,
            is_taxable=is_taxable,
            taxation_type=taxation_type,
            remark=remark,
        )

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Verkauf auf Börse",
            "Erworben auf Börse",
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
            "Werbungskosten in EUR",
            #
            "Gewinn/Verlust in EUR",
            "davon steuerbar in EUR",
            "Einkunftsart",
            "Bemerkung",
        ]


class UnrealizedSellReportEntry(TaxReportEntry):
    event_type = "Bestände"

    def __init__(
        self,
        sell_platform: str,
        buy_platform: str,
        amount: decimal.Decimal,
        coin: str,
        sell_utc_time: datetime.datetime,
        buy_utc_time: datetime.datetime,
        sell_value_in_fiat: decimal.Decimal,
        buy_cost_in_fiat: decimal.Decimal,
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
            first_value_in_fiat=sell_value_in_fiat,
            second_value_in_fiat=buy_cost_in_fiat,
            is_taxable=is_taxable,
            taxation_type=taxation_type,
            remark=remark,
        )

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "Bestand auf Börse zum Stichtag",
            "Erworben auf Börse",
            #
            "Anzahl",
            "Währung",
            #
            "Unrealisiertes Verkaufsdatum",
            "Erwerbsdatum",
            #
            "-",
            "-",
            "-",
            "-",
            "-",
            "-",
            #
            "Unrealisierter Veräußerungserlös in EUR",
            "Anschaffungskosten in EUR",
            "-",
            #
            "Unrealisierter Gewinn/Verlust in EUR",
            "davon wären steuerbar in EUR",
            "Einkunftsart",
            "Bemerkung",
        ]


class BuyReportEntry(TaxReportEntry):
    event_type = "Kauf"
    abs_gain_loss = True

    def __init__(
        self,
        platform: str,
        amount: decimal.Decimal,
        coin: str,
        utc_time: datetime.datetime,
        first_fee_amount: decimal.Decimal,
        first_fee_coin: str,
        first_fee_in_fiat: decimal.Decimal,
        second_fee_amount: decimal.Decimal,
        second_fee_coin: str,
        second_fee_in_fiat: decimal.Decimal,
        buy_value_in_fiat: decimal.Decimal,
        remark: str,
    ) -> None:
        super().__init__(
            second_platform=platform,
            amount=amount,
            coin=coin,
            second_utc_time=utc_time,
            first_fee_amount=first_fee_amount,
            first_fee_coin=first_fee_coin,
            first_fee_in_fiat=first_fee_in_fiat,
            second_fee_amount=second_fee_amount,
            second_fee_coin=second_fee_coin,
            second_fee_in_fiat=second_fee_in_fiat,
            second_value_in_fiat=buy_value_in_fiat,
            remark=remark,
        )

    @classmethod
    def _labels(cls) -> list[str]:
        return [
            "-",
            "Erworben auf Börse",
            #
            "Anzahl",
            "Währung",
            #
            "-",
            "Erwerbsdatum",
            #
            "(1) Anzahl Transaktionsgebühr",
            "(1) Währung Transaktionsgebühr",
            "(1) Transaktionsgebühr in EUR",
            "(2) Anzahl Transaktionsgebühr",
            "(2) Währung Transaktionsgebühr",
            "(2) Transaktionsgebühr in EUR",
            #
            "-",
            "Kaufpreis in EUR",
            "Werbungskosten in EUR",
            #
            "Anschaffungskosten in EUR",
            "-",
            "-",
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
            "davon steuerbar in EUR",
            "Einkunftsart",
            "Bemerkung",
        ]


class LendingInterestReportEntry(InterestReportEntry):
    event_type = "Coin-Lending Einkünfte"


class StakingInterestReportEntry(InterestReportEntry):
    event_type = "Staking Einkünfte"


class AirdropReportEntry(TaxReportEntry):
    event_type = "Airdrops"

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
            "davon steuerbar in EUR",
            "Einkunftsart",
            "Bemerkung",
        ]


class CommissionReportEntry(AirdropReportEntry):
    event_type = "Belohnungen-Bonus"


class TransferReportEntry(TaxReportEntry):
    event_type = "Ein-&Auszahlungen"
    abs_gain_loss = True

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
            "-",
            #
            "Kosten in EUR",
            "-",
            "-",
            "Bemerkung",
        ]


class DepositReportEntry(TransferReportEntry):
    allowed_missing_fields = ["second_platform", "second_utc_time"]

    def __init__(
        self,
        platform: str,
        amount: decimal.Decimal,
        coin: str,
        utc_time: datetime.datetime,
        first_fee_amount: decimal.Decimal,
        first_fee_coin: str,
        first_fee_in_fiat: decimal.Decimal,
        remark: str,
    ) -> None:
        TaxReportEntry.__init__(
            self,
            first_platform=platform,
            amount=amount,
            coin=coin,
            first_utc_time=utc_time,
            first_fee_amount=first_fee_amount,
            first_fee_coin=first_fee_coin,
            first_fee_in_fiat=first_fee_in_fiat,
            remark=remark,
        )


class WithdrawalReportEntry(TransferReportEntry):
    allowed_missing_fields = ["first_platform", "first_utc_time"]

    def __init__(
        self,
        platform: str,
        amount: decimal.Decimal,
        coin: str,
        utc_time: datetime.datetime,
        first_fee_amount: decimal.Decimal,
        first_fee_coin: str,
        first_fee_in_fiat: decimal.Decimal,
        remark: str,
    ) -> None:
        TaxReportEntry.__init__(
            self,
            second_platform=platform,
            amount=amount,
            coin=coin,
            second_utc_time=utc_time,
            first_fee_amount=first_fee_amount,
            first_fee_coin=first_fee_coin,
            first_fee_in_fiat=first_fee_in_fiat,
            remark=remark,
        )


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

tax_report_entry_order = [
    BuyReportEntry,
    SellReportEntry,
    LendingInterestReportEntry,
    StakingInterestReportEntry,
    InterestReportEntry,
    AirdropReportEntry,
    CommissionReportEntry,
    TransferReportEntry,
    DepositReportEntry,
    WithdrawalReportEntry,
    LendingReportEntry,
    StakingReportEntry,
    UnrealizedSellReportEntry,
]


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
    return misc.sort_by_order_and_key(operations_order, operations, keys=keys)


def sort_tax_report_entries(
    tax_report_entries: list[TaxReportEntry],
) -> list[TaxReportEntry]:
    return misc.sort_by_order_and_key(tax_report_entry_order, tax_report_entries)
