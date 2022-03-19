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
import typing
from pathlib import Path

import log_config

log = log_config.getLogger(__name__)


@dataclasses.dataclass
class Operation:
    utc_time: datetime.datetime
    platform: str
    change: decimal.Decimal
    coin: str
    line: int
    file_path: Path

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

            actual_type = typing.get_origin(field.type) or field.type

            if isinstance(actual_type, str):
                actual_type = eval(actual_type)
            elif isinstance(actual_type, typing._SpecialForm):
                actual_type = field.type.__args__

            actual_value = getattr(self, field.name)
            if not isinstance(actual_value, actual_type):
                log.warning(
                    f"\t{field.name}: '{type(actual_value)}' "
                    f"instead of '{field.type}'"
                )
                ret = False
        return ret


class Fee(Operation):
    pass


class CoinLend(Operation):
    pass


class CoinLendEnd(Operation):
    pass


class Staking(Operation):
    pass


class StakingEnd(Operation):
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
    pass


class Airdrop(Transaction):
    pass


class Commission(Transaction):
    pass


class Deposit(Transaction):
    pass


class Withdrawal(Transaction):
    pass


# Helping variables


@dataclasses.dataclass
class SoldCoin:
    op: Operation
    sold: decimal.Decimal


@dataclasses.dataclass
class TaxEvent:
    taxation_type: str
    taxed_gain: decimal.Decimal
    op: Operation
    is_taxable: bool = True
    sell_value: decimal.Decimal = decimal.Decimal()
    real_gain: decimal.Decimal = decimal.Decimal()
    remark: str = ""


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

    def key(op: Operation) -> tuple:
        try:
            idx = operations_order.index(type(op))
        except ValueError:
            idx = 0
        return tuple(([getattr(op, key) for key in keys] if keys else []) + [idx])

    return sorted(operations, key=key)
