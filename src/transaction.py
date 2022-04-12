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
from typing import ClassVar, Optional

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
    link: typing.Optional[Withdrawal] = None


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

    def key_function(op: Operation) -> tuple:
        try:
            idx = operations_order.index(type(op))
        except ValueError:
            idx = 0
        return tuple(([getattr(op, key) for key in keys] if keys else []) + [idx])

    return sorted(operations, key=key_function)
