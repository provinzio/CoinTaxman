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
import logging
import typing

log = logging.getLogger(__name__)


@dataclasses.dataclass
class Operation:
    utc_time: datetime.datetime
    platform: str
    change: float
    coin: str

    def __post_init__(self):
        assert self.validate_types()

        if self.change < 0:
            raise ValueError("Operation.change must be positive.")

    def __lt__(self, other: Operation) -> bool:
        if issubclass(other.__class__, Operation):
            return self.utc_time < other.utc_time and self.platform < other.platform
        return NotImplemented

    def validate_types(self) -> bool:
        ret = True
        for field_name, field_def in self.__dataclass_fields__.items():
            if isinstance(field_def.type, typing._SpecialForm):
                # No check for typing.Any, typing.Union, typing.ClassVar (without parameters)
                continue

            actual_type = typing.get_origin(field_def.type) or field_def.type

            if isinstance(actual_type, str):
                actual_type = eval(actual_type)
            elif isinstance(actual_type, typing._SpecialForm):
                actual_type = field_def.type.__args__

            actual_value = getattr(self, field_name)
            if not isinstance(actual_value, actual_type):
                log.warning(
                    f"\t{field_name}: '{type(actual_value)}' instead of '{field_def.type}'")
                ret = False
        return ret


class Fee(Operation):
    pass


class CoinLend(Operation):
    pass


class CoinLendEnd(Operation):
    pass


@dataclasses.dataclass
class Transaction(Operation):
    pass


class Buy(Transaction):
    pass


class Sell(Transaction):
    pass


class CoinLendInterest(Transaction):
    pass


class Airdrop(Transaction):
    pass


class Deposit(Transaction):
    pass


class Withdraw(Transaction):
    pass


# Helping variables

@dataclasses.dataclass
class SoldCoin:
    op: Operation
    sold: float


@dataclasses.dataclass
class TaxEvent:
    taxation_type: str
    taxed_gain: float
    op: Operation
    remark: str = ""
