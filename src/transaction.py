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
    sell_price: decimal.Decimal = decimal.Decimal()
    real_gain: decimal.Decimal = decimal.Decimal()
    remark: str = ""


# Functions


def time_batches(
    operations: list[Operation],
    max_difference: typing.Optional[int],
    max_size: typing.Optional[int] = None,
) -> typing.Iterable[list[datetime.datetime]]:
    """Return timestamps of operations in batches.

    The batches are clustered such that the batches time difference
    from first to last operation is lesser than `max_difference` minutes and the
    batches have a maximum size of `max_size`.

    TODO Solve the clustering optimally. (It's already optimal, if max_size is None.)

    Args:
        operations (list[Operation]): List of operations.
        max_difference (Optional[int], optional):
            Maximal time difference in batch (in minutes).
            Defaults to None (unlimited time difference).
        limax_sizemit (Optional[int], optional):
            Maximum size of batch.
            Defaults to None (unlimited size).

    Yields:
        Generator[None, list[datetime.datetime], None]: Yield the timestamp clusters.
    """
    assert max_difference is None or max_difference >= 0
    assert max_size is None or max_size > 0

    batch: list[datetime.datetime] = []

    if not operations:
        # Nothing to cluster, return empty list.
        return batch

    # Calculate the latest time which is allowed to be in this cluster.
    if max_difference:
        max_time = operations[0].utc_time + datetime.timedelta(minutes=max_difference)
    else:
        max_time = datetime.datetime.max

    for op in operations:
        timestamp = op.utc_time

        # Check if timestamp is before max_time and
        # that our cluster isn't to large already.
        if timestamp < max_time and (not max_size or len(batch) < max_size):
            batch.append(timestamp)
        else:
            yield batch

            batch = [timestamp]

            if max_difference:
                max_time = timestamp + datetime.timedelta(minutes=max_difference)
    yield batch  # fixes bug where last batch ist not yielded


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
