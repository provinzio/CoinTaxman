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

import abc
import collections
import dataclasses
import decimal
from typing import Union

import log_config
import transaction

log = log_config.getLogger(__name__)


@dataclasses.dataclass
class BalancedOperation:
    op: transaction.Operation
    sold: decimal.Decimal = decimal.Decimal()


class BalanceQueue(abc.ABC):
    def __init__(self) -> None:
        self.queue: collections.deque[BalancedOperation] = collections.deque()
        # Buffer fees which could not be directly set off
        # with the current coins in the queue.
        # This can happen if the exchange takes the fees before
        # the buy/sell process.
        self.buffer_fee = decimal.Decimal()

    def put(self, item: Union[transaction.Operation, BalancedOperation]) -> None:
        """Put a new item in the queue and set off buffered fees.

        Args:
            item (Union[Operation, BalancedOperation])
        """
        if isinstance(item, transaction.Operation):
            item = BalancedOperation(item)

        if not isinstance(item, BalancedOperation):
            raise ValueError

        self._put(item)

        # Remove fees which could not be set off before now.
        if self.buffer_fee:
            # Clear the buffer and remove the buffered fee from the queue.
            fee, self.buffer_fee = self.buffer_fee, decimal.Decimal()
            self.remove_fee(fee)

    @abc.abstractmethod
    def _put(self, bop: BalancedOperation) -> None:
        """Put a new item in the queue.

        Args:
            item (BalancedOperation)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def get(self) -> BalancedOperation:
        """Get an item from the queue.

        Returns:
            BalancedOperation
        """
        raise NotImplementedError

    @abc.abstractmethod
    def peek(self) -> BalancedOperation:
        """Peek at the next item in the queue.

        Returns:
            BalancedOperation
        """
        raise NotImplementedError

    def sell(
        self,
        change: decimal.Decimal,
    ) -> tuple[list[transaction.SoldCoin], decimal.Decimal]:
        """Sell/remove as many coins as possible from the queue.

        Depending on the QueueType, the coins will be removed FIFO or LIFO.

        Args:
            change (decimal.Decimal): Amount of sold coins which will be removed
                from the queue.

        Returns:
          - list[transaction.SoldCoin]: List of specific coins which were
                (depending on the tax regulation) sold in the transaction.
          - decimal.Decimal: Amount of change which could not be removed
                because the queue ran out of coins to sell.
        """
        sold_coins: list[transaction.SoldCoin] = []

        while self.queue and change > 0:
            # Look at the next coin in the queue.
            bop = self.peek()

            # Calculate the amount of coins, which are not sold yet.
            not_sold = bop.op.change - bop.sold
            assert not_sold > 0

            if not_sold > change:
                # There are more coins left than change.
                # Update the sold value,
                bop.sold += change
                # keep track of the sold amount and
                sold_coins.append(transaction.SoldCoin(bop.op, change))
                # Set the change to 0.
                change = decimal.Decimal()
                break

            else:  # change >= not_sold
                # The change is higher than or equal to the (left over) coin.
                # Update the left over change,
                change -= not_sold
                # remove the fully sold coin from the queue and
                self.get()
                # keep track of the sold amount.
                sold_coins.append(transaction.SoldCoin(bop.op, not_sold))

        assert change >= 0
        return sold_coins, change

    def remove_fee(self, fee: decimal.Decimal) -> None:
        """Remove fee from the last added transaction.

        Args:
            fee: decimal.Decimal
        """
        _, left_over_fee = self.sell(fee)
        if left_over_fee:
            # Not enough coins in queue to remove fee.
            # Buffer the fee for next time.
            self.buffer_fee += left_over_fee


class BalanceFIFOQueue(BalanceQueue):
    def _put(self, bop: BalancedOperation) -> None:
        """Put a new item in the queue.

        Args:
            item (BalancedOperation)
        """
        self.queue.append(bop)

    def get(self) -> BalancedOperation:
        """Get an item from the queue.

        Returns:
            BalancedOperation
        """
        return self.queue.popleft()

    def peek(self) -> BalancedOperation:
        """Peek at the next item in the queue.

        Returns:
            BalancedOperation
        """
        return self.queue[0]


class BalanceLIFOQueue(BalanceQueue):
    def _put(self, bop: BalancedOperation) -> None:
        """Put a new item in the queue.

        Args:
            item (BalancedOperation)
        """
        self.queue.append(bop)

    def get(self) -> BalancedOperation:
        """Get an item from the queue.

        Returns:
            BalancedOperation
        """
        return self.queue.pop()

    def peek(self) -> BalancedOperation:
        """Peek at the next item in the queue.

        Returns:
            BalancedOperation
        """
        return self.queue[-1]
