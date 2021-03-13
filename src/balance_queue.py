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

import collections
import dataclasses
import logging
from typing import Deque, Union, Optional
import queue

import transaction


log = logging.getLogger(__name__)


@dataclasses.dataclass
class BalancedOperation:
    op: transaction.Operation
    sold: float = 0.0


class BalanceQueue:

    def __init__(self) -> None:
        self.queue: Deque[BalancedOperation] = collections.deque()
        self.buffer_fee: list[float] = []

    def put(self, item: Union[transaction.Operation, BalancedOperation]) -> None:
        """Put a new item in the queue.

        Args:
            item (Union[Operation, BalancedOperation])
        """
        if not isinstance(item, (transaction.Operation, BalancedOperation)):
            raise ValueError
        if isinstance(item, transaction.Operation):
            item = BalancedOperation(item)

        self._put(item)

        # Fees which could not be removed from the queue because it was empty
        # before.
        buffer_fee = self.buffer_fee.copy()
        self.buffer_fee = []
        for fee in buffer_fee:
            self.remove_fee(fee)

    def get(self) -> Optional[BalancedOperation]:
        """Get an item from the queue.

        Returns:
            BalancedOperation
            ...or None, if the queue ran out of items to sell.
        """
        return self._get()

    def _put(self, bop: BalancedOperation) -> None:
        self.queue.append(bop)

    def _get(self) -> Optional[BalancedOperation]:
        try:
            return self.queue.popleft()
        except IndexError:
            return None

    def remove_fee(self, fee: float) -> None:
        """Remove fee from the last added transaction.

        Args:
            fee: float
        """
        while True:
            try:
                bop: BalancedOperation = self.queue.pop()
            except IndexError:
                # Not enough coins in queue to remove fee.
                # This can happen if the exchange takes the fees before
                # the buy/sell process.
                # Buffer the fees for the next put action.
                self.buffer_fee.append(fee)
                break

            not_sold = bop.op.change - bop.sold
            assert not_sold > 0

            if not_sold >= fee:
                bop.sold += fee
                self.queue.append(bop)
                break
            else:
                fee -= not_sold

    def sell(self, change: float) -> Optional[list[transaction.SoldCoin]]:
        """Sell/remove coins from the queue, returning the sold coins.

        Depending on the QueueType, the coins will be removed FIFO or LIFO.

        Args:
            change (float): Amount of sold coins which will be removed
                            from the queue.

        Returns:
            list[SoldCoin]: List of specific coins which were (depending on
                            the tax regulation) sold in the transaction.
            ...or None, if the queue ran out of items to sell.
        """
        assert change > 0
        sold_coins: list[transaction.SoldCoin] = []
        while change > 0:
            bop: Optional[BalancedOperation] = self.get()

            if bop is None:
                return None

            not_sold = bop.op.change - bop.sold
            assert not_sold > 0

            if not_sold > change:
                bop.sold += change
                self.queue.append(bop)
                sold_coins.append(transaction.SoldCoin(bop.op, change))
                break
            else:
                change -= not_sold
                sold_coins.append(transaction.SoldCoin(bop.op, not_sold))

        return sold_coins


class BalanceLIFOQueue(queue.LifoQueue, BalanceQueue):

    def _put(self, item: BalancedOperation) -> None:
        self.queue.append(item)

    def _get(self) -> BalancedOperation:
        item: BalancedOperation = self.queue.pop()
        return item
