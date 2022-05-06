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

import config
import log_config
import transaction as tr

log = log_config.getLogger(__name__)


@dataclasses.dataclass
class BalancedOperation:
    op: tr.Operation
    sold: decimal.Decimal = decimal.Decimal()

    @property
    def not_sold(self) -> decimal.Decimal:
        """Calculate the amount of coins which are not sold yet.

        Returns:
            decimal.Decimal: Amount of coins which are not sold yet.
        """
        not_sold = self.op.change - self.sold
        # If the left over amount is <= 0, this coin shouldn't be in the queue.
        assert not_sold > 0, f"{not_sold=} should be > 0"
        return not_sold


class BalanceQueue(abc.ABC):
    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.queue: collections.deque[BalancedOperation] = collections.deque()
        # It might happen, that the exchange takes fees before the buy/sell-
        # transaction. Keep fees, which couldn't be removed directly from the
        # queue and remove them as soon as possible.
        # At the end, all fees should have been paid (removed from the buffer).
        self.buffer_fee = decimal.Decimal()

    @abc.abstractmethod
    def _put_(self, bop: BalancedOperation) -> None:
        """Put a new item in the queue.

        Args:
            item (BalancedOperation)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _pop_(self) -> BalancedOperation:
        """Pop an item from the queue.

        Returns:
            BalancedOperation
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _peek_(self) -> BalancedOperation:
        """Peek at the next item in the queue.

        Returns:
            BalancedOperation
        """
        raise NotImplementedError

    def _put(self, item: Union[tr.Operation, BalancedOperation]) -> None:
        """Put a new item in the queue and remove buffered fees.

        Args:
            item (Union[Operation, BalancedOperation])
        """
        if isinstance(item, tr.Operation):
            item = BalancedOperation(item)
        elif not isinstance(item, BalancedOperation):
            raise TypeError

        self._put_(item)

        # Remove fees which couldn't be removed before.
        if self.buffer_fee:
            # Clear the buffer.
            fee, self.buffer_fee = self.buffer_fee, decimal.Decimal()
            # Try to remove the fees.
            self._remove_fee(fee)

    def _pop(self) -> BalancedOperation:
        """Pop an item from the queue.

        Returns:
            BalancedOperation
        """
        return self._pop_()

    def _peek(self) -> BalancedOperation:
        """Peek at the next item in the queue.

        Returns:
            BalancedOperation
        """
        return self._peek_()

    def add(self, op: tr.Operation) -> None:
        """Add an operation with coins to the balance.

        Args:
            op (tr.Operation)
        """
        assert not isinstance(op, tr.Fee)
        assert op.coin == self.coin
        self._put(op)

    def _remove(
        self,
        change: decimal.Decimal,
    ) -> tuple[list[tr.SoldCoin], decimal.Decimal]:
        """Remove as many coins as necessary from the queue.

        The removement logic is defined by the BalanceQueue child class.

        Args:
            change (decimal.Decimal): Amount of coins to be removed.

        Returns:
          - list[tr.SoldCoin]: List of coins which were removed.
          - decimal.Decimal: Amount of change which could not be removed
                because the queue ran out of coins.
        """
        sold_coins: list[tr.SoldCoin] = []

        while self.queue and change > 0:
            # Look at the next coin in the queue.
            bop = self._peek()

            # Get the amount of not sold coins.
            not_sold = bop.not_sold

            if not_sold > change:
                # There are more coins left than change.
                # Update the sold value,
                bop.sold += change
                # keep track of the sold amount
                sold_coins.append(tr.SoldCoin(bop.op, change))
                # and set the change to 0.
                change = decimal.Decimal()
                # All demanded change was removed.
                break

            else:  # not_sold <= change
                # The change is higher than or equal to the left over coins.
                # Update the left over change,
                change -= not_sold
                # remove the fully sold coin from the queue
                self._pop()
                # and keep track of the sold amount.
                sold_coins.append(tr.SoldCoin(bop.op, not_sold))

        assert change >= 0, "Removed more than necessary from the queue."
        return sold_coins, change

    def remove(
        self,
        op: tr.Operation,
    ) -> list[tr.SoldCoin]:
        """Remove as many coins as necessary from the queue.

        The removement logic is defined by the BalanceQueue child class.

        Args:
            op (tr.Operation): Operation with coins to be removed.

        Raises:
            RuntimeError: When there are not enough coins in queue to be sold.

        Returns:
          - list[tr.SoldCoin]: List of coins which were removed.
        """
        assert op.coin == self.coin
        sold_coins, unsold_change = self._remove(op.change)

        if unsold_change:
            # Queue ran out of items to sell and not all coins could be sold.
            msg = (
                f"Not enough {op.coin} in queue to sell: "
                f"missing {unsold_change} {op.coin} "
                f"(transaction from {op.utc_time} on {op.platform}, "
                f"see {op.file_path.name} lines {op.line})\n"
                f"This can happen when you sold more {op.coin} than you have "
                "according to your account statements. Have you added every "
                "account statement including these from last years and the "
                f"all deposits of {op.coin}?"
            )
            if self.coin == config.FIAT:
                log.warning(
                    f"{msg}\n"
                    "Tracking of your home fiat is not important for tax "
                    f"evaluation but the {op.coin} in your portfolio at "
                    "deadline will be wrong."
                )
            else:
                log.error(
                    f"{msg}\n"
                    "\tThis error may also occur after deposits from unknown "
                    "sources. CoinTaxman requires the full transaction history to "
                    "evaluate taxation (when and where were these deposited coins "
                    "bought?).\n"
                )
                raise RuntimeError

        return sold_coins

    def _remove_fee(self, fee: decimal.Decimal) -> None:
        """Remove fee from the last added transaction.

        Args:
            fee: decimal.Decimal
        """
        _, left_over_fee = self._remove(fee)
        if left_over_fee:
            log.warning(
                "Not enough coins in queue to remove fee. Buffer the fee for "
                "next adding time... "
                "This should not happen. You might be missing an account "
                "statement. Please open issue or PR if you need help."
            )
            self.buffer_fee += left_over_fee

    def remove_fee(self, fee: tr.Fee) -> None:
        assert fee.coin == self.coin
        self._remove_fee(fee.change)

    def sanity_check(self) -> None:
        """Validate that all fees were paid or raise an exception.

        At the end, all fees should have been paid.

        Raises:
            RuntimeError: Not all fees were paid.
        """
        if self.buffer_fee:
            log.error(
                f"Not enough {self.coin} in queue to pay left over fees: "
                f"missing {self.buffer_fee} {self.coin}.\n"
                "\tThis error occurs when you sold more coins than you have "
                "according to your account statements. Have you added every "
                "account statement, including these from the last years?\n"
                "\tThis error may also occur after deposits from unknown "
                "sources. CoinTaxman requires the full transaction history to "
                "evaluate taxation (when where these deposited coins bought?).\n"
            )
            raise RuntimeError

    def remove_all(self) -> list[tr.SoldCoin]:
        sold_coins = []
        while self.queue:
            bop = self._pop()
            not_sold = bop.not_sold
            sold_coins.append(tr.SoldCoin(bop.op, not_sold))
        return sold_coins


class BalanceFIFOQueue(BalanceQueue):
    def _put_(self, bop: BalancedOperation) -> None:
        """Put a new item in the queue.

        Args:
            item (BalancedOperation)
        """
        self.queue.append(bop)

    def _pop_(self) -> BalancedOperation:
        """Pop an item from the queue.

        Returns:
            BalancedOperation
        """
        return self.queue.popleft()

    def _peek_(self) -> BalancedOperation:
        """Peek at the next item in the queue.

        Returns:
            BalancedOperation
        """
        return self.queue[0]


class BalanceLIFOQueue(BalanceQueue):
    def _put_(self, bop: BalancedOperation) -> None:
        """Put a new item in the queue.

        Args:
            item (BalancedOperation)
        """
        self.queue.append(bop)

    def _pop_(self) -> BalancedOperation:
        """Pop an item from the queue.

        Returns:
            BalancedOperation
        """
        return self.queue.pop()

    def _peek_(self) -> BalancedOperation:
        """Peek at the next item in the queue.

        Returns:
            BalancedOperation
        """
        return self.queue[-1]
