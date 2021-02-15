# CoinTaxman
# Copyright (C) 2021  Carsten Docktor

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

import logging

from bilance_queue import *
from book import Book
import config
import core
import misc
from price_data import PriceData
from transaction import *

log = logging.getLogger(__name__)


class Taxman:
    def __init__(self, book: Book, price_data: PriceData) -> None:
        self.book = book
        self.price_data = price_data

        self.tax_events: list[TaxEvent] = []
        self.bilances: dict[str, list[BilanceQueue]] = {}

        # Determine used functions/classes depending on the config.
        country = config.COUNTRY.name
        try:
            self.__evaluate_taxation = getattr(
                self, f"_evaluate_taxation_{country}")
        except AttributeError:
            raise NotImplementedError(
                f"Unable to evaluate taxation for {country=}.")

        if config.PRINCIPLE == core.Principle.FIFO:
            self.BilanceType = BilanceQueue
        elif config.PRINCIPLE == core.Principle.LIFO:
            self.BilanceType = BilanceLIFOQueue
        else:
            raise NotImplementedError(
                f"Unable to evaluate taxation for {config.PRINCIPLE=}.")

    def in_tax_year(self, op: Operation) -> bool:
        return op.utc_time.year == config.TAX_YEAR

    def _evaluate_taxation_GERMANY(self, coin: str, operations: list[Operation]) -> None:
        bilance = self.BilanceType()

        for op in operations:
            if isinstance(op, Fee):
                bilance.remove_fee(op.change)
                if self.in_tax_year(op):
                    # Fees reduce taxed gain.
                    taxation_type = "Sonstige Einkünfte"
                    taxed_gain = -self.price_data.get_cost(op)
                    tx = TaxEvent(taxation_type, taxed_gain, op)
                    self.tax_events.append(tx)
            elif isinstance(op, CoinLend):
                pass
            elif isinstance(op, CoinLendEnd):
                pass
            elif isinstance(op, Buy):
                bilance.put(op)
            elif isinstance(op, Sell):
                try:
                    sold_coins = bilance.sell(op.change)
                except IndexError:
                    if coin == config.FIAT:
                        continue
                    raise RuntimeError(
                        f"Not enough {coin} in queue to remove sold ones.")
                if self.in_tax_year(op) and coin != config.FIAT:
                    taxation_type = "Sonstige Einkünfte"
                    # Price of the sell.
                    total_win = self.price_data.get_cost(op)
                    taxed_gain = 0.0
                    # Coins which are older than (in this case) one year or
                    # which come from an Airdrop or CoinLend will not be taxed.
                    for sc in sold_coins:
                        if not config.IS_LONG_TERM(sc.op.utc_time, op.utc_time) and not isinstance(sc.op, (Airdrop, CoinLendInterest)):
                            partial_win = (sc.sold / op.change) * total_win
                            taxed_gain += partial_win - \
                                self.price_data.get_cost(sc)
                    remark = ", ".join(
                        f"{sc.sold} from {sc.op.utc_time}" for sc in sold_coins)
                    tx = TaxEvent(taxation_type, taxed_gain, op, remark)
                    self.tax_events.append(tx)
            elif isinstance(op, CoinLendInterest):
                bilance.put(op)
                if self.in_tax_year(op):
                    if misc.is_fiat(coin):
                        taxation_type = "Einkünfte aus Kapitalvermögen"
                    else:
                        taxation_type = "Einkünfte aus sonstigen Leistungen"
                    taxed_gain = self.price_data.get_cost(op)
                    tx = TaxEvent(taxation_type, taxed_gain, op)
                    self.tax_events.append(tx)
            elif isinstance(op, Airdrop):
                bilance.put(op)
            elif isinstance(op, Deposit):
                pass
            elif isinstance(op, Withdraw):
                pass
            else:
                raise NotImplementedError

        # Check that all relevant positions were considered.
        if bilance.buffer_fee:
            log.warning("Bilance has outstanding fees which were not considered: %s %s", ", ".join(
                fee for fee in bilance.buffer_fee), coin)

        self.bilances[coin] = bilance

    def evaluate_taxation(self) -> None:
        """Evaluate the taxation per coin using the country specific function."""
        for coin, operations in misc.group_by(self.book.operations, "coin").items():
            operations = sorted(operations)  # Sort by time.
            self.__evaluate_taxation(coin, operations)

    def print_evaluation(self) -> None:
        print()
        for taxation_type, tax_events in misc.group_by(self.tax_events, "taxation_type").items():
            taxed_gains = sum(tx.taxed_gain for tx in tax_events)
            print(f"{taxation_type}: {taxed_gains} {config.FIAT}")

    def export_evaluation(self) -> None:
        # TODO Print CSV with more information
        #      which could be sent to tax office if they have any questions.
        raise NotImplementedError

    def export_bilance(self) -> None:
        # TODO Print bilance at end of tax year.
        raise NotImplementedError
