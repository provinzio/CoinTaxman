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

import csv
from pathlib import Path
import logging
import re

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
        self.bilances: dict[str, BilanceQueue] = {}

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
                sold_coins = bilance.sell(op.change)
                if sold_coins is None:
                    # Queue ran out of items to sell...
                    if coin == config.FIAT:
                        # ...this is OK for fiat currencies (not taxable)
                        continue
                    else:
                        # ...but not for crypto coins (taxable)
                        log.error(
                            f"{op.file_path.name}: Line {op.line}: "
                            f"Not enough {coin} in queue to sell (transaction from {op.utc_time} on {op.platform})\n"
                            f"\tIs your account statement missing any transactions?\n"
                            f"\tThis error may also occur after deposits from unknown sources.\n"
                        )
                        raise RuntimeError
                if self.in_tax_year(op) and coin != config.FIAT:
                    taxation_type = "Sonstige Einkünfte"
                    # Price of the sell.
                    total_win = self.price_data.get_cost(op)
                    taxed_gain = 0.0
                    # Coins which are older than (in this case) one year or
                    # which come from an Airdrop, CoinLend or Commission (in an
                    # foreign currency) will not be taxed.
                    for sc in sold_coins:
                        if not config.IS_LONG_TERM(sc.op.utc_time, op.utc_time) and not (isinstance(sc.op, (Airdrop, CoinLendInterest, Commission)) and not sc.op.coin == config.FIAT):
                            partial_win = (sc.sold / op.change) * total_win
                            taxed_gain += partial_win - \
                                self.price_data.get_cost(sc)
                    remark = ", ".join(
                        f"{sc.sold} from {sc.op.utc_time} ({sc.op.__class__.__name__})" for sc in sold_coins)
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
            elif isinstance(op, Commission):
                bilance.put(op)
                if self.in_tax_year(op):
                    taxation_type = "Einkünfte aus sonstigen Leistungen"
                    taxed_gain = self.price_data.get_cost(op)
                    tx = TaxEvent(taxation_type, taxed_gain, op)
                    self.tax_events.append(tx)
            elif isinstance(op, Deposit):
                pass
            elif isinstance(op, Withdraw):
                pass
            else:
                raise NotImplementedError

        # Check that all relevant positions were considered.
        if bilance.buffer_fee:
            log.warning("Bilance has outstanding fees which were not considered: %s %s", ", ".join(
                str(fee) for fee in bilance.buffer_fee), coin)

        self.bilances[coin] = bilance

    def evaluate_taxation(self) -> None:
        """Evaluate the taxation per coin using the country specific function."""
        log.debug("Starting evaluation...")
        for coin, operations in misc.group_by(self.book.operations, "coin").items():
            operations = sorted(operations, key=lambda op: op.utc_time)
            self.__evaluate_taxation(coin, operations)

    def print_evaluation(self) -> None:
        """Print short summary of evaluation to stdout."""
        if self.tax_events:
            print()
            print(f"Your tax evaluation for {config.TAX_YEAR}:")
            for taxation_type, tax_events in misc.group_by(self.tax_events, "taxation_type").items():
                taxed_gains = sum(tx.taxed_gain for tx in tax_events)
                print(f"{taxation_type}: {taxed_gains} {config.FIAT}")
        else:
            print(
                f"Either the evaluation has not run or there are no tax events for {config.TAX_YEAR}.")

    def export_evaluation(self) -> Path:
        """Export detailed summary of all tax events to CSV.

        File will be placed in export/ with ascending revision numbers
        (in case multiple evaluations will be done).

        When no tax events occured, the CSV will be exported only with
        a header line.

        Returns:
            Path: Path to the exported file.
        """
        file_path = misc.get_next_file_path(
            config.EXPORT_PATH, str(config.TAX_YEAR), "csv")

        with open(file_path, "w", newline="") as f:
            writer = csv.writer(f)
            header = ["Date", "Taxation Type", f"Taxed Gain in {config.FIAT}",
                      "Action", "Amount", "Asset",  "Remark"]
            writer.writerow(header)
            # Tax events are currently sorted by coin. Sort by time instead.
            for tx in sorted(self.tax_events, key=lambda tx: tx.op.utc_time):
                line = [tx.op.utc_time, tx.taxation_type, tx.taxed_gain,
                        tx.op.__class__.__name__,  tx.op.change, tx.op.coin, tx.remark]
                writer.writerow(line)

        log.info("Saved evaluation in %s.", file_path)
        return file_path

    def export_bilance(self) -> None:
        # TODO Print bilance at end of tax year.
        raise NotImplementedError
