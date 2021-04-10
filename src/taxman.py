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
import datetime
import decimal
import logging
from pathlib import Path
from typing import Type

import balance_queue
import config
import core
import misc
import transaction
from book import Book
from price_data import PriceData

log = logging.getLogger(__name__)


class Taxman:
    def __init__(self, book: Book, price_data: PriceData) -> None:
        self.book = book
        self.price_data = price_data

        self.tax_events: list[transaction.TaxEvent] = []
        self.balances: dict[str, balance_queue.BalanceQueue] = {}

        # Determine used functions/classes depending on the config.
        country = config.COUNTRY.name
        try:
            self.__evaluate_taxation = getattr(self, f"_evaluate_taxation_{country}")
        except AttributeError:
            raise NotImplementedError(f"Unable to evaluate taxation for {country=}.")

        if config.PRINCIPLE == core.Principle.FIFO:
            # Explicity define type for BalanceType on first declaration
            # to avoid mypy errors.
            self.BalanceType: Type[
                balance_queue.BalanceQueue
            ] = balance_queue.BalanceFIFOQueue
        elif config.PRINCIPLE == core.Principle.LIFO:
            self.BalanceType = balance_queue.BalanceLIFOQueue
        else:
            raise NotImplementedError(
                f"Unable to evaluate taxation for {config.PRINCIPLE=}."
            )

    def in_tax_year(self, op: transaction.Operation) -> bool:
        return op.utc_time.year == config.TAX_YEAR

    def _evaluate_taxation_GERMANY(
        self,
        coin: str,
        operations: list[transaction.Operation],
    ) -> None:
        balance = self.BalanceType()

        for op in operations:
            if isinstance(op, transaction.Fee):
                balance.remove_fee(op.change)
                if self.in_tax_year(op):
                    # Fees reduce taxed gain.
                    taxation_type = "Sonstige Einkünfte"
                    taxed_gain = -self.price_data.get_cost(op)
                    tx = transaction.TaxEvent(taxation_type, taxed_gain, op)
                    self.tax_events.append(tx)
            elif isinstance(op, transaction.CoinLend):
                pass
            elif isinstance(op, transaction.CoinLendEnd):
                pass
            elif isinstance(op, transaction.Buy):
                balance.put(op)
            elif isinstance(op, transaction.Sell):
                sold_coins, unsold_coins = balance.sell(op.change)
                if unsold_coins:
                    # Queue ran out of items to sell and not all coins
                    # could be sold.
                    if coin == config.FIAT:
                        # This is OK for the own fiat currencies (not taxable).
                        continue
                    else:
                        log.error(
                            f"{op.file_path.name}: Line {op.line}: "
                            f"Not enough {coin} in queue to sell "
                            f"(transaction from {op.utc_time} "
                            f"on {op.platform})\n"
                            "\tThis error occurs if your account statements "
                            "have unmatched buy/sell positions.\n"
                            "\tHave you added all your account statements "
                            "of the last years?\n"
                            "\tThis error may also occur after deposits "
                            "from unknown sources.\n"
                        )
                        raise RuntimeError
                if self.in_tax_year(op) and coin != config.FIAT:
                    taxation_type = "Sonstige Einkünfte"
                    # Price of the sell.
                    total_win = self.price_data.get_cost(op)
                    taxed_gain = decimal.Decimal()
                    # Coins which are older than (in this case) one year or
                    # which come from an Airdrop, CoinLend or Commission (in an
                    # foreign currency) will not be taxed.
                    for sc in sold_coins:
                        if not config.IS_LONG_TERM(
                            sc.op.utc_time, op.utc_time
                        ) and not (
                            isinstance(
                                sc.op,
                                (
                                    transaction.Airdrop,
                                    transaction.CoinLendInterest,
                                    transaction.StakingInterest,
                                    transaction.Commission,
                                ),
                            )
                            and not sc.op.coin == config.FIAT
                        ):
                            partial_win = (sc.sold / op.change) * total_win
                            taxed_gain += partial_win - self.price_data.get_cost(sc)
                    remark = ", ".join(
                        f"{sc.sold} from {sc.op.utc_time} "
                        f"({sc.op.__class__.__name__})"
                        for sc in sold_coins
                    )
                    tx = transaction.TaxEvent(taxation_type, taxed_gain, op, remark)
                    self.tax_events.append(tx)
            elif isinstance(
                op, (transaction.CoinLendInterest, transaction.StakingInterest)
            ):
                balance.put(op)
                if self.in_tax_year(op):
                    if misc.is_fiat(coin):
                        assert not isinstance(
                            op, transaction.StakingInterest
                        ), "You can not stake fiat currencies."
                        taxation_type = "Einkünfte aus Kapitalvermögen"
                    else:
                        taxation_type = "Einkünfte aus sonstigen Leistungen"
                    taxed_gain = self.price_data.get_cost(op)
                    tx = transaction.TaxEvent(taxation_type, taxed_gain, op)
                    self.tax_events.append(tx)
            elif isinstance(op, transaction.Airdrop):
                balance.put(op)
            elif isinstance(op, transaction.Commission):
                balance.put(op)
                if self.in_tax_year(op):
                    taxation_type = "Einkünfte aus sonstigen Leistungen"
                    taxed_gain = self.price_data.get_cost(op)
                    tx = transaction.TaxEvent(taxation_type, taxed_gain, op)
                    self.tax_events.append(tx)
            elif isinstance(op, transaction.Deposit):
                pass
            elif isinstance(op, transaction.Withdraw):
                pass
            else:
                raise NotImplementedError

        # Check that all relevant positions were considered.
        if balance.buffer_fee:
            log.warning(
                "Balance has outstanding fees which were not considered: "
                f"{balance.buffer_fee} {coin}"
            )

        self.balances[coin] = balance

    def evaluate_taxation(self) -> None:
        """Evaluate the taxation per coin using country specific function."""
        log.debug("Starting evaluation...")
        for coin, operations in misc.group_by(self.book.operations, "coin").items():
            operations = sorted(operations, key=lambda op: op.utc_time)
            self.__evaluate_taxation(coin, operations)

    def print_evaluation(self) -> None:
        """Print short summary of evaluation to stdout."""
        if self.tax_events:
            print()
            print(f"Your tax evaluation for {config.TAX_YEAR}:")
            for taxation_type, tax_events in misc.group_by(
                self.tax_events, "taxation_type"
            ).items():
                taxed_gains = sum(tx.taxed_gain for tx in tax_events)
                print(f"{taxation_type}: {taxed_gains} {config.FIAT}")
        else:
            print(
                "Either the evaluation has not run or there are no tax events "
                f"for {config.TAX_YEAR}."
            )

    def export_evaluation_as_csv(self) -> Path:
        """Export detailed summary of all tax events to CSV.

        File will be placed in export/ with ascending revision numbers
        (in case multiple evaluations will be done).

        When no tax events occured, the CSV will be exported only with
        a header line.

        Returns:
            Path: Path to the exported file.
        """
        file_path = misc.get_next_file_path(
            config.EXPORT_PATH, str(config.TAX_YEAR), "csv"
        )

        with open(file_path, "w", newline="", encoding="utf8") as f:
            writer = csv.writer(f)
            # Add embedded metadata info
            writer.writerow(
                ["# software", "CoinTaxman <https://github.com/provinzio/CoinTaxman>"]
            )
            if commit_hash := misc.get_current_commit_hash():
                writer.writerow(["# commit", commit_hash])
            writer.writerow(["# updated", datetime.date.today().strftime("%x")])

            header = [
                "Date",
                "Taxation Type",
                f"Taxed Gain in {config.FIAT}",
                "Action",
                "Amount",
                "Asset",
                "Remark",
            ]
            writer.writerow(header)
            # Tax events are currently sorted by coin. Sort by time instead.
            for tx in sorted(self.tax_events, key=lambda tx: tx.op.utc_time):
                line = [
                    tx.op.utc_time,
                    tx.taxation_type,
                    tx.taxed_gain,
                    tx.op.__class__.__name__,
                    tx.op.change,
                    tx.op.coin,
                    tx.remark,
                ]
                writer.writerow(line)

        log.info("Saved evaluation in %s.", file_path)
        return file_path
