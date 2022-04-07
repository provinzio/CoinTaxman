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
from pathlib import Path
from typing import Optional, Type

import balance_queue
import config
import core
import log_config
import misc
import transaction
from book import Book
from price_data import PriceData

log = log_config.getLogger(__name__)


class Taxman:
    def __init__(self, book: Book, price_data: PriceData) -> None:
        self.book = book
        self.price_data = price_data

        self.tax_events: list[transaction.TaxEvent] = []
        # Tax Events which would occur if all left over coins were sold now.
        self.virtual_tax_events: list[transaction.TaxEvent] = []

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

        def evaluate_sell(
            op: transaction.Operation, force: bool = False
        ) -> Optional[transaction.TaxEvent]:
            # Remove coins from queue.
            sold_coins, unsold_coins = balance.sell(op.change)

            if coin == config.FIAT:
                # Not taxable.
                return None

            if unsold_coins:
                # Queue ran out of items to sell and not all coins
                # could be sold.
                log.error(
                    f"{op.file_path.name}: Line {op.line}: "
                    f"Not enough {coin} in queue to sell: "
                    f"missing {unsold_coins} {coin} "
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

            if not self.in_tax_year(op) and not force:
                # Sell is only taxable in the respective year.
                return None

            taxation_type = "Sonstige Einkünfte"
            # Price of the sell.
            sell_price = self.price_data.get_cost(op)
            taxed_gain = decimal.Decimal()
            real_gain = decimal.Decimal()
            # Coins which are older than (in this case) one year or
            # which come from an Airdrop, CoinLend or Commission (in an
            # foreign currency) will not be taxed.
            for sc in sold_coins:
                is_taxable = not config.IS_LONG_TERM(
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
                )
                # Only calculate the gains if necessary.
                if is_taxable or config.CALCULATE_UNREALIZED_GAINS:
                    partial_sell_price = (sc.sold / op.change) * sell_price
                    sold_coin_cost = self.price_data.get_cost(sc)
                    gain = partial_sell_price - sold_coin_cost
                    if is_taxable:
                        taxed_gain += gain
                    if config.CALCULATE_UNREALIZED_GAINS:
                        real_gain += gain
            remark = ", ".join(
                f"{sc.sold} from {sc.op.utc_time} " f"({sc.op.__class__.__name__})"
                for sc in sold_coins
            )
            return transaction.TaxEvent(
                taxation_type,
                taxed_gain,
                op,
                sell_price,
                real_gain,
                remark,
            )

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
            elif isinstance(op, transaction.Staking):
                pass
            elif isinstance(op, transaction.StakingEnd):
                pass
            elif isinstance(op, transaction.Buy):
                balance.put(op)
            elif isinstance(op, transaction.Sell):
                if tx_ := evaluate_sell(op):
                    self.tax_events.append(tx_)
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
                if coin != config.FIAT:
                    log.warning(
                        f"Unresolved deposit of {op.change} {coin} "
                        f"on {op.platform} at {op.utc_time}. "
                        "The evaluation might be wrong."
                    )
            elif isinstance(op, transaction.Withdrawal):
                if coin != config.FIAT:
                    log.warning(
                        f"Unresolved withdrawal of {op.change} {coin} "
                        f"from {op.platform} at {op.utc_time}. "
                        "The evaluation might be wrong."
                    )
            else:
                raise NotImplementedError

        # Check that all relevant positions were considered.
        if balance.buffer_fee:
            log.warning(
                "Balance has outstanding fees which were not considered: "
                f"{balance.buffer_fee} {coin}"
            )

        # Calculate the amount of coins which should be left on the platform
        # and evaluate the (taxed) gain, if the coin would be sold right now.
        if config.CALCULATE_UNREALIZED_GAINS and (
            (left_coin := sum(((bop.op.change - bop.sold) for bop in balance.queue)))
            and self.price_data.get_cost(op)
        ):
            assert isinstance(left_coin, decimal.Decimal)
            virtual_sell = transaction.Sell(
                datetime.datetime.now().astimezone(),
                op.platform,
                left_coin,
                coin,
                -1,
                Path(""),
            )
            if tx_ := evaluate_sell(virtual_sell, force=True):
                self.virtual_tax_events.append(tx_)

    def _evaluate_taxation_per_coin(
        self,
        operations: list[transaction.Operation],
    ) -> None:
        """Evaluate the taxation for a list of operations per coin using
        country specific functions.

        Args:
            operations (list[transaction.Operation])
        """
        for coin, coin_operations in misc.group_by(operations, "coin").items():
            coin_operations = transaction.sort_operations(coin_operations, ["utc_time"])
            self.__evaluate_taxation(coin, coin_operations)

    def evaluate_taxation(self) -> None:
        """Evaluate the taxation using country specific function."""
        log.debug("Starting evaluation...")

        if config.MULTI_DEPOT:
            # Evaluate taxation separated by platforms and coins.
            for _, operations in misc.group_by(
                self.book.operations, "platform"
            ).items():
                self._evaluate_taxation_per_coin(operations)
        else:
            # Evaluate taxation separated by coins in a single virtual depot.
            self._evaluate_taxation_per_coin(self.book.operations)

    def print_evaluation(self) -> None:
        """Print short summary of evaluation to stdout."""
        eval_str = "Evaluation:\n\n"

        # Summarize the tax evaluation.
        if self.tax_events:
            eval_str += f"Your tax evaluation for {config.TAX_YEAR}:\n"
            for taxation_type, tax_events in misc.group_by(
                self.tax_events, "taxation_type"
            ).items():
                taxed_gains = sum(tx.taxed_gain for tx in tax_events)
                eval_str += f"{taxation_type}: {taxed_gains:.2f} {config.FIAT}\n"
        else:
            eval_str += (
                "Either the evaluation has not run or there are no tax events "
                f"for {config.TAX_YEAR}.\n"
            )

        # Summarize the virtual sell, if all left over coins would be sold right now.
        if self.virtual_tax_events:
            assert config.CALCULATE_UNREALIZED_GAINS
            invsted = sum(tx.sell_price for tx in self.virtual_tax_events)
            real_gains = sum(tx.real_gain for tx in self.virtual_tax_events)
            taxed_gains = sum(tx.taxed_gain for tx in self.virtual_tax_events)
            eval_str += "\n"
            eval_str += (
                f"You are currently invested with {invsted:.2f} {config.FIAT}.\n"
                f"If you would sell everything right now, "
                f"you would realize {real_gains:.2f} {config.FIAT} gains "
                f"({taxed_gains:.2f} {config.FIAT} taxed gain).\n"
            )

            eval_str += "\n"
            eval_str += "Your current portfolio should be:\n"
            for tx in sorted(
                self.virtual_tax_events,
                key=lambda tx: tx.sell_price,
                reverse=True,
            ):
                eval_str += (
                    f"{tx.op.platform}: "
                    f"{tx.op.change:.6f} {tx.op.coin} > "
                    f"{tx.sell_price:.2f} {config.FIAT} "
                    f"({tx.real_gain:.2f} gain, {tx.taxed_gain:.2f} taxed gain)\n"
                )

        log.info(eval_str)

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
            commit_hash = misc.get_current_commit_hash(default="undetermined")
            writer.writerow(["# commit", commit_hash])
            writer.writerow(["# updated", datetime.date.today().strftime("%x")])

            header = [
                "Date",
                "Taxation Type",
                f"Taxed Gain in {config.FIAT}",
                "Action",
                "Amount",
                "Asset",
                f"Sell Price in {config.FIAT}",
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
                    tx.sell_price,
                    tx.remark,
                ]
                writer.writerow(line)

        log.info("Saved evaluation in %s.", file_path)
        return file_path
