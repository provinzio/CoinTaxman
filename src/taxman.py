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
from typing import Any, Optional, Type

import balance_queue
import config
import core
import log_config
import misc
import transaction as tr
from book import Book
from price_data import PriceData

log = log_config.getLogger(__name__)

TAX_DEADLINE = min(
    datetime.datetime.now(),  # now
    datetime.datetime(config.TAX_YEAR, 12, 31, 23, 59, 59),  # end of year
)


def in_tax_year(op: tr.Operation) -> bool:
    return op.utc_time.year == config.TAX_YEAR


class Taxman:
    def __init__(self, book: Book, price_data: PriceData) -> None:
        self.book = book
        self.price_data = price_data

        # TODO@now REFACTOR how TaxEvents are kept.
        self.tax_events: list[tr.TaxEvent] = []
        # Tax Events which would occur if all left over coins were sold now.
        self.virtual_tax_events: list[tr.TaxEvent] = []

        # Determine used functions/classes depending on the config.
        country = config.COUNTRY.name
        try:
            self.__evaluate_taxation = getattr(self, f"_evaluate_taxation_{country}")
        except AttributeError:
            raise NotImplementedError(f"Unable to evaluate taxation for {country=}.")

        # Determine the BalanceType.
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

        self._balances: dict[Any, balance_queue.BalanceQueue] = {}

    ###########################################################################
    # Helper functions for balances.
    # TODO Refactor this into separated BalanceDict class?
    ###########################################################################

    def balance(self, platform: str, coin: str) -> balance_queue.BalanceQueue:
        key = (platform, coin) if config.MULTI_DEPOT else coin
        try:
            return self._balances[key]
        except KeyError:
            self._balances[key] = self.BalanceType(coin)
            return self._balances[key]

    def balance_op(self, op: tr.Operation) -> balance_queue.BalanceQueue:
        balance = self.balance(op.platform, op.coin)
        return balance

    def add_to_balance(self, op: tr.Operation) -> None:
        self.balance_op(op).add(op)

    def remove_from_balance(self, op: tr.Operation) -> list[tr.SoldCoin]:
        return self.balance_op(op).remove(op)

    def remove_fees_from_balance(self, fees: Optional[list[tr.Fee]]) -> None:
        if fees is not None:
            for fee in fees:
                self.balance_op(fee).remove_fee(fee)

    ###########################################################################
    # Country specific evaluation functions.
    ###########################################################################

    def evaluate_sell(self, op: tr.Sell, sold_coins: list[tr.SoldCoin]) -> tr.TaxEvent:
        assert op.coin != config.FIAT
        assert in_tax_year(op)

        # TODO REFACTOR Berechnung
        # TODO Beachte Deposit als Quelle. Fehler, wenn Quelle fehlt
        # TODO Werfe Fehler, falls bestimmte operation nicht beachtet wird.
        # Veräußerungserlös
        # Anschaffungskosten
        # TODO Beachte buying fees zu anschaffungskosten
        # Werbungskosten
        # TODO Beachte fees
        # Gewinn / Verlust
        # davon steuerbar

        taxation_type = "Sonstige Einkünfte"
        # Price of the sell.
        sell_value = self.price_data.get_cost(op)
        taxed_gain = decimal.Decimal()
        real_gain = decimal.Decimal()
        # Coins which are older than (in this case) one year or
        # which come from an Airdrop, CoinLend or Commission (in an
        # foreign currency) will not be taxed.
        for sc in sold_coins:
            if isinstance(sc.op, tr.Deposit):
                # If these coins get sold, we need to now when and for which price
                # they were bought.
                # TODO Implement matching for Deposit and Withdrawals to determine
                # the correct acquisition cost and to determine whether this stell
                # is tax relevant.
                log.warning(
                    f"You sold {sc.op.coin} which were deposited from "
                    f"somewhere else onto {sc.op.platform} (see "
                    f"{sc.op.file_path} {sc.op.line}). "
                    "Matching of Deposits and Withdrawals is currently not "
                    "implementeded. Therefore it is unknown when and for which "
                    f"price these {sc.op.coin} were bought. "
                    "A correct tax evaluation is not possible. "
                    "Please create an issue or PR to help solve this problem. "
                    "For now, we assume that the coins were bought at deposit, "
                    "The price is gathered from the platform onto which the coin "
                    f"was deposited ({sc.op.platform})."
                )

            is_taxable = not config.IS_LONG_TERM(sc.op.utc_time, op.utc_time) and not (
                isinstance(
                    sc.op,
                    (
                        tr.Airdrop,
                        tr.CoinLendInterest,
                        tr.StakingInterest,
                        tr.Commission,
                    ),
                )
                and not sc.op.coin == config.FIAT
            )
            # Only calculate the gains if necessary.
            if is_taxable or config.CALCULATE_UNREALIZED_GAINS:
                partial_sell_value = (sc.sold / op.change) * sell_value
                sold_coin_cost = self.price_data.get_cost(sc)
                gain = partial_sell_value - sold_coin_cost
                if is_taxable:
                    taxed_gain += gain
                if config.CALCULATE_UNREALIZED_GAINS:
                    real_gain += gain
        remark = ", ".join(
            f"{sc.sold} from {sc.op.utc_time} " f"({sc.op.__class__.__name__})"
            for sc in sold_coins
        )
        return tr.TaxEvent(
            taxation_type,
            taxed_gain,
            op,
            sell_value,
            real_gain,
            remark,
        )

    def _evaluate_taxation_GERMANY(self, op: tr.Operation) -> None:

        if isinstance(op, (tr.CoinLend, tr.Staking)):
            # TODO determine which coins get lended/etc., use fifo if it's
            # unclear. it might be worth to optimize the order
            # of coins given away (is this legal?)
            # TODO mark them as currently lended/etc., so they don't get sold
            pass

        elif isinstance(op, (tr.CoinLendEnd, tr.StakingEnd)):
            # TODO determine which coins come back from lending/etc. use fifo
            # if it's unclear; it might be nice to match Start and
            # End of these operations like deposit and withdrawal operations.
            # e.g.
            # - lending 1 coin for 2 months
            # - lending 2 coins for 1 month
            # - getting back 2 coins from lending
            # --> this should be the second and third coin,
            #     not the first and second
            # TODO mark them as not lended/etc. anymore, so they could be sold
            # again
            # TODO lending/etc might increase the tax-free speculation period!
            pass

        elif isinstance(op, tr.Buy):
            # Buys and sells always come in a pair. The buying/receiving
            # part is not tax relevant per se.
            # The fees of this buy/sell-transaction are saved internally in
            # both operations. The "buying fees" are only relevant when
            # detemining the acquisition cost of the bought coins.
            # For now we'll just add our bought coins to the balance.
            self.add_to_balance(op)

        elif isinstance(op, tr.Sell):
            # Buys and sells always come in a pair. The selling/redeeming
            # time is tax relevant.
            # Remove the sold coins and paid fees from the balance.
            sold_coins = self.remove_from_balance(op)
            self.remove_fees_from_balance(op.fees)

            if op.coin != config.FIAT and in_tax_year(op):
                tx = self.evaluate_sell(op, sold_coins)
                self.tax_events.append(tx)

        elif isinstance(op, (tr.CoinLendInterest, tr.StakingInterest)):
            # TODO@now
            self.add_to_balance(op)

            # TODO@now REFACTOR
            if in_tax_year(op):
                if misc.is_fiat(op.coin):
                    assert not isinstance(
                        op, tr.StakingInterest
                    ), "You can not stake fiat currencies."
                    taxation_type = "Einkünfte aus Kapitalvermögen"
                else:
                    taxation_type = "Einkünfte aus sonstigen Leistungen"

                taxed_gain = self.price_data.get_cost(op)
                tx = tr.TaxEvent(taxation_type, taxed_gain, op)
                self.tax_events.append(tx)

        elif isinstance(op, tr.Airdrop):
            # TODO write information text
            self.add_to_balance(op)

            if in_tax_year(op):
                # TODO do correct taxation.
                log.warning(
                    "You received an Aridrop. An airdrop could be taxed as "
                    "`Einkünfte aus sonstigen Leistungen` or `Schenkung` or "
                    "something else?, as the case may be. "
                    "In the current implementation, all airdrops are taxed as "
                    "`Einkünfte aus sonstigen Leistungen`. "
                    "This can result in paying more taxes than necessary. "
                    "Please inform yourself and open a PR to fix this."
                )
                taxation_type = "Einkünfte aus sonstigen Leistungen"
                taxed_gain = self.price_data.get_cost(op)
                tx = tr.TaxEvent(taxation_type, taxed_gain, op)
                self.tax_events.append(tx)

        elif isinstance(op, tr.Commission):
            # TODO write information text
            self.add_to_balance(op)

            if in_tax_year(op):
                # TODO do correct taxation.
                log.warning(
                    "You have received a Commission. "
                    "I am currently unsure how Commissions get taxed. "
                    "For now they are taxed as `Einkünfte aus sonstigen "
                    "Leistungen`. "
                    "Please inform yourself and help us to fix this problem "
                    "by opening and issue or creating a PR."
                )
                taxation_type = "Einkünfte aus sonstigen Leistungen"
                taxed_gain = self.price_data.get_cost(op)
                tx = tr.TaxEvent(taxation_type, taxed_gain, op)
                self.tax_events.append(tx)

        elif isinstance(op, tr.Deposit):
            # Coins get deposited onto this platform/balance.
            # TODO are transaction costs deductable from the tax? if yes, when?
            #      on withdrawal or deposit or on sell of the moved coin??
            self.add_to_balance(op)

        elif isinstance(op, tr.Withdrawal):
            # Coins get moved to somewhere else. At this point, we only have
            # to remove them from the corresponding balance.
            self.remove_from_balance(op)

        else:
            raise NotImplementedError

    ###########################################################################
    # General tax evaluation functions.
    ###########################################################################

    def _evaluate_taxation(self, operations: list[tr.Operation]) -> None:
        """Evaluate the taxation for a list of operations using
        country specific functions.

        Args:
            operations (list[tr.Operation])
        """
        operations = tr.sort_operations(operations, ["utc_time"])
        for operation in operations:
            self.__evaluate_taxation(operation)

        for balance in self._balances.values():
            balance.sanity_check()

        # TODO REFACTOR and try to integrate this into balance.close

        # # Calculate the amount of coins which should be left on the platform
        # # and evaluate the (taxed) gain, if the coin would be sold right now.
        # if config.CALCULATE_UNREALIZED_GAINS and (
        #     (left_coin := misc.dsum((bop.not_sold for bop in balance.queue)))
        # ):
        #     assert isinstance(left_coin, decimal.Decimal)
        #     # Calculate unrealized gains for the last time of `TAX_YEAR`.
        #     # If we are currently in ´TAX_YEAR` take now.
        #     virtual_sell = tr.Sell(
        #         TAX_DEADLINE,
        #         op.platform,
        #         left_coin,
        #         coin,
        #         [-1],
        #         Path(""),
        #     )
        #     if tx_ := self._funktion_verändert_evaluate_sell(virtual_sell, force=True):
        #         self.virtual_tax_events.append(tx_)

    def evaluate_taxation(self) -> None:
        """Evaluate the taxation using country specific function."""
        log.debug("Starting evaluation...")

        assert all(
            op.utc_time.year <= config.TAX_YEAR for op in self.book.operations
        ), "For tax evaluation, no operation should happen after the tax year."

        if config.MULTI_DEPOT:
            # Evaluate taxation separated by platforms and coins.
            for _, operations in misc.group_by(
                self.book.operations, "platform"
            ).items():
                self._evaluate_taxation(operations)
        else:
            # Evaluate taxation separated by coins "in a single virtual depot".
            self._evaluate_taxation(self.book.operations)

    ###########################################################################
    # Export / Summary
    ###########################################################################

    def print_evaluation(self) -> None:
        """Print short summary of evaluation to stdout."""
        eval_str = "Evaluation:\n\n"

        # Summarize the tax evaluation.
        if self.tax_events:
            eval_str += f"Your tax evaluation for {config.TAX_YEAR}:\n"
            for taxation_type, tax_events in misc.group_by(
                self.tax_events, "taxation_type"
            ).items():
                taxed_gains = misc.dsum(tx.taxed_gain for tx in tax_events)
                eval_str += f"{taxation_type}: {taxed_gains:.2f} {config.FIAT}\n"
        else:
            eval_str += (
                "Either the evaluation has not run or there are no tax events "
                f"for {config.TAX_YEAR}.\n"
            )

        # Summarize the virtual sell, if all left over coins would be sold right now.
        if self.virtual_tax_events:
            assert config.CALCULATE_UNREALIZED_GAINS
            latest_operation = max(
                self.virtual_tax_events, key=lambda tx: tx.op.utc_time
            )
            lo_date = latest_operation.op.utc_time.strftime("%d.%m.%y")

            invested = misc.dsum(tx.sell_value for tx in self.virtual_tax_events)
            real_gains = misc.dsum(tx.real_gain for tx in self.virtual_tax_events)
            taxed_gains = misc.dsum(tx.taxed_gain for tx in self.virtual_tax_events)
            eval_str += "\n"
            eval_str += (
                f"Deadline {config.TAX_YEAR}: {lo_date}\n"
                f"You were invested with {invested:.2f} {config.FIAT}.\n"
                f"If you would have sold everything then, "
                f"you would have realized {real_gains:.2f} {config.FIAT} gains "
                f"({taxed_gains:.2f} {config.FIAT} taxed gain).\n"
            )

            eval_str += "\n"
            eval_str += f"Your portfolio on {lo_date} was:\n"
            for tx in sorted(
                self.virtual_tax_events,
                key=lambda tx: tx.sell_value,
                reverse=True,
            ):
                eval_str += (
                    f"{tx.op.platform}: "
                    f"{tx.op.change:.6f} {tx.op.coin} > "
                    f"{tx.sell_value:.2f} {config.FIAT} "
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
                "Date and Time UTC",
                "Platform",
                "Taxation Type",
                f"Taxed Gain in {config.FIAT}",
                "Action",
                "Amount",
                "Asset",
                f"Sell Value in {config.FIAT}",
                "Remark",
            ]
            writer.writerow(header)
            # Tax events are currently sorted by coin. Sort by time instead.
            for tx in sorted(self.tax_events, key=lambda tx: tx.op.utc_time):
                line = [
                    tx.op.utc_time.strftime("%Y-%m-%d %H:%M:%S"),
                    tx.op.platform,
                    tx.taxation_type,
                    tx.taxed_gain,
                    tx.op.__class__.__name__,
                    tx.op.change,
                    tx.op.coin,
                    tx.sell_value,
                    tx.remark,
                ]
                writer.writerow(line)

        log.info("Saved evaluation in %s.", file_path)
        return file_path
