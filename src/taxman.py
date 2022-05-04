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
import csv
import datetime
import decimal
from pathlib import Path
from typing import Any, Optional, Type

import xlsxwriter

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
    datetime.datetime.now().replace(tzinfo=config.LOCAL_TIMEZONE),  # now
    datetime.datetime(
        config.TAX_YEAR, 12, 31, 23, 59, 59, tzinfo=config.LOCAL_TIMEZONE
    ),  # end of year
)


def in_tax_year(op: tr.Operation) -> bool:
    return op.utc_time.year == config.TAX_YEAR


class Taxman:
    def __init__(self, book: Book, price_data: PriceData) -> None:
        self.book = book
        self.price_data = price_data

        self.tax_report_entries: list[tr.TaxReportEntry] = []
        self.portfolio_at_deadline: dict[
            str, dict[str, decimal.Decimal]
        ] = collections.defaultdict(lambda: collections.defaultdict(decimal.Decimal))

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

    def _evaluate_fee(
        self,
        fee: tr.Fee,
        percent: decimal.Decimal,
    ) -> tuple[decimal.Decimal, str, decimal.Decimal]:
        return (
            fee.change * percent,
            fee.coin,
            self.price_data.get_partial_cost(fee, percent),
        )

    def _evaluate_sell(
        self,
        op: tr.Sell,
        sc: tr.SoldCoin,
        additional_fee: Optional[decimal.Decimal] = None,
        ReportType: Type[tr.SellReportEntry] = tr.SellReportEntry,
    ) -> None:
        """Evaluate a (partial) sell operation.

        Args:
            op (tr.Sell): The sell operation.
            sc (tr.SoldCoin): The sold coin.
            additional_fee (Optional[decimal.Decimal], optional):
                The additional fee. Defaults to None.
            ReportType (Type[tr.SellReportEntry], optional):
                The type of the report entry. Defaults to tr.SellReportEntry.

        Raises:
            NotImplementedError: When there are more than two different fee coins.
        """
        assert op.coin == sc.op.coin
        if additional_fee is None:
            additional_fee = decimal.Decimal()

        # Share the fees and sell_value proportionally to the coins sold.
        percent = sc.sold / op.change

        # fee amount/coin/in_fiat
        first_fee_amount = decimal.Decimal(0)
        first_fee_coin = ""
        first_fee_in_fiat = decimal.Decimal(0)
        second_fee_amount = decimal.Decimal(0)
        second_fee_coin = ""
        second_fee_in_fiat = decimal.Decimal(0)
        if op.fees is None or len(op.fees) == 0:
            pass
        elif len(op.fees) >= 1:
            first_fee_amount, first_fee_coin, first_fee_in_fiat = self._evaluate_fee(
                op.fees[0], percent
            )
        elif len(op.fees) >= 2:
            second_fee_amount, second_fee_coin, second_fee_in_fiat = self._evaluate_fee(
                op.fees[1], percent
            )
        else:
            raise NotImplementedError("More than two fee coins are not supported")

        # buying_fees
        if sc.op.fees:
            sc_percent = sc.sold / sc.op.change
            buying_fees = misc.dsum(
                self.price_data.get_partial_cost(f, sc_percent) for f in sc.op.fees
            )
        else:
            buying_fees = decimal.Decimal()
        # buy_value_in_fiat
        buy_value_in_fiat = self.price_data.get_cost(sc) + buying_fees + additional_fee

        # TODO Recognized increased speculation period for lended/staked coins?
        # TODO handle operations on sell differently? are some not tax relevant?
        #      gifted Airdrops, Commission?
        is_taxable = not config.IS_LONG_TERM(sc.op.utc_time, op.utc_time)

        sell_report_entry = ReportType(
            sell_platform=op.platform,
            buy_platform=sc.op.platform,
            amount=sc.sold,
            coin=op.coin,
            sell_utc_time=op.utc_time,
            buy_utc_time=sc.op.utc_time,
            first_fee_amount=first_fee_amount,
            first_fee_coin=first_fee_coin,
            first_fee_in_fiat=first_fee_in_fiat,
            second_fee_amount=second_fee_amount,
            second_fee_coin=second_fee_coin,
            second_fee_in_fiat=second_fee_in_fiat,
            sell_value_in_fiat=self.price_data.get_partial_cost(op, percent),
            buy_value_in_fiat=buy_value_in_fiat,
            is_taxable=is_taxable,
            taxation_type="Sonstige Einkünfte",
            remark="",
        )

        self.tax_report_entries.append(sell_report_entry)

    def evaluate_sell(
        self,
        op: tr.Sell,
        sold_coins: list[tr.SoldCoin],
    ) -> None:
        assert op.coin != config.FIAT
        assert in_tax_year(op)
        assert op.change == misc.dsum(sc.sold for sc in sold_coins)

        for sc in sold_coins:

            if isinstance(sc.op, tr.Deposit) and sc.op.link:
                # TODO Are withdrawal/deposit fees tax relevant?
                assert (
                    sc.op.link.change >= sc.op.change
                ), "Withdrawal must be equal or greather the deposited amount."
                deposit_fee = sc.op.link.change - sc.op.change
                sold_percent = sc.sold / sc.op.change
                sold_deposit_fee = deposit_fee * sold_percent

                for wsc in sc.op.link.partial_withdrawn_coins(sold_percent):
                    wsc_percent = wsc.sold / sc.op.link.change
                    wsc_deposit_fee = sold_deposit_fee * wsc_percent

                    wsc_fee_in_fiat = decimal.Decimal()
                    if wsc_deposit_fee:
                        # TODO Are withdrawal/deposit fees tax relevant?
                        log.warning(
                            "You paid fees for withdrawal and deposit of coins. "
                            "I am currently not sure if you can reduce your taxed "
                            "gain with these. For now, the deposit/withdrawal fees "
                            "are not included in the tax report. "
                            "Please open an issue or PR if you can resolve this."
                        )
                        # Deposit fees are evaluated on deposited platform.
                        # wsc_fee_in_fiat = (
                        #     self.price_data.get_price(
                        #         sc.op.platform, sc.op.coin, sc.op.utc_time, config.FIAT
                        #     )
                        #     * wsc_deposit_fee
                        # )

                    self._evaluate_sell(op, wsc, wsc_fee_in_fiat)

            else:

                if isinstance(sc.op, tr.Deposit):
                    # Raise a warning when a deposit link is missing.
                    log.warning(
                        f"You sold {sc.op.change} {sc.op.coin} which were deposited "
                        f"from somewhere unknown onto {sc.op.platform} (see "
                        f"{sc.op.file_path} {sc.op.line}). "
                        "A correct tax evaluation is not possible! "
                        "For now, we assume that the coins were bought at deposit."
                    )

                self._evaluate_sell(op, sc)

    def _evaluate_taxation_GERMANY(self, op: tr.Operation) -> None:
        report_entry: tr.TaxReportEntry

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
            # TODO Add Lending/Staking TaxReportEntry (duration of lend)
            # TODO maybe add total accumulated fees?
            #      might be impossible to match CoinInterest with CoinLend periods
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
                self.evaluate_sell(op, sold_coins)

        elif isinstance(op, (tr.CoinLendInterest, tr.StakingInterest)):
            # Received coins from lending or staking. Add the received coins
            # to the balance.
            self.add_to_balance(op)

            if in_tax_year(op):
                # Determine the taxation type depending on the received coin.
                if isinstance(op, tr.CoinLendInterest):
                    if misc.is_fiat(op.coin):
                        ReportType = tr.InterestReportEntry
                        taxation_type = "Einkünfte aus Kapitalvermögen"
                    else:
                        ReportType = tr.LendingInterestReportEntry
                        taxation_type = "Einkünfte aus sonstigen Leistungen"
                elif isinstance(op, tr.StakingInterest):
                    ReportType = tr.StakingInterestReportEntry
                    taxation_type = "Einkünfte aus sonstigen Leistungen"
                else:
                    raise NotImplementedError

                report_entry = ReportType(
                    platform=op.platform,
                    amount=op.change,
                    coin=op.coin,
                    utc_time=op.utc_time,
                    interest_in_fiat=self.price_data.get_cost(op),
                    taxation_type=taxation_type,
                    remark="",
                )
                self.tax_report_entries.append(report_entry)

        elif isinstance(op, tr.Airdrop):
            # Depending on how you received the coins, the taxation varies.
            # If you didn't "do anything" to get the coins, the airdrop counts
            # as a gift.
            self.add_to_balance(op)

            if in_tax_year(op):
                # TODO do correct taxation.
                log.warning(
                    "You received an Airdrop. An airdrop could be taxed as "
                    "`Einkünfte aus sonstigen Leistungen` or `Schenkung` or "
                    "something else?, as the case may be. "
                    "In the current implementation, all airdrops are taxed as "
                    "`Einkünfte aus sonstigen Leistungen`. "
                    "This can result in paying more taxes than necessary. "
                    "Please inform yourself and open a PR to fix this."
                )
                if True:
                    taxation_type = "Einkünfte aus sonstigen Leistungen"
                else:
                    taxation_type = "Schenkung"
                report_entry = tr.AirdropReportEntry(
                    platform=op.platform,
                    amount=op.change,
                    coin=op.coin,
                    utc_time=op.utc_time,
                    in_fiat=self.price_data.get_cost(op),
                    taxation_type=taxation_type,
                    remark="",
                )
                self.tax_report_entries.append(report_entry)

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
                    "by opening an issue or creating a PR."
                )
                report_entry = tr.CommissionReportEntry(
                    platform=op.platform,
                    amount=op.change,
                    coin=op.coin,
                    utc_time=op.utc_time,
                    in_fiat=self.price_data.get_cost(op),
                    taxation_type="Einkünfte aus sonstigen Leistungen",
                    remark="",
                )
                self.tax_report_entries.append(report_entry)

        elif isinstance(op, tr.Deposit):
            # Coins get deposited onto this platform/balance.
            # TODO are transaction costs deductable from the tax? if yes, when?
            #      on withdrawal or deposit or on sell of the moved coin??
            #      > currently tax relevant on sell
            self.add_to_balance(op)

            if op.link:
                assert op.coin == op.link.coin
                report_entry = tr.TransferReportEntry(
                    first_platform=op.platform,
                    second_platform=op.link.platform,
                    amount=op.change,
                    coin=op.coin,
                    first_utc_time=op.utc_time,
                    second_utc_time=op.link.utc_time,
                    first_fee_amount=op.link.change - op.change,
                    first_fee_coin=op.coin,
                    first_fee_in_fiat=self.price_data.get_cost(op),
                    remark="",
                )
                self.tax_report_entries.append(report_entry)

        elif isinstance(op, tr.Withdrawal):
            # Coins get moved to somewhere else. At this point, we only have
            # to remove them from the corresponding balance.
            op.withdrawn_coins = self.remove_from_balance(op)

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

        # Evaluate the balance at deadline.
        for balance in self._balances.values():
            balance.sanity_check()

            # Calculate the unrealized profit/loss.
            sold_coins = balance.remove_all()
            for sc in sold_coins:
                # Sum up the portfolio at deadline.
                self.portfolio_at_deadline[sc.op.platform][sc.op.coin] += sc.sold

                # "Sell" these coins which makes it possible to calculate the
                # unrealized gain afterwards.
                unrealized_sell = tr.Sell(
                    utc_time=TAX_DEADLINE,
                    platform=sc.op.platform,
                    change=sc.sold,
                    coin=sc.op.coin,
                    line=[-1],
                    file_path=Path(),
                    fees=None,
                )
                self._evaluate_sell(
                    unrealized_sell,
                    sc,
                    ReportType=tr.UnrealizedSellReportEntry,
                )
                # TODO UnrealizedSellReportEntry nicht von irgendwas vererben?
                # TODO _evaluate_sell darf noch nicht hinzufügen, nur anlegen?
                #      return ReportType
                # TODO offene Positionen nur platform/coin, wert,... ohne kauf
                #      und verkaufsdatum

                # TODO ODER Offene  Position bei "Einkunftsart" -> "Herkunft"
                #      (Kauf, Interest, ...)
                # TODO dann noch eine Zusammenfassung der offenen Positionen

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
        eval_str = (
            f"Your tax evaluation for {config.TAX_YEAR} "
            f"(Deadline {TAX_DEADLINE.strftime('%d.%m.%Y')}):\n\n"
        )
        for taxation_type, tax_report_entries in misc.group_by(
            self.tax_report_entries, "taxation_type"
        ).items():
            taxable_gain = misc.dsum(
                tre.taxable_gain
                for tre in tax_report_entries
                if not isinstance(tre, tr.UnrealizedSellReportEntry)
            )
            eval_str += f"{taxation_type}: {taxable_gain:.2f} {config.FIAT}\n"

        unrealized_report_entries = [
            tre
            for tre in self.tax_report_entries
            if isinstance(tre, tr.UnrealizedSellReportEntry)
        ]
        assert all(tre.gain_in_fiat is not None for tre in unrealized_report_entries)
        unrealized_gain = misc.dsum(
            misc.not_none(tre.gain_in_fiat) for tre in unrealized_report_entries
        )
        unrealized_taxable_gain = misc.dsum(
            tre.taxable_gain for tre in unrealized_report_entries
        )
        eval_str += (
            "----------------------------------------\n"
            f"Unrealized gain: {unrealized_gain:.2f} {config.FIAT}\n"
            "Unrealized taxable gain at deadline: "
            f"{unrealized_taxable_gain:.2f} {config.FIAT}\n"
            "----------------------------------------\n"
            f"Your portfolio on {TAX_DEADLINE.strftime('%x')} was:\n"
        )

        for platform, platform_portfolio in self.portfolio_at_deadline.items():
            for coin, amount in platform_portfolio.items():
                eval_str += f"{platform} {coin}: {amount:.2f}\n"

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

            # Header
            header = [
                "Verkauf auf Börse",
                "Erworben von Börse",
                #
                "Anzahl",
                "Währung",
                #
                "Verkaufsdatum",
                "Erwerbsdatum",
                #
                "(1) Anzahl Transaktionsgebühr",
                "(1) Währung Transaktionsgebühr",
                "(1) Transaktionsgebühr in EUR",
                "(2) Anzahl Transaktionsgebühr",
                "(2) Währung Transaktionsgebühr",
                "(2) Transaktionsgebühr in EUR",
                #
                "Veräußerungserlös in EUR",
                "Anschaffungskosten in EUR",
                "Gesamt Transaktionsgebühr in EUR",
                #
                "Gewinn/Verlust in EUR",
                "davon steuerbar",
                "Einkunftsart",
                "Bemerkung",
            ]
            writer.writerow(header)

            # Tax events are currently sorted by coin. Sort by time instead.
            assert all(
                tre.first_utc_time is not None for tre in self.tax_report_entries
            )
            for tre in sorted(
                self.tax_report_entries,
                key=lambda tre: misc.not_none(tre.first_utc_time),
            ):
                assert isinstance(tre, tr.TaxReportEntry)
                writer.writerow(tre.values())

        log.info("Saved evaluation in %s.", file_path)
        return file_path

    def export_evaluation_as_excel(self) -> Path:
        """Export detailed summary of all tax events to Excel.

        File will be placed in export/ with ascending revision numbers
        (in case multiple evaluations will be done).

        When no tax events occured, the Excel will be exported only with
        a header line and a general sheet.

        Returns:
            Path: Path to the exported file.
        """
        file_path = misc.get_next_file_path(
            config.EXPORT_PATH, str(config.TAX_YEAR), "xlsx"
        )
        wb = xlsxwriter.Workbook(file_path, {"remove_timezone": True})

        # General
        ws_general = wb.add_worksheet("Allgemein")
        commit_hash = misc.get_current_commit_hash(default="undetermined")
        general_data = [
            ["Allgemeine Daten"],
            ["Stichtag", TAX_DEADLINE],
            ["Erstellt am", datetime.datetime.now()],
            ["Software", "CoinTaxman <https://github.com/provinzio/CoinTaxman>"],
            ["Commit", commit_hash],
        ]
        for row, data in enumerate(general_data):
            ws_general.write_row(row, 0, data)

        # Sheets per ReportType
        for ReportType, tax_report_entries in misc.group_by(
            self.tax_report_entries, "__class__"
        ).items():
            ws = wb.add_worksheet(ReportType.event_type)
            # Header
            # TODO increase height of first row
            ws.write_row(0, 0, ReportType.labels())
            # TODO set column width (custom?) and correct format (datetime,
            #      change up to 8 decimal places, ...)

            for row, entry in enumerate(tax_report_entries, 1):
                ws.write_row(row, 0, entry.values())

        wb.close()
        log.info("Saved evaluation in %s.", file_path)
        return file_path
