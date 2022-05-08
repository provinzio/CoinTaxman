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
from database import get_sorted_tablename
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
        self.multi_depot_portfolio: dict[
            str, dict[str, decimal.Decimal]
        ] = collections.defaultdict(lambda: collections.defaultdict(decimal.Decimal))
        self.single_depot_portfolio: dict[
            str, decimal.Decimal
        ] = collections.defaultdict(decimal.Decimal)

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

    def get_buy_cost(self, sc: tr.SoldCoin) -> decimal.Decimal:
        """Calculate the buy cost of a sold coin.

        Args:
            sc (tr.SoldCoin): The sold coin.

        Raises:
            NotImplementedError: Calculation is currently not implemented
                for buy operations.

        Returns:
            decimal.Decimal: The buy value of the sold coin in fiat
        """
        assert sc.sold <= sc.op.change
        percent = sc.sold / sc.op.change

        # Fees paid when buying the now sold coins.
        buying_fees = decimal.Decimal()
        if sc.op.fees:
            buying_fees = misc.dsum(
                self.price_data.get_partial_cost(f, percent) for f in sc.op.fees
            )

        if isinstance(sc.op, tr.Buy):
            # Buy cost of a bought coin should be the sell value of the
            # previously sold coin and not the sell value of the bought coin.
            # Gains of combinations like below are not correctly calculated:
            #   1 BTC=1€, 1ETH=2€, 1BTC=1ETH
            # e.g. buy 1 BTC for 1 €, buy 1 ETH for 1 BTC, buy 2 € for 1 ETH.
            if sc.op.link is None:
                log.warning(
                    "Unable to correctly determine buy cost of bought coins "
                    "because the link to the corresponding previous sell could "
                    "not be estalished. Buying cost will be set to the buy "
                    "value of the bought coins instead of the sell value of the "
                    "previously sold coins of the trade. "
                    "The calculated buy cost might be wrong. "
                    "This may lead to a false tax evaluation.\n"
                    f"{sc.op}"
                )
                buy_value = self.price_data.get_cost(sc)
            else:
                prev_sell_value = self.price_data.get_partial_cost(sc.op.link, percent)
                buy_value = prev_sell_value
        else:
            # All other operations "begin their existence" as that coin and
            # weren't traded/exchanged before.
            # The buy cost of these coins is the value from when yout got them.
            buy_value = self.price_data.get_cost(sc)

        return buy_value + buying_fees

    def _get_fee_param_dict(self, op: tr.Operation, percent: decimal.Decimal) -> dict:

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

        return dict(
            first_fee_amount=first_fee_amount,
            first_fee_coin=first_fee_coin,
            first_fee_in_fiat=first_fee_in_fiat,
            second_fee_amount=second_fee_amount,
            second_fee_coin=second_fee_coin,
            second_fee_in_fiat=second_fee_in_fiat,
        )

    def _evaluate_sell(
        self,
        op: tr.Sell,
        sc: tr.SoldCoin,
        ReportType: Type[tr.SellReportEntry] = tr.SellReportEntry,
    ) -> None:
        """Evaluate a (partial) sell operation.

        Args:
            op (tr.Sell): The general sell operation.
            sc (tr.SoldCoin): The specific sold coins with their origin (sc.op).
                `sc.sold` can be a partial sell of `op.change`.
            ReportType (Type[tr.SellReportEntry], optional):
                The type of the report entry. Defaults to tr.SellReportEntry.

        Raises:
            NotImplementedError: When there are more than two different fee coins.
        """
        assert op.coin == sc.op.coin
        assert op.change >= sc.sold

        # Share the fees and sell_value proportionally to the coins sold.
        percent = sc.sold / op.change

        fee_params = self._get_fee_param_dict(op, percent)
        buy_cost_in_fiat = self.get_buy_cost(sc)

        # TODO Recognized increased speculation period for lended/staked coins?
        is_taxable = not config.IS_LONG_TERM(sc.op.utc_time, op.utc_time)

        try:
            sell_value_in_fiat = self.price_data.get_partial_cost(op, percent)
        except NotImplementedError:
            # Do not raise an error when we are unable to calculate an
            # unrealized sell value.
            if ReportType is tr.UnrealizedSellReportEntry:
                log.warning(
                    f"Gathering prices for platform {op.platform} is currently "
                    "not implemented. Therefore I am unable to calculate the "
                    f"unrealized sell value for your {op.coin} at evaluation "
                    "deadline. If you want to see your unrealized sell value "
                    "in the evaluation, please add a price by hand in the "
                    f"table {get_sorted_tablename(op.coin, config.FIAT)[0]} "
                    f"at {op.utc_time}; "
                    "or open an issue/PR to gather prices for your platform."
                )
                sell_value_in_fiat = decimal.Decimal()
            else:
                raise

        sell_report_entry = ReportType(
            sell_platform=op.platform,
            buy_platform=sc.op.platform,
            amount=sc.sold,
            coin=op.coin,
            sell_utc_time=op.utc_time,
            buy_utc_time=sc.op.utc_time,
            **fee_params,
            sell_value_in_fiat=sell_value_in_fiat,
            buy_cost_in_fiat=buy_cost_in_fiat,
            is_taxable=is_taxable,
            taxation_type="Sonstige Einkünfte",
            remark=op.remark,
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
                assert (
                    sc.op.link.change >= sc.op.change
                ), "Withdrawal must be equal or greater than the deposited amount."
                deposit_fee = sc.op.link.change - sc.op.change
                sold_percent = sc.sold / sc.op.change
                sold_deposit_fee = deposit_fee * sold_percent

                for wsc in sc.op.link.partial_withdrawn_coins(sold_percent):
                    wsc_percent = wsc.sold / sc.op.link.change
                    wsc_deposit_fee = sold_deposit_fee * wsc_percent

                    if wsc_deposit_fee:
                        # TODO Are withdrawal/deposit fees tax relevant?
                        log.warning(
                            "You paid fees for withdrawal and deposit of coins. "
                            "I am currently not sure if you can reduce your taxed "
                            "gain with these. For now, the deposit/withdrawal fees "
                            "are not included in the tax report. "
                            "Please open an issue or PR if you can resolve this."
                        )
                        # # Deposit fees are evaluated on deposited platform.
                        # wsc_fee_in_fiat = (
                        #     self.price_data.get_price(
                        #         sc.op.platform,
                        #         sc.op.coin,
                        #         sc.op.utc_time,
                        #         config.FIAT,
                        #     )
                        #     * wsc_deposit_fee
                        # )

                    self._evaluate_sell(op, wsc)

            else:

                if isinstance(sc.op, tr.Deposit):
                    # Raise a warning when a deposit link is missing.
                    log.warning(
                        f"You sold {sc.op.change} {sc.op.coin} which were deposited "
                        f"from somewhere unknown onto {sc.op.platform} (see "
                        f"{sc.op.file_path} {sc.op.line}). "
                        "A correct tax evaluation is not possible! "
                        "For now, we assume that the coins were bought at "
                        "the timestamp of the deposit."
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
            # if it's unclear; it might be nice to match start and
            # end of these operations like deposit and withdrawal operations.
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

            # Add tp export for informational purpose.
            fee_params = self._get_fee_param_dict(op, decimal.Decimal(1))
            tax_report_entry = tr.BuyReportEntry(
                platform=op.platform,
                amount=op.change,
                coin=op.coin,
                utc_time=op.utc_time,
                **fee_params,
                buy_value_in_fiat=self.price_data.get_cost(op),
                remark=op.remark,
            )
            self.tax_report_entries.append(tax_report_entry)

        elif isinstance(op, tr.Sell):
            # Buys and sells always come in a pair. The selling/redeeming
            # time is tax relevant.
            # Remove the sold coins and paid fees from the balance.
            # Evaluate the sell to determine the taxed gain and other relevant
            # informations for the tax declaration.
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
                    remark=op.remark,
                )
                self.tax_report_entries.append(report_entry)

        elif isinstance(op, tr.Airdrop):
            # Depending on how you received the coins, the taxation varies.
            # If you didn't "do anything" to get the coins, the airdrop counts
            # as a gift.
            self.add_to_balance(op)

            if in_tax_year(op):
                if config.ALL_AIRDROPS_ARE_GIFTS:
                    taxation_type = "Schenkung"
                else:
                    taxation_type = "Einkünfte aus sonstigen Leistungen"
                report_entry = tr.AirdropReportEntry(
                    platform=op.platform,
                    amount=op.change,
                    coin=op.coin,
                    utc_time=op.utc_time,
                    in_fiat=self.price_data.get_cost(op),
                    taxation_type=taxation_type,
                    remark=op.remark,
                )
                self.tax_report_entries.append(report_entry)

        elif isinstance(op, tr.Commission):
            # You received a commission. It is assumed that his is a customer-
            # recruit-customer-bonus which is taxed as `Einkünfte aus sonstigen
            # Leistungen`.
            self.add_to_balance(op)

            if in_tax_year(op):
                report_entry = tr.CommissionReportEntry(
                    platform=op.platform,
                    amount=op.change,
                    coin=op.coin,
                    utc_time=op.utc_time,
                    in_fiat=self.price_data.get_cost(op),
                    taxation_type="Einkünfte aus sonstigen Leistungen",
                    remark=op.remark,
                )
                self.tax_report_entries.append(report_entry)

        elif isinstance(op, tr.Deposit):
            # Coins get deposited onto this platform/balance.
            self.add_to_balance(op)

            if op.link:
                assert op.coin == op.link.coin
                assert op.fees is None
                first_fee_amount = op.link.change - op.change
                first_fee_coin = op.coin if first_fee_amount else ""
                first_fee_in_fiat = (
                    self.price_data.get_price(op.platform, op.coin, op.utc_time)
                    if first_fee_amount
                    else decimal.Decimal()
                )
                report_entry = tr.TransferReportEntry(
                    first_platform=op.platform,
                    second_platform=op.link.platform,
                    amount=op.change,
                    coin=op.coin,
                    first_utc_time=op.utc_time,
                    second_utc_time=op.link.utc_time,
                    first_fee_amount=first_fee_amount,
                    first_fee_coin=first_fee_coin,
                    first_fee_in_fiat=first_fee_in_fiat,
                    remark=op.remark,
                )
            else:
                assert op.fees is None
                report_entry = tr.DepositReportEntry(
                    platform=op.platform,
                    amount=op.change,
                    coin=op.coin,
                    utc_time=op.utc_time,
                    first_fee_amount=decimal.Decimal(),
                    first_fee_coin="",
                    first_fee_in_fiat=decimal.Decimal(),
                    remark=op.remark,
                )

            self.tax_report_entries.append(report_entry)

        elif isinstance(op, tr.Withdrawal):
            # Coins get moved to somewhere else. At this point, we only have
            # to remove them from the corresponding balance.
            op.withdrawn_coins = self.remove_from_balance(op)

            if not op.has_link:
                assert op.fees is None
                report_entry = tr.WithdrawalReportEntry(
                    platform=op.platform,
                    amount=op.change,
                    coin=op.coin,
                    utc_time=op.utc_time,
                    first_fee_amount=decimal.Decimal(),
                    first_fee_coin="",
                    first_fee_in_fiat=decimal.Decimal(),
                    remark=op.remark,
                )
                self.tax_report_entries.append(report_entry)

        else:
            raise NotImplementedError

    def _evaluate_unrealized_sells(self) -> None:
        """Evaluate the unrealized sells at taxation deadline."""
        for balance in self._balances.values():
            # Get all left over coins from the balance.
            sold_coins = balance.remove_all()
            for sc in sold_coins:
                # Sum up the portfolio at deadline.
                # If the evaluation was done with a virtual single depot,
                # the values per platform might not match the real values at
                # platform.
                self.multi_depot_portfolio[sc.op.platform][sc.op.coin] += sc.sold
                self.single_depot_portfolio[sc.op.coin] += sc.sold

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

    ###########################################################################
    # General tax evaluation functions.
    ###########################################################################

    def evaluate_taxation(self) -> None:
        """Evaluate the taxation using country specific functions."""
        log.debug("Starting evaluation...")

        assert all(
            op.utc_time.year <= config.TAX_YEAR for op in self.book.operations
        ), "For tax evaluation, no operation should happen after the tax year."

        # Sort the operations by time.
        operations = tr.sort_operations(self.book.operations, ["utc_time"])

        # Evaluate the operations one by one.
        # Difference between the config.MULTI_DEPOT and "single depot" method
        # is done by keeping balances per platform and coin or only
        # per coin (see self.balance).
        for operation in operations:
            self.__evaluate_taxation(operation)

        # Make sure, that all fees were paid.
        for balance in self._balances.values():
            balance.sanity_check()

        # Evaluate the balance at deadline to calculate unrealized sells.
        if config.CALCULATE_UNREALIZED_GAINS:
            self._evaluate_unrealized_sells()

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
            if taxation_type is None:
                continue
            taxable_gain = misc.dsum(
                tre.taxable_gain_in_fiat
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
            tre.taxable_gain_in_fiat for tre in unrealized_report_entries
        )

        if config.CALCULATE_UNREALIZED_GAINS:
            eval_str += (
                "----------------------------------------\n"
                f"Unrealized gain: {unrealized_gain:.2f} {config.FIAT}\n"
                "Unrealized taxable gain at deadline: "
                f"{unrealized_taxable_gain:.2f} {config.FIAT}\n"
                "----------------------------------------\n"
                f"Your portfolio on {TAX_DEADLINE.strftime('%x')} was:\n"
            )

        if config.MULTI_DEPOT:
            for platform, platform_portfolio in self.multi_depot_portfolio.items():
                for coin, amount in platform_portfolio.items():
                    eval_str += f"{platform} {coin}: {amount:.2f}\n"
        else:
            for coin, amount in self.single_depot_portfolio.items():
                eval_str += f"{coin}: {amount:.2f}\n"

        log.info(eval_str)

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
            config.EXPORT_PATH, str(config.TAX_YEAR), ["xlsx", "log"]
        )
        wb = xlsxwriter.Workbook(file_path, {"remove_timezone": True})
        datetime_format = wb.add_format({"num_format": "dd.mm.yyyy hh:mm;@"})
        date_format = wb.add_format({"num_format": "dd.mm.yyyy;@"})
        change_format = wb.add_format({"num_format": "#,##0.00000000"})
        fiat_format = wb.add_format({"num_format": "#,##0.00"})
        header_format = wb.add_format(
            {
                "bold": True,
                "border": 5,
                "align": "center",
                "valign": "center",
                "text_wrap": True,
            }
        )

        def get_format(field: dataclasses.Field) -> Optional[xlsxwriter.format.Format]:
            if field.type in ("datetime.datetime", "Optional[datetime.datetime]"):
                return datetime_format
            if field.type in ("decimal.Decimal", "Optional[decimal.Decimal]"):
                if field.name.endswith("in_fiat"):
                    return fiat_format
                return change_format
            return None

        #
        # General
        #
        ws_general = wb.add_worksheet("Allgemein")
        ws_general.merge_range(0, 0, 0, 1, "Allgemeine Daten", header_format)
        ws_general.write_row(1, 0, ["Stichtag", TAX_DEADLINE.date()], date_format)
        ws_general.write_row(
            2,
            0,
            ["Erstellt am", datetime.datetime.now(config.LOCAL_TIMEZONE)],
            datetime_format,
        )
        ws_general.write_row(
            3, 0, ["Software", "CoinTaxman <https://github.com/provinzio/CoinTaxman>"]
        )
        commit_hash = misc.get_current_commit_hash(default="undetermined")
        ws_general.write_row(4, 0, ["Commit", commit_hash])
        ws_general.write_row(5, 0, ["Alle Zeiten in", config.LOCAL_TIMEZONE_KEY])
        # Set column format and freeze first row.
        ws_general.set_column(0, 0, 13)
        ws_general.set_column(1, 1, 20)
        ws_general.freeze_panes(1, 0)

        #
        # Add summary of tax relevant amounts.
        #
        ws_summary = wb.add_worksheet("Zusammenfassung")
        ws_summary.write_row(
            0, 0, ["Einkunftsart", "steuerbarer Betrag in EUR"], header_format
        )
        ws_summary.set_row(0, 30)
        row = 1
        for taxation_type, tax_report_entries in misc.group_by(
            self.tax_report_entries, "taxation_type"
        ).items():
            if taxation_type is None:
                continue
            taxable_gain = misc.dsum(
                tre.taxable_gain_in_fiat
                for tre in tax_report_entries
                if not isinstance(tre, tr.UnrealizedSellReportEntry)
            )
            ws_summary.write_row(row, 0, [taxation_type, taxable_gain])
            row += 1
        # Set column format and freeze first row.
        ws_summary.set_column(0, 0, 35)
        ws_summary.set_column(1, 1, 13, fiat_format)
        ws_summary.freeze_panes(1, 0)

        #
        # Sheets per ReportType
        #
        for event_type, tax_report_entries in misc.group_by(
            self.tax_report_entries, "event_type"
        ).items():
            ReportType = type(tax_report_entries[0])

            ws = wb.add_worksheet(event_type)

            # Header
            labels = ReportType.excel_labels()
            ws.write_row(0, 0, labels, header_format)
            # Set height
            ws.set_row(0, 45)
            ws.autofilter(f"A1:{misc.column_num_to_string(len(labels))}1")

            # Data
            for row, entry in enumerate(tax_report_entries, 1):
                ws.write_row(row, 0, entry.excel_values())

            # Set column format and freeze first row.
            for col, (field, width, hidden) in enumerate(
                ReportType.excel_field_and_width()
            ):
                cell_format = get_format(field)
                ws.set_column(
                    col,
                    col,
                    width,
                    cell_format,
                    dict(hidden=hidden),
                )
            ws.freeze_panes(1, 0)

        wb.close()
        log.info("Saved evaluation in %s.", file_path)
        return file_path
