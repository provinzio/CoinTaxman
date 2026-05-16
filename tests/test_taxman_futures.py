import datetime
import decimal
import os
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

# Taxman imports xlsxwriter at module import time. Provide a minimal stub so
# logic tests can run without optional export dependencies installed.
if "xlsxwriter" not in sys.modules:
    class _WorkbookStub:
        def __init__(self, *args, **kwargs):
            pass

    class _FormatStub:
        pass

    xlsxwriter_stub = types.SimpleNamespace(
        Workbook=_WorkbookStub,
        format=types.SimpleNamespace(Format=_FormatStub),
    )
    sys.modules["xlsxwriter"] = xlsxwriter_stub

from taxman import Taxman
import config
import transaction as tr

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class _BookStub:
    def __init__(self, operations: list[tr.Operation]) -> None:
        self.operations = operations


class _PriceDataStub:
    def get_cost(self, op) -> decimal.Decimal:
        if isinstance(op, tr.SoldCoin):
            return op.sold
        return op.change

    def get_partial_cost(self, op, percent: decimal.Decimal) -> decimal.Decimal:
        return op.change * percent


class _SwapFallbackPriceDataStub(_PriceDataStub):
    def get_partial_cost(self, op, percent: decimal.Decimal) -> decimal.Decimal:
        # Simulate missing fiat valuation for the bought asset in a linked swap.
        if isinstance(op, tr.Buy) and op.coin == "ARB":
            return decimal.Decimal("0")
        return super().get_partial_cost(op, percent)


class TaxmanFuturesTests(unittest.TestCase):
    def _utc(self, month: int, day: int) -> datetime.datetime:
        return datetime.datetime(
            config.TAX_YEAR,
            month,
            day,
            12,
            0,
            0,
            tzinfo=datetime.timezone.utc,
        )

    def test_germany_futures_profit_includes_attached_fees(self) -> None:
        profit = tr.FuturesProfit(
            utc_time=self._utc(1, 10),
            platform="bitunix",
            change=decimal.Decimal("100"),
            coin="USDT",
            line=[1],
            file_path=Path("account_statements/bitunix 2025/bitunix_tax.csv"),
        )
        fee_one = tr.Fee(
            utc_time=self._utc(1, 10),
            platform="bitunix",
            change=decimal.Decimal("2"),
            coin="USDT",
            line=[1],
            file_path=profit.file_path,
        )
        fee_two = tr.Fee(
            utc_time=self._utc(1, 10),
            platform="bitunix",
            change=decimal.Decimal("1"),
            coin="USDT",
            line=[1],
            file_path=profit.file_path,
        )
        profit.fees = [fee_one, fee_two]

        taxman = Taxman(_BookStub([profit]), _PriceDataStub())
        taxman.evaluate_taxation()

        self.assertEqual(len(taxman.tax_report_entries), 1)
        entry = taxman.tax_report_entries[0]
        self.assertIsInstance(entry, tr.FuturesProfitReportEntry)
        self.assertEqual(entry.taxation_type, "Einkünfte aus Termingeschäften")
        self.assertEqual(entry.gain_in_fiat, decimal.Decimal("97"))
        self.assertEqual(entry.taxable_gain_in_fiat, decimal.Decimal("97"))
        self.assertEqual(len(taxman._balances), 0)

    def test_germany_futures_loss_includes_attached_fees(self) -> None:
        loss = tr.FuturesLoss(
            utc_time=self._utc(2, 11),
            platform="pionex",
            change=decimal.Decimal("50"),
            coin="USDT",
            line=[2],
            file_path=Path("account_statements/pionex 2025/position_futures.csv"),
        )
        fee = tr.Fee(
            utc_time=self._utc(2, 11),
            platform="pionex",
            change=decimal.Decimal("3"),
            coin="USDT",
            line=[2],
            file_path=loss.file_path,
        )
        loss.fees = [fee]

        taxman = Taxman(_BookStub([loss]), _PriceDataStub())
        taxman.evaluate_taxation()

        self.assertEqual(len(taxman.tax_report_entries), 1)
        entry = taxman.tax_report_entries[0]
        self.assertIsInstance(entry, tr.FuturesLossReportEntry)
        self.assertEqual(entry.taxation_type, "Einkünfte aus Termingeschäften")
        self.assertEqual(entry.gain_in_fiat, decimal.Decimal("-53"))
        self.assertEqual(entry.taxable_gain_in_fiat, decimal.Decimal("-53"))
        self.assertEqual(len(taxman._balances), 0)

    def test_germany_futures_loss_cap_applies_to_summary(self) -> None:
        loss = tr.FuturesLoss(
            utc_time=self._utc(3, 12),
            platform="bitget",
            change=decimal.Decimal("150"),
            coin="USDT",
            line=[3],
            file_path=Path(
                "account_statements/bitget 2025/Exported USDT-M Futures transactions.csv"
            ),
        )

        with patch.object(
            config,
            "TERMINGESCHAEFTE_VERLUSTVERRECHNUNG_LIMIT_EUR",
            decimal.Decimal("100"),
        ):
            taxman = Taxman(_BookStub([loss]), _PriceDataStub())
            taxman.evaluate_taxation()

            entry = taxman.tax_report_entries[0]
            self.assertEqual(entry.taxable_gain_in_fiat, decimal.Decimal("-150"))
            self.assertEqual(
                taxman._apply_taxable_gain_adjustments(
                    "Einkünfte aus Termingeschäften",
                    decimal.Decimal("-150"),
                ),
                decimal.Decimal("-100"),
            )

    def test_evaluate_sell_fallback_when_linked_withdrawal_coins_missing(self) -> None:
        withdrawal = tr.Withdrawal(
            utc_time=self._utc(1, 1),
            platform="bitget",
            change=decimal.Decimal("1"),
            coin="BTC",
            line=[10],
            file_path=Path("account_statements/bitget 2025/debug.csv"),
        )
        deposit = tr.Deposit(
            utc_time=self._utc(1, 2),
            platform="bitget",
            change=decimal.Decimal("0.99"),
            coin="BTC",
            line=[11],
            file_path=withdrawal.file_path,
        )
        deposit.link = withdrawal

        sell = tr.Sell(
            utc_time=self._utc(1, 3),
            platform="bitget",
            change=decimal.Decimal("0.5"),
            coin="BTC",
            line=[12],
            file_path=withdrawal.file_path,
        )
        sold_coins = [tr.SoldCoin(deposit, decimal.Decimal("0.5"))]

        taxman = Taxman(_BookStub([]), _PriceDataStub())
        taxman.evaluate_sell(sell, sold_coins)

        self.assertEqual(len(taxman.tax_report_entries), 1)
        self.assertIsInstance(taxman.tax_report_entries[0], tr.SellReportEntry)

    def test_bitget_api_sell_adds_synthetic_deposit_on_missing_balance(self) -> None:
        sell = tr.Sell(
            utc_time=self._utc(5, 26),
            platform="bitget",
            change=decimal.Decimal("226.64500148028"),
            coin="USDT",
            line=[94],
            file_path=Path("bitget-api"),
        )

        taxman = Taxman(_BookStub([sell]), _PriceDataStub())
        taxman.evaluate_taxation()

        self.assertEqual(len(taxman.tax_report_entries), 1)
        self.assertIsInstance(taxman.tax_report_entries[0], tr.SellReportEntry)
        self.assertEqual(
            taxman.tax_report_entries[0].taxable_gain_in_fiat,
            decimal.Decimal("0"),
        )

    def test_bitget_csv_withdrawal_adds_synthetic_deposit_on_missing_balance(self) -> None:
        withdrawal = tr.Withdrawal(
            utc_time=self._utc(9, 15),
            platform="bitget",
            change=decimal.Decimal("10000"),
            coin="USDT",
            line=[9747],
            file_path=Path(
                "account_statements/bitget 2025/Export spot transactions 5345536923-2026-04-07 01_11_50.458.csv"
            ),
        )

        taxman = Taxman(_BookStub([withdrawal]), _PriceDataStub())
        taxman.evaluate_taxation()

        self.assertEqual(len(taxman.tax_report_entries), 1)
        self.assertIsInstance(taxman.tax_report_entries[0], tr.WithdrawalReportEntry)

    def test_linked_usdt_sell_uses_disposed_asset_value_when_buy_value_is_zero(self) -> None:
        buy_usdt = tr.Buy(
            utc_time=self._utc(4, 20),
            platform="bitget",
            change=decimal.Decimal("100"),
            coin="USDT",
            line=[1],
            file_path=Path("account_statements/bitget 2025/spot.csv"),
        )
        buy_arb = tr.Buy(
            utc_time=self._utc(4, 21),
            platform="bitget",
            change=decimal.Decimal("50"),
            coin="ARB",
            line=[2],
            file_path=buy_usdt.file_path,
        )
        sell_usdt = tr.Sell(
            utc_time=self._utc(4, 21),
            platform="bitget",
            change=decimal.Decimal("100"),
            coin="USDT",
            line=[2],
            file_path=buy_usdt.file_path,
        )

        buy_arb.link = sell_usdt
        sell_usdt.link = buy_arb

        taxman = Taxman(_BookStub([]), _SwapFallbackPriceDataStub())
        taxman.evaluate_sell(
            sell_usdt,
            [tr.SoldCoin(buy_usdt, decimal.Decimal("100"))],
        )

        self.assertEqual(len(taxman.tax_report_entries), 1)
        entry = taxman.tax_report_entries[0]
        self.assertIsInstance(entry, tr.SellReportEntry)
        self.assertEqual(entry.gain_in_fiat, decimal.Decimal("0"))


if __name__ == "__main__":
    unittest.main()
