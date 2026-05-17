import transaction as tr
from book import Book
import datetime
import decimal
import os
import sys
import unittest
from pathlib import Path

from unittest.mock import Mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class BookFeeMatchingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(
            2025, 10, 6, 0, 52, 39, tzinfo=datetime.timezone.utc)
        self.file_path = Path("account_statements/pionex 2025/trading.csv")

    def _buy(self, amount: str, coin: str, line: int) -> tr.Buy:
        return tr.Buy(
            utc_time=self.utc_time,
            platform="pionex",
            change=decimal.Decimal(amount),
            coin=coin,
            line=[line],
            file_path=self.file_path,
        )

    def _sell(self, amount: str, coin: str, line: int | list[int]) -> tr.Sell:
        lines = [line] if isinstance(line, int) else line
        return tr.Sell(
            utc_time=self.utc_time,
            platform="pionex",
            change=decimal.Decimal(amount),
            coin=coin,
            line=lines,
            file_path=self.file_path,
        )

    def _fee(self, amount: str, line: int | list[int]) -> tr.Fee:
        lines = [line] if isinstance(line, int) else line
        return tr.Fee(
            utc_time=self.utc_time,
            platform="pionex",
            change=decimal.Decimal(amount),
            coin="USDT",
            line=lines,
            file_path=self.file_path,
        )

    def test_match_fees_splits_merged_fee_for_four_transactions(self) -> None:
        book = Book(Mock())

        buy_link = self._buy("275.72", "LINK", 222)
        sell_usdt = self._sell("5831.3989", "USDT", 222)
        sell_link = self._sell("6.78", "LINK", 223)
        buy_usdt = self._buy("155.0812", "USDT", 223)
        merged_fee = self._fee("2.73421695", [222, 223])

        book.operations = [buy_link, sell_usdt, sell_link, buy_usdt, merged_fee]
        book.match_fees()

        expected_half = decimal.Decimal("1.367108475")
        for op in [buy_link, sell_usdt, sell_link, buy_usdt]:
            self.assertIsNotNone(op.fees)
            assert op.fees is not None
            self.assertEqual(len(op.fees), 1)
            self.assertEqual(op.fees[0].change, expected_half)
            self.assertEqual(len(op.fees[0].line), 1)

    def test_match_fees_handles_shared_transaction_across_two_lines(self) -> None:
        book = Book(Mock())

        sell_xrp = self._sell("53", "XRP", 272)
        buy_usdt = self._buy("191.4197", "USDT", 272)
        buy_usdt.line = [272, 273]
        sell_inj = self._sell("3.4", "INJ", 273)
        merged_fee = self._fee("0.03828394", [272, 273])

        book.operations = [sell_xrp, buy_usdt, sell_inj, merged_fee]
        book.match_fees()

        expected_half = decimal.Decimal("0.01914197")

        self.assertIsNotNone(sell_xrp.fees)
        assert sell_xrp.fees is not None
        self.assertEqual(len(sell_xrp.fees), 1)
        self.assertEqual(sell_xrp.fees[0].change, expected_half)

        self.assertIsNotNone(sell_inj.fees)
        assert sell_inj.fees is not None
        self.assertEqual(len(sell_inj.fees), 1)
        self.assertEqual(sell_inj.fees[0].change, expected_half)

        self.assertIsNotNone(buy_usdt.fees)
        assert buy_usdt.fees is not None
        self.assertEqual(len(buy_usdt.fees), 2)
        self.assertEqual(
            sum((f.change for f in buy_usdt.fees), decimal.Decimal("0")),
            decimal.Decimal("0.03828394"),
        )

    def test_match_fees_converts_fee_only_group_to_sell(self) -> None:
        book = Book(Mock())

        fee = tr.Fee(
            utc_time=self.utc_time,
            platform="pionex",
            change=decimal.Decimal("0.219953308243032"),
            coin="USDT",
            line=[1619],
            file_path=Path("account_statements/pionex 2025/others.csv"),
        )

        book.operations = [fee]
        book.match_fees()

        self.assertEqual(len(book.operations), 1)
        converted = book.operations[0]
        self.assertIsInstance(converted, tr.Sell)
        assert isinstance(converted, tr.Sell)
        self.assertEqual(converted.coin, "USDT")
        self.assertEqual(converted.change, decimal.Decimal("0.219953308243032"))
        self.assertIn(
            "Unmatched standalone fee treated as sell",
            converted.remarks,
        )

    def test_match_fees_converts_futures_fee_only_group_to_futures_loss(self) -> None:
        book = Book(Mock())

        fee = tr.Fee(
            utc_time=self.utc_time,
            platform="bitget",
            change=decimal.Decimal("0.001382500201"),
            coin="USDT",
            line=[1619],
            file_path=Path(
                "account_statements/bitget 2025/Exported USDT-M Futures transactions.csv"),
            remarks=["Bitget futures contract_main_settle_fee BTCUSDT 1299209590342819873"],
        )

        book.operations = [fee]
        book.match_fees()

        self.assertEqual(len(book.operations), 1)
        converted = book.operations[0]
        self.assertIsInstance(converted, tr.FuturesLoss)
        assert isinstance(converted, tr.FuturesLoss)
        self.assertEqual(converted.coin, "USDT")
        self.assertEqual(converted.change, decimal.Decimal("0.001382500201"))
        self.assertIn(
            "Unmatched standalone fee treated as futures loss",
            converted.remarks,
        )

    def test_match_fees_attaches_fee_to_single_sell(self) -> None:
        book = Book(Mock())

        sell = self._sell("17.56275844", "USDT", 54)
        fee = self._fee("17.05652844", 54)
        fee.platform = "bitunix"
        sell.platform = "bitunix"

        book.operations = [sell, fee]
        book.match_fees()

        self.assertEqual(len(book.operations), 1)
        self.assertIs(book.operations[0], sell)
        self.assertIsNotNone(sell.fees)
        assert sell.fees is not None
        self.assertEqual(len(sell.fees), 1)
        self.assertEqual(sell.fees[0].change, decimal.Decimal("17.05652844"))

    def test_match_fees_keeps_fee_for_single_commission(self) -> None:
        book = Book(Mock())

        commission = tr.Commission(
            utc_time=self.utc_time,
            platform="bitunix",
            change=decimal.Decimal("21.24747089"),
            coin="USDT",
            line=[60],
            file_path=Path("account_statements/bitunix 2025/bitunix_tax.csv"),
        )
        fee = tr.Fee(
            utc_time=self.utc_time,
            platform="bitunix",
            change=decimal.Decimal("1.0"),
            coin="USDT",
            line=[60],
            file_path=Path("account_statements/bitunix 2025/bitunix_tax.csv"),
        )

        book.operations = [commission, fee]
        book.match_fees()

        self.assertEqual(len(book.operations), 2)
        self.assertIs(book.operations[0], commission)
        converted = book.operations[1]
        self.assertIsInstance(converted, tr.Sell)
        assert isinstance(converted, tr.Sell)
        self.assertEqual(converted.change, decimal.Decimal("1.0"))

    def test_match_fees_attaches_standalone_futures_fee_by_order_id(self) -> None:
        book = Book(Mock())

        futures_profit = tr.FuturesProfit(
            utc_time=self.utc_time,
            platform="bitget",
            change=decimal.Decimal("100"),
            coin="USDT",
            line=[300],
            file_path=Path(
                "account_statements/bitget 2025/Exported USDT-M Futures transactions.csv"),
            remarks=["Bitget futures close_long BTCUSDT 1299209590342819873"],
        )
        fee = tr.Fee(
            utc_time=self.utc_time,
            platform="bitget",
            change=decimal.Decimal("0.001382500201"),
            coin="USDT",
            line=[301],
            file_path=Path(
                "account_statements/bitget 2025/Exported USDT-M Futures transactions.csv"),
            remarks=["Bitget futures contract_main_settle_fee BTCUSDT 1299209590342819873"],
        )

        book.operations = [futures_profit, fee]
        book.match_fees()

        self.assertEqual(len(book.operations), 1)
        self.assertIs(book.operations[0], futures_profit)
        self.assertIsNotNone(futures_profit.fees)
        assert futures_profit.fees is not None
        self.assertEqual(len(futures_profit.fees), 1)
        self.assertEqual(futures_profit.fees[0].change,
                         decimal.Decimal("0.001382500201"))


if __name__ == "__main__":
    unittest.main()
