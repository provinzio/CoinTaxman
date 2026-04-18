import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path

from exchanges.binance import BinanceReader
from exchanges.bitget_csv import BitgetCsvReader
from exchanges.coinbase import CoinbaseReader
from exchanges.pionex import PionexReader
from exchanges.registry import create_exchange_reader, detect_exchange_reader

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class ExchangeRegistryTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[list[str]]) -> None:
        with path.open("w", encoding="utf8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerows(rows)

    def test_create_exchange_reader_uses_versioned_binance(self) -> None:
        reader = create_exchange_reader("binance_v3")
        self.assertIsInstance(reader, BinanceReader)
        assert isinstance(reader, BinanceReader)
        self.assertEqual(reader.version, 3)

    def test_create_exchange_reader_uses_versioned_coinbase(self) -> None:
        reader = create_exchange_reader("coinbase_v4")
        self.assertIsInstance(reader, CoinbaseReader)
        assert isinstance(reader, CoinbaseReader)
        self.assertEqual(reader.version, 4)

    def test_create_exchange_reader_returns_none_for_unknown_exchange(self) -> None:
        self.assertIsNone(create_exchange_reader("does_not_exist"))

    def test_detect_exchange_reader_detects_coinbase_v4(self) -> None:
        header = [
            "ID",
            "Timestamp",
            "Transaction Type",
            "Asset",
            "Quantity Transacted",
            "Price Currency",
            "Price at Transaction",
            "Subtotal",
            "Total (inclusive of fees and/or spread)",
            "Fees and/or Spread",
            "Notes",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "coinbase_v4.csv"
            self._write_csv(csv_path, [["x"], ["x"], ["x"], header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, CoinbaseReader)
        assert isinstance(reader, CoinbaseReader)
        self.assertEqual(reader.version, 4)

    def test_detect_exchange_reader_detects_pionex_by_filename(self) -> None:
        header = ["date(UTC+0)", "tx_type", "amount", "coin", "network", "txid", "fee"]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "deposit-withdraw.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, PionexReader)

    def test_detect_exchange_reader_detects_bitget_deposit_withdraw(self) -> None:
        header = [
            "Date",
            "Type",
            "Funding account",
            "Coin",
            "Quantity",
            "Address",
            "TxID",
            "Status",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "withdrawal records 123.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, BitgetCsvReader)

    def test_detect_exchange_reader_detects_bitget_spot_transactions(self) -> None:
        header = ["order", "Date", "Coin", "Type", "Amount", "Fee", "Available"]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "Export spot transactions 123.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, BitgetCsvReader)

    def test_detect_exchange_reader_detects_bitget_unified_transactions(self) -> None:
        header = [
            "Order ID",
            "Date",
            "Trade Type",
            "Coin",
            "Trading Pair",
            "Transaction Type",
            "Amount",
            "Fee",
            "Balance Changes",
            "Balance",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / \
                "Export transactions of unified trading account 123.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, BitgetCsvReader)

    def test_detect_exchange_reader_detects_bitget_onchain_transactions(self) -> None:
        header = ["Coin", "Type", "Time", "Quantity", "Balance"]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "Export Onchain transactions 123.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, BitgetCsvReader)

    def test_detect_exchange_reader_detects_bitget_earn(self) -> None:
        header = ["Product name", "Amount", "Profit type", "Date", "Type", "Status"]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "Export Earn-Simple Earn Flexible-profit.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, BitgetCsvReader)

    def test_detect_exchange_reader_detects_bitget_spot_order_details(self) -> None:
        header = [
            "Date",
            "Trading pair",
            "Base Asset",
            "Quote Asset",
            "Direction",
            "Price",
            "Amount",
            "Total",
            "Fee",
            "Fee Coin",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "Export spot order details 123.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, BitgetCsvReader)

    def test_detect_exchange_reader_detects_bitget_futures_position_history(self) -> None:
        header = [
            "Futures",
            "Opening time",
            "Average entry price",
            "Average closing price",
            "Closed amount",
            "Closed value",
            "Position Pnl",
            "Realized PnL",
            "Fees",
            "Opening fee",
            "Closing fee",
            "Closed time",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "Exported USDT-M Futures position history 123.csv"
            self._write_csv(csv_path, [header])
            reader = detect_exchange_reader(csv_path)

        self.assertIsInstance(reader, BitgetCsvReader)


if __name__ == "__main__":
    unittest.main()
