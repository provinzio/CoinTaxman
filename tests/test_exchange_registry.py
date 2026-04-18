from exchanges.registry import create_exchange_reader, detect_exchange_reader
from exchanges.pionex import PionexReader
from exchanges.coinbase import CoinbaseReader
from exchanges.binance import BinanceReader
import csv
import os
import sys
import tempfile
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
