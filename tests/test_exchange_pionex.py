from exchanges.pionex import PionexReader
from book import Book
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class DummyPriceData:
    pass


class PionexReaderTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[list[str]]) -> None:
        path.write_text(
            "\n".join(
                ",".join(f'\"{value}\"' for value in row) for row in rows
            )
            + "\n",
            encoding="utf8",
        )

    def test_read_trading_skips_futures_rows(self) -> None:
        reader = PionexReader()
        book = Book(DummyPriceData())
        rows = [
            [
                "date(UTC+0)",
                "executed_qty",
                "amount",
                "price",
                "side",
                "symbol",
                "fee",
                "fee_coin",
                "market_type",
                "tax_id",
            ],
            [
                "2025-09-18 14:41:21",
                "82500.00000000000000000000",
                "2066.05981700000000000000",
                "0.02504315",
                "BUY",
                "VET_USDT_PERP",
                "0.94871495",
                "USDT",
                "Futures USDT",
                "s_4",
            ],
        ]

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "trading.csv"
            self._write_csv(csv_path, rows)
            reader.read_file(csv_path, book)

        self.assertEqual(book.operations, [])

    def test_read_trading_keeps_spot_rows(self) -> None:
        reader = PionexReader()
        book = Book(DummyPriceData())
        rows = [
            [
                "date(UTC+0)",
                "executed_qty",
                "amount",
                "price",
                "side",
                "symbol",
                "fee",
                "fee_coin",
                "market_type",
                "tax_id",
            ],
            [
                "2025-09-18 14:41:21",
                "100",
                "206.6",
                "2.066",
                "BUY",
                "VET_USDT",
                "0.5",
                "USDT",
                "Spot",
                "s_4",
            ],
        ]

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "trading.csv"
            self._write_csv(csv_path, rows)
            reader.read_file(csv_path, book)

        self.assertEqual(len(book.operations), 3)
        self.assertEqual(book.operations[0].type_name, "Buy")
        self.assertEqual(book.operations[0].coin, "VET")
        self.assertEqual(book.operations[1].type_name, "Sell")
        self.assertEqual(book.operations[1].coin, "USDT")
        self.assertEqual(book.operations[2].type_name, "Fee")
        self.assertEqual(book.operations[2].coin, "USDT")
