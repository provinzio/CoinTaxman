from exchanges.bitunix import BitunixReader
from book import Book
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class DummyPriceData:
    pass


class BitunixReaderTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[list[str]]) -> None:
        path.write_text(
            "\n".join(
                ",".join(f'\"{value}\"' for value in row) for row in rows
            )
            + "\n",
            encoding="utf8",
        )

    def test_read_file_skips_futures_pnl_rows(self) -> None:
        reader = BitunixReader()
        book = Book(DummyPriceData())
        rows = [
            [
                "Date (UTC)",
                "Label",
                "Outgoing Asset",
                "Outgoing Amount",
                "Incoming Asset",
                "Incoming Amount",
                "Fee Asset",
                "Fee Amount",
                "Trx. ID",
                "Comment",
            ],
            [
                "2025-10-06 15:50:59",
                "Futures Loss",
                "USDT",
                "11080.81200226",
                "",
                "0",
                "USDT",
                "37.7791664",
                "T0005",
                "Futures Loss",
            ],
            [
                "2025-10-06 08:18:22",
                "Futures Profit",
                "",
                "0",
                "USDT",
                "603.34886253",
                "USDT",
                "11.35130165",
                "T0004",
                "Futures PnL",
            ],
        ]

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "bitunix_tax.csv"
            self._write_csv(csv_path, rows)
            reader.read_file(csv_path, book)

        self.assertEqual(book.operations, [])

    def test_read_file_keeps_spot_trade_rows(self) -> None:
        reader = BitunixReader()
        book = Book(DummyPriceData())
        rows = [
            [
                "Date (UTC)",
                "Label",
                "Outgoing Asset",
                "Outgoing Amount",
                "Incoming Asset",
                "Incoming Amount",
                "Fee Asset",
                "Fee Amount",
                "Trx. ID",
                "Comment",
            ],
            [
                "2025-09-21 16:24:22",
                "Spot Trade",
                "USDT",
                "763.46",
                "SPX",
                "647",
                "SPX",
                "0.3882",
                "T0001",
                "SPOT BUY",
            ],
        ]

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "bitunix_tax.csv"
            self._write_csv(csv_path, rows)
            reader.read_file(csv_path, book)

        self.assertEqual(len(book.operations), 3)
        self.assertEqual(book.operations[0].type_name, "Sell")
        self.assertEqual(book.operations[1].type_name, "Buy")
        self.assertEqual(book.operations[2].type_name, "Fee")


if __name__ == "__main__":
    unittest.main()
