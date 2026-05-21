from exchanges.trade_republic import TradeRepublicReader
from book import Book
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class DummyPriceData:
    pass


class TradeRepublicReaderTests(unittest.TestCase):
    def _write_csv(self, path: Path, rows: list[list[str]]) -> None:
        path.write_text(
            "\n".join(";".join(row) for row in rows) + "\n",
            encoding="utf8",
        )

    def test_read_file_parses_buy_and_sell_with_fee(self) -> None:
        reader = TradeRepublicReader()
        book = Book(DummyPriceData())
        rows = [
            [
                "Asset",
                "transaktion",
                "nominale",
                "preis_pro_stück",
                "gebühren",
                "gebucht",
                "gewinn",
                "gewinn_<1_jahr",
            ],
            [
                "BTC",
                "13.04.2025 KAUF",
                "0,009905",
                "75.702,20",
                "1,00",
                "-750,83",
                "0,00",
                "0,00",
            ],
            [
                "BTC",
                "06.10.2025 VERKAUF",
                "0,016448",
                "105.050,13",
                "1,00",
                "1.726,86",
                "475,10",
                "475,10",
            ],
        ]

        with tempfile.TemporaryDirectory() as tmp:
            csv_path = Path(tmp) / "traderepublic 2025.csv"
            self._write_csv(csv_path, rows)
            reader.read_file(csv_path, book)

        self.assertEqual(len(book.operations), 6)
        self.assertEqual(book.operations[0].type_name, "Buy")
        self.assertEqual(book.operations[0].coin, "BTC")
        self.assertEqual(book.operations[1].type_name, "Sell")
        self.assertEqual(book.operations[1].coin, "EUR")
        self.assertEqual(book.operations[2].type_name, "Fee")
        self.assertEqual(book.operations[2].coin, "EUR")

        self.assertEqual(book.operations[3].type_name, "Sell")
        self.assertEqual(book.operations[3].coin, "BTC")
        self.assertEqual(book.operations[4].type_name, "Buy")
        self.assertEqual(book.operations[4].coin, "EUR")
        self.assertEqual(book.operations[5].type_name, "Fee")
        self.assertEqual(book.operations[5].coin, "EUR")


if __name__ == "__main__":
    unittest.main()
