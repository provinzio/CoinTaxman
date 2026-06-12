from book import Book
import transaction as tr
import datetime
import decimal
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class BookResolveDepositsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(
            2025, 1, 3, 12, 0, tzinfo=datetime.timezone.utc
        )
        self.file_path = Path("account_statements/pionex 2025/trading.csv")

    def test_resolve_deposits_ignores_conversion_like_pairs(self) -> None:
        book = Book(Mock())

        withdrawal = tr.Withdrawal(
            utc_time=self.utc_time,
            platform="pionex",
            change=decimal.Decimal("100"),
            coin="USDT",
            line=[10],
            file_path=self.file_path,
        )
        deposit = tr.Deposit(
            utc_time=self.utc_time,
            platform="pionex",
            change=decimal.Decimal("0.0021"),
            coin="BTC",
            line=[11],
            file_path=self.file_path,
        )

        book.operations = [withdrawal, deposit]
        book.resolve_deposits()

        self.assertNotIn("Herkunft der Einzahlung unbekannt", deposit.remarks)
        self.assertNotIn("Ziel der Auszahlung unbekannt", withdrawal.remarks)

    def test_resolve_deposits_marks_real_unmatched_deposit(self) -> None:
        book = Book(Mock())

        deposit = tr.Deposit(
            utc_time=self.utc_time,
            platform="pionex",
            change=decimal.Decimal("0.1"),
            coin="ETH",
            line=[12],
            file_path=self.file_path,
        )

        book.operations = [deposit]
        book.resolve_deposits()

        self.assertIn("Herkunft der Einzahlung unbekannt", deposit.remarks)

    def test_resolve_deposits_ignores_bitget_internal_future_transfer_deposit(self) -> None:
        book = Book(Mock())

        deposit = tr.Deposit(
            utc_time=self.utc_time,
            platform="bitget",
            change=decimal.Decimal("250"),
            coin="USDT",
            line=[13],
            file_path=Path("bitget-api"),
            remarks=["Bitget future record 123 (taxType: TRANSFER_IN)"],
        )

        book.operations = [deposit]
        book.resolve_deposits()

        self.assertNotIn("Herkunft der Einzahlung unbekannt", deposit.remarks)


if __name__ == "__main__":
    unittest.main()
