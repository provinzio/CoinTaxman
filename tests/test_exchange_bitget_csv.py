from exchanges.bitget_csv import BitgetCsvReader
import csv
import datetime
import decimal
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class _BookStub:
    def __init__(self) -> None:
        self.operations = []

    def append_operation(
        self,
        operation,
        utc_time,
        platform,
        change,
        coin,
        row,
        file_path,
        remark=None,
    ) -> None:
        self.operations.append(
            {
                "operation": operation,
                "utc_time": utc_time,
                "platform": platform,
                "change": change,
                "coin": coin,
                "row": row,
                "file_path": str(file_path),
                "remark": remark,
            }
        )


class BitgetCsvReaderTests(unittest.TestCase):
    def test_spot_transactions_ignore_ordinary_withdrawal_when_withdrawal_records_exist(self) -> None:
        reader = BitgetCsvReader()
        book = _BookStub()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            spot_path = tmp / "Export spot transactions 123.csv"
            withdraw_path = tmp / "Export withdrawal records 123.csv"

            with spot_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["order", "Date", "Coin", "Type",
                                "Amount", "Fee", "Available"])
                writer.writerow([
                    "1",
                    "2025-09-15 16:59:22",
                    "USDT",
                    "Ordinary Withdrawal",
                    "-10000",
                    "0",
                    "456.56",
                ])
                writer.writerow([
                    "2",
                    "2025-09-15 16:41:27",
                    "USDT",
                    "Fiat",
                    "10466.5676",
                    "0",
                    "10466.5676",
                ])

            with withdraw_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["Date", "Type", "Funding account",
                                "Coin", "Quantity", "Address", "TxID", "Status"])
                writer.writerow([
                    "2025-09-15 16:56:18",
                    "Withdraw",
                    "Spot account",
                    "USDT",
                    "10000",
                    "On-chain address",
                    "tx",
                    "Successful",
                ])

            reader.read_file(spot_path, book)

        self.assertEqual(len(book.operations), 1)
        self.assertEqual(book.operations[0]["operation"], "Buy")
        self.assertEqual(book.operations[0]["coin"], "USDT")
        self.assertEqual(book.operations[0]["change"], decimal.Decimal("10466.5676"))
        self.assertEqual(
            book.operations[0]["utc_time"],
            datetime.datetime(2025, 9, 15, 16, 41, 27, tzinfo=datetime.timezone.utc),
        )

    def test_unified_account_transactions_maps_futures_pnl_signed(self) -> None:
        reader = BitgetCsvReader()
        book = _BookStub()

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp = Path(tmp_dir)
            unified_path = tmp / "Export transactions of unified trading account 1.csv"

            with unified_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "Date",
                        "Coin",
                        "Transaction Type",
                        "Amount",
                        "Fee",
                        "Order ID",
                        "Trade Type",
                        "Trading Pair",
                    ]
                )
                writer.writerow(
                    [
                        "2025-04-25 10:00:00",
                        "USDT",
                        "close_long",
                        "12.5",
                        "0",
                        "o1",
                        "Futures",
                        "BTCUSDT",
                    ]
                )
                writer.writerow(
                    [
                        "2025-04-25 11:00:00",
                        "USDT",
                        "open_short",
                        "-3.25",
                        "0",
                        "o2",
                        "Futures",
                        "BTCUSDT",
                    ]
                )

            reader.read_file(unified_path, book)

        self.assertEqual(len(book.operations), 2)
        self.assertEqual(book.operations[0]["operation"], "FuturesProfit")
        self.assertEqual(book.operations[0]["change"], decimal.Decimal("12.5"))
        self.assertEqual(book.operations[1]["operation"], "FuturesLoss")
        self.assertEqual(book.operations[1]["change"], decimal.Decimal("3.25"))


if __name__ == "__main__":
    unittest.main()
