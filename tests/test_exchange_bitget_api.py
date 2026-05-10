from exchanges.bitget_api import BitgetApiReader
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import patch

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


class BitgetApiReaderTests(unittest.TestCase):
    def test_import_spot_records_uses_spot_tax_type_field(self) -> None:
        reader = BitgetApiReader()
        book = _BookStub()
        records = [
            {
                "coin": "USDT",
                "spotTaxType": "Transfer out",
                "amount": "-10000",
                "fee": "0",
                "ts": "1758198095000",
                "bizOrderId": "order-1",
            },
            {
                "coin": "EUR",
                "spotTaxType": "fiat_recharge_in",
                "amount": "3000",
                "fee": "0",
                "ts": "1758197000000",
                "bizOrderId": "order-2",
            },
        ]

        with patch.object(
            reader,
            "_fetch_all_range",
            return_value=[(0, 0, records, {}, {})],
        ):
            reader.import_spot_records(book, 0, 0)

        self.assertEqual(len(book.operations), 2)
        self.assertEqual(book.operations[0]["operation"], "Withdrawal")
        self.assertEqual(book.operations[0]["coin"], "USDT")
        self.assertEqual(book.operations[0]["platform"], "bitget")
        self.assertEqual(book.operations[1]["operation"], "Deposit")
        self.assertEqual(book.operations[1]["coin"], "EUR")
        self.assertEqual(
            book.operations[0]["utc_time"],
            datetime.datetime.fromtimestamp(1758198095, datetime.timezone.utc),
        )

    def test_import_future_records_maps_signed_pnl_to_futures_operations(self) -> None:
        reader = BitgetApiReader()
        book = _BookStub()
        records = [
            {
                "coin": "USDT",
                "taxType": "CLOSE_LONG",
                "amount": "120.5",
                "fee": "0",
                "ts": "1758198095000",
                "bizOrderId": "future-1",
            },
            {
                "coin": "USDT",
                "taxType": "OPEN_LONG",
                "amount": "-42.25",
                "fee": "0",
                "ts": "1758199095000",
                "bizOrderId": "future-2",
            },
        ]

        with patch.object(
            reader,
            "_fetch_all_range",
            return_value=[(0, 0, records, {}, {})],
        ):
            reader.import_future_records(book, 0, 0)

        self.assertEqual(len(book.operations), 2)
        self.assertEqual(book.operations[0]["operation"], "FuturesProfit")
        self.assertEqual(book.operations[0]["change"], decimal.Decimal("120.5"))
        self.assertEqual(book.operations[1]["operation"], "FuturesLoss")
        self.assertEqual(book.operations[1]["change"], decimal.Decimal("42.25"))


if __name__ == "__main__":
    unittest.main()
