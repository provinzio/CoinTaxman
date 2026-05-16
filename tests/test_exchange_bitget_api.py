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

    def test_import_future_records_supports_future_tax_type_and_margin_coin_fields(self) -> None:
        reader = BitgetApiReader()
        book = _BookStub()
        records = [
            {
                "marginCoin": "USDT",
                "futureTaxType": "close_short",
                "amount": "10.0",
                "fee": "0",
                "ts": "1758198095000",
                "bizOrderId": "future-3",
            },
            {
                "marginCoin": "USDT",
                "futureTaxType": "open_long",
                "amount": "-1.5",
                "fee": "0",
                "ts": "1758199095000",
                "bizOrderId": "future-4",
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
        self.assertEqual(book.operations[0]["coin"], "USDT")
        self.assertEqual(book.operations[0]["change"], decimal.Decimal("10.0"))
        self.assertEqual(book.operations[1]["operation"], "FuturesLoss")
        self.assertEqual(book.operations[1]["coin"], "USDT")
        self.assertEqual(book.operations[1]["change"], decimal.Decimal("1.5"))

    def test_import_spot_records_maps_consumption_and_ignores_gains(self) -> None:
        reader = BitgetApiReader()
        book = _BookStub()
        records = [
            {
                "coin": "USDT",
                "spotTaxType": "Consumption",
                "amount": "-3876.8169",
                "fee": "0",
                "ts": "1748242582267",
                "bizOrderId": "order-3",
            },
            {
                "coin": "NEAR",
                "spotTaxType": "Gains",
                "amount": "1359.32",
                "fee": "0",
                "ts": "1748242582250",
                "bizOrderId": "order-3",
            },
        ]

        with patch.object(
            reader,
            "_fetch_all_range",
            return_value=[(0, 0, records, {}, {})],
        ):
            reader.import_spot_records(book, 0, 0)

        self.assertEqual(len(book.operations), 1)
        self.assertEqual(book.operations[0]["operation"], "Sell")
        self.assertEqual(book.operations[0]["coin"], "USDT")
        self.assertEqual(book.operations[0]["change"], decimal.Decimal("3876.8169"))

    def test_map_spot_tax_type_supports_withdrawal_and_copy_refund_variants(self) -> None:
        reader = BitgetApiReader()

        self.assertEqual(
            reader._map_spot_tax_type("Ordinary Withdrawal"),
            "Withdrawal",
        )
        self.assertEqual(
            reader._map_spot_tax_type("Copy trade - Profit share refunds"),
            "Commission",
        )

    def test_import_spot_copy_trade_records_maps_buy_sell_and_fees(self) -> None:
        reader = BitgetApiReader()
        book = _BookStub()
        records = [
            {
                "trackingNo": "123",
                "traderId": "999",
                "fillSize": "0.0316",
                "buyFee": "-0.00001902",
                "sellFee": "-0.66104988",
                "symbol": "BTCUSDT",
                "buyTime": "1695729617968",
                "sellTime": "1695729886269",
            }
        ]

        with patch.object(
            reader,
            "_fetch_copy_trade_history_range",
            return_value=[(0, 0, records, {}, {})],
        ):
            reader.import_spot_copy_trade_records(book, 0, 0)

        self.assertEqual(len(book.operations), 4)
        self.assertEqual(book.operations[0]["operation"], "Buy")
        self.assertEqual(book.operations[0]["coin"], "BTC")
        self.assertEqual(book.operations[0]["change"], decimal.Decimal("0.0316"))
        self.assertEqual(book.operations[1]["operation"], "Fee")
        self.assertEqual(book.operations[1]["coin"], "USDT")
        self.assertEqual(
            book.operations[1]["change"],
            decimal.Decimal("0.00001902"),
        )
        self.assertEqual(book.operations[2]["operation"], "Sell")
        self.assertEqual(book.operations[2]["coin"], "BTC")
        self.assertEqual(book.operations[3]["operation"], "Fee")
        self.assertEqual(book.operations[3]["coin"], "USDT")
        self.assertEqual(
            book.operations[3]["change"],
            decimal.Decimal("0.66104988"),
        )

    def test_import_future_copy_trade_records_maps_pnl_and_fees(self) -> None:
        reader = BitgetApiReader()
        book = _BookStub()
        records = [
            {
                "trackingNo": "456",
                "traderId": "888",
                "symbol": "BTCUSDT",
                "netProfit": "-697.74100000",
                "openFee": "-5.92649260",
                "closeFee": "-5.22875160",
                "closeTime": "1695353868557",
            }
        ]

        with patch.object(
            reader,
            "_fetch_future_copy_trade_history_range",
            side_effect=[
                [(0, 0, records, {}, {})],
                [(0, 0, [], {}, {})],
                [(0, 0, [], {}, {})],
            ],
        ):
            reader.import_future_copy_trade_records(book, 0, 0)

        self.assertEqual(len(book.operations), 3)
        self.assertEqual(book.operations[0]["operation"], "FuturesLoss")
        self.assertEqual(book.operations[0]["coin"], "USDT")
        self.assertEqual(
            book.operations[0]["change"],
            decimal.Decimal("697.74100000"),
        )
        self.assertEqual(book.operations[1]["operation"], "Fee")
        self.assertEqual(book.operations[1]["coin"], "USDT")
        self.assertEqual(
            book.operations[1]["change"],
            decimal.Decimal("5.92649260"),
        )
        self.assertEqual(book.operations[2]["operation"], "Fee")
        self.assertEqual(book.operations[2]["coin"], "USDT")
        self.assertEqual(
            book.operations[2]["change"],
            decimal.Decimal("5.22875160"),
        )


if __name__ == "__main__":
    unittest.main()
