from price_providers.pionex import PionexPriceProvider
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class PionexProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    @patch("price_providers.pionex.requests.get")
    def test_direct_symbol_close_price(self, mock_get: Mock) -> None:
        response = Mock()
        response.json.return_value = {
            "data": {"tickers": [{"symbol": "BTC_USDT", "close": "123.45"}]}
        }
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        provider = PionexPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USDT")

        self.assertEqual(price, decimal.Decimal("123.45"))

    @patch("price_providers.pionex.requests.get")
    def test_swapped_symbol_uses_reciprocal(self, mock_get: Mock) -> None:
        response1 = Mock()
        response1.json.return_value = {"data": {"tickers": []}}
        response1.raise_for_status.return_value = None

        response2 = Mock()
        response2.json.return_value = {
            "data": {"tickers": [{"symbol": "USDT_BTC", "close": "2"}]}
        }
        response2.raise_for_status.return_value = None

        mock_get.side_effect = [response1, response2]

        provider = PionexPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USDT")

        self.assertEqual(price, decimal.Decimal("0.5"))


if __name__ == "__main__":
    unittest.main()
