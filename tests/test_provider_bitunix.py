from price_providers.bitunix import BitunixPriceProvider
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class BitunixProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    @patch("price_providers.bitunix.requests.get")
    def test_403_falls_back_to_binance(self, mock_get: Mock) -> None:
        response = Mock()
        response.status_code = 403
        http_error = requests.exceptions.HTTPError(response=response)
        response.raise_for_status.side_effect = http_error
        mock_get.return_value = response

        def fake_get_price(platform: str, *args, **kwargs):
            if platform == "binance":
                return decimal.Decimal("99")
            raise AssertionError("unexpected platform")

        provider = BitunixPriceProvider(fake_get_price)
        price = provider.fetch_price("BTC", self.utc_time, "USDT")

        self.assertEqual(price, decimal.Decimal("99"))

    @patch("price_providers.bitunix.requests.get")
    def test_missing_symbol_is_cached(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"data": {"tickers": []}}
        mock_get.return_value = response

        provider = BitunixPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))

        first = provider.fetch_price("EUR", self.utc_time, "ATH")
        second = provider.fetch_price("EUR", self.utc_time, "ATH")

        self.assertEqual(first, decimal.Decimal("0"))
        self.assertEqual(second, decimal.Decimal("0"))
        self.assertEqual(mock_get.call_count, 2)

    @patch("price_providers.bitunix.requests.get")
    def test_connection_error_returns_zero(self, mock_get: Mock) -> None:
        mock_get.side_effect = requests.exceptions.ConnectionError()

        provider = BitunixPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USDT")

        self.assertEqual(price, decimal.Decimal("0"))


if __name__ == "__main__":
    unittest.main()
