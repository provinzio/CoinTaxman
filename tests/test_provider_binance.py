from price_providers.binance import BinancePriceProvider
from price_providers import FallbackPriceNotFound
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class BinanceProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    @patch("price_providers.binance.requests.get")
    def test_weighted_average_price(self, mock_get: Mock) -> None:
        response = Mock()
        response.text = '[{"p":"10","q":"2"},{"p":"20","q":"1"}]'
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        provider = BinancePriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USDT")

        self.assertEqual(price.quantize(decimal.Decimal("0.00000001")),
                         decimal.Decimal("13.33333333"))

    @patch("price_providers.binance.requests.get")
    def test_fallback_mode_swapped_raises(self, mock_get: Mock) -> None:
        response = Mock()
        response.text = '{"code":-1121,"msg":"Invalid symbol."}'
        mock_get.return_value = response

        provider = BinancePriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        with self.assertRaises(FallbackPriceNotFound):
            provider.fetch_price(
                "BTC",
                self.utc_time,
                "USDT",
                swapped_symbols=True,
                fallback_mode=True,
            )

    @patch("price_providers.binance.requests.get")
    def test_connection_error_returns_zero(self, mock_get: Mock) -> None:
        mock_get.side_effect = requests.exceptions.ConnectionError()

        provider = BinancePriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USDT")

        self.assertEqual(price, decimal.Decimal("0"))

    @patch("price_providers.binance.requests.get")
    def test_connection_error_in_fallback_mode_raises(self, mock_get: Mock) -> None:
        mock_get.side_effect = requests.exceptions.ConnectionError()

        provider = BinancePriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        with self.assertRaises(FallbackPriceNotFound):
            provider.fetch_price(
                "BTC",
                self.utc_time,
                "USDT",
                fallback_mode=True,
            )


if __name__ == "__main__":
    unittest.main()
