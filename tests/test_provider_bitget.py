from price_providers.bitget import BitgetPriceProvider
from price_providers import FallbackPriceNotFound
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class BitgetProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    @patch("price_providers.bitget.requests.get")
    def test_parses_array_candle(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = [["0", "0", "110", "90", "0", "0"]]
        mock_get.return_value = response

        provider = BitgetPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USDT")

        self.assertEqual(price, decimal.Decimal("100"))

    @patch("price_providers.bitget.requests.get")
    def test_fallback_mode_swapped_not_found_raises(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = []
        mock_get.return_value = response

        provider = BitgetPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        with self.assertRaises(FallbackPriceNotFound):
            provider.fetch_price(
                "BTC",
                self.utc_time,
                "USDT",
                swapped_symbols=True,
                fallback_mode=True,
            )

    @patch("price_providers.bitget.requests.get")
    def test_invalid_symbol_is_cached(self, mock_get: Mock) -> None:
        response = Mock()
        response.status_code = 400
        response.text = "symbol does not exist"
        response.json.return_value = {"code": "40034", "msg": "symbol does not exist"}
        http_error = requests.exceptions.HTTPError(response=response)
        response.raise_for_status.side_effect = http_error
        mock_get.return_value = response

        provider = BitgetPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))

        first = provider.fetch_price("EUR", self.utc_time, "ATH")
        second = provider.fetch_price("EUR", self.utc_time, "ATH")

        self.assertEqual(first, decimal.Decimal("0"))
        self.assertEqual(second, decimal.Decimal("0"))
        self.assertEqual(mock_get.call_count, 2)

    @patch("price_providers.bitget.log.warning")
    @patch("price_providers.bitget.requests.get")
    def test_fallback_exhaustion_is_cached(self, mock_get: Mock, mock_warning: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = []
        mock_get.return_value = response

        def fake_get_price(platform: str, *args, **kwargs):
            if platform == "binance":
                raise FallbackPriceNotFound
            if platform == "bitget" and kwargs.get("fallback_mode"):
                raise FallbackPriceNotFound
            return decimal.Decimal("0")

        provider = BitgetPriceProvider(fake_get_price)

        first = provider.fetch_price("SUI", self.utc_time, "EUR")
        second = provider.fetch_price("SUI", self.utc_time, "EUR")

        self.assertEqual(first, decimal.Decimal("0"))
        self.assertEqual(second, decimal.Decimal("0"))
        self.assertEqual(mock_get.call_count, 1)
        self.assertEqual(mock_warning.call_count, 1)

    @patch("price_providers.bitget.requests.get")
    def test_uses_binance_fallback_when_bitget_has_no_data(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = []
        mock_get.return_value = response

        def fake_get_price(platform: str, *args, **kwargs):
            if platform == "binance":
                return decimal.Decimal("0.92")
            raise FallbackPriceNotFound

        provider = BitgetPriceProvider(fake_get_price)
        price = provider.fetch_price("USDT", self.utc_time, "EUR")

        self.assertEqual(price, decimal.Decimal("0.92"))

    def test_known_missing_symbol_uses_binance_fallback(self) -> None:
        def fake_get_price(platform: str, *args, **kwargs):
            if platform == "binance":
                return decimal.Decimal("0.91")
            raise FallbackPriceNotFound

        provider = BitgetPriceProvider(fake_get_price)
        provider.mark_missing_symbol("USDTEUR")

        price = provider.fetch_price("USDT", self.utc_time, "EUR")
        self.assertEqual(price, decimal.Decimal("0.91"))

    @patch("price_providers.bitget.requests.get")
    def test_zero_candle_uses_binance_fallback(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = [["0", "0", "0", "0", "0", "0"]]
        mock_get.return_value = response

        def fake_get_price(platform: str, *args, **kwargs):
            if platform == "binance":
                return decimal.Decimal("0.93")
            raise FallbackPriceNotFound

        provider = BitgetPriceProvider(fake_get_price)
        price = provider.fetch_price("USDT", self.utc_time, "EUR")

        self.assertEqual(price, decimal.Decimal("0.93"))


if __name__ == "__main__":
    unittest.main()
