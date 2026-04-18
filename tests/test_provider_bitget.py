from price_providers.bitget import BitgetPriceProvider
from price_providers import FallbackPriceNotFound
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

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


if __name__ == "__main__":
    unittest.main()
