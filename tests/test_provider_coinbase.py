from price_providers.coinbase import CoinbaseProPriceProvider
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class CoinbaseProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(
            2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

    @patch("price_providers.coinbase.requests.get")
    def test_returns_average_of_open_close_from_closest_candle(self, mock_get: Mock) -> None:
        response = Mock()
        older = int(self.utc_time.timestamp()) - 60
        newer = int(self.utc_time.timestamp()) + 60
        response.text = str([
            [newer, "0", "0", "100", "110", "0"],
            [older, "0", "0", "200", "220", "0"],
        ]).replace("'", '"')
        response.raise_for_status.return_value = None
        mock_get.return_value = response

        provider = CoinbaseProPriceProvider(
            lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USD")

        self.assertEqual(price, decimal.Decimal("105"))


if __name__ == "__main__":
    unittest.main()
