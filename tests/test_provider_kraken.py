from price_providers.kraken import KrakenPriceProvider
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class KrakenProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(
            2025, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc)

    @patch("price_providers.kraken.requests.get")
    def test_returns_price_from_closest_trade(self, mock_get: Mock) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        ts_old = str(self.utc_time.timestamp() - 120)
        ts_new = str(self.utc_time.timestamp() + 120)
        response.text = (
            '{"error": [], "result": {"BTCUSD": [['
            '"100.0","1","' + ts_old + '","b","l",""],'
            '["200.0","1","' + ts_new + '","b","l",""]], "last": "0"}}'
        )
        mock_get.return_value = response

        provider = KrakenPriceProvider(lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "USD")

        self.assertEqual(price, decimal.Decimal("100.0"))


if __name__ == "__main__":
    unittest.main()
