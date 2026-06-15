from price_providers.bitpanda import BitpandaProPriceProvider
import datetime
import decimal
import os
import sys
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class BitpandaProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    @patch("price_providers.bitpanda.requests.get")
    def test_returns_average_high_low(self, mock_get: Mock) -> None:
        response = Mock()
        response.status_code = 200
        response.json.return_value = [{"high": "10", "low": "8"}]
        mock_get.return_value = response

        provider = BitpandaProPriceProvider(
            lambda *args, **kwargs: decimal.Decimal("0"))
        price = provider.fetch_price("BTC", self.utc_time, "EUR")

        self.assertEqual(price, decimal.Decimal("9"))


if __name__ == "__main__":
    unittest.main()
