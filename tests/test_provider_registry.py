from price_providers.registry import create_price_provider
from price_providers.pionex import PionexPriceProvider
import decimal
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class ProviderRegistryTests(unittest.TestCase):
    def test_returns_known_provider(self) -> None:
        provider = create_price_provider(
            "pionex", lambda *args, **kwargs: decimal.Decimal("0")
        )
        self.assertIsInstance(provider, PionexPriceProvider)

    def test_returns_none_for_unknown_provider(self) -> None:
        provider = create_price_provider(
            "unknown", lambda *args, **kwargs: decimal.Decimal("0")
        )
        self.assertIsNone(provider)


if __name__ == "__main__":
    unittest.main()
