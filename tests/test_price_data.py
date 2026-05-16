from price_providers.base import PriceProvider
from price_data import PriceData
import config
import datetime
import decimal
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class FakePriceProvider(PriceProvider):
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs,
    ) -> decimal.Decimal:
        self.mark_missing_symbol(f"{base_asset}{quote_asset}")
        return decimal.Decimal("0")


class RefreshingPionexProvider(PriceProvider):
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs,
    ) -> decimal.Decimal:
        if base_asset == "USDT" and quote_asset == "EUR":
            return decimal.Decimal("0.91")
        return decimal.Decimal("0")


class PriceDataTests(unittest.TestCase):
    def setUp(self) -> None:
        self.utc_time = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    def test_get_price_persists_missing_symbol_cache_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir)

            with patch.object(config, "DATA_PATH", data_path), patch(
                "price_data.get_price_db", return_value=None
            ), patch("price_data.set_price_db"), patch(
                "price_data.create_price_provider",
                side_effect=lambda platform, get_price_func, missing_symbols=None: FakePriceProvider(
                    get_price_func,
                    missing_symbols=missing_symbols,
                ),
            ):
                price_data = PriceData()

                price = price_data.get_price("bitget", "EUR", self.utc_time, "ATH")

                self.assertEqual(price, decimal.Decimal("0"))
                cache_path = data_path / "missing_price_symbols.json"
                self.assertTrue(cache_path.exists())
                with open(cache_path, encoding="utf8") as f:
                    payload = json.load(f)

                self.assertEqual(payload, {"bitget": ["EURATH"]})

    def test_loads_persisted_missing_symbols_and_reuses_provider(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            data_path = Path(tmpdir)
            cache_path = data_path / "missing_price_symbols.json"
            cache_path.write_text(
                json.dumps({"bitget": ["EURATH"]}),
                encoding="utf8",
            )

            created_providers = []

            def factory(platform, get_price_func, missing_symbols=None):
                provider = FakePriceProvider(
                    get_price_func,
                    missing_symbols=missing_symbols,
                )
                created_providers.append(provider)
                return provider

            with patch.object(config, "DATA_PATH", data_path), patch(
                "price_data.create_price_provider",
                side_effect=factory,
            ):
                price_data = PriceData()

                first_provider = price_data._get_provider("bitget")
                second_provider = price_data._get_provider("bitget")

                self.assertIs(first_provider, second_provider)
                self.assertEqual(len(created_providers), 1)
                self.assertTrue(first_provider.is_known_missing_symbol("EURATH"))

    def test_refreshes_cached_zero_pionex_stablecoin_price(self) -> None:
        with patch("price_data.get_price_db", return_value=decimal.Decimal("0")), patch(
            "price_data.set_price_db"
        ) as set_price_db_mock, patch(
            "price_data.create_price_provider",
            side_effect=lambda platform, get_price_func, missing_symbols=None: RefreshingPionexProvider(
                get_price_func,
                missing_symbols=missing_symbols,
            ),
        ):
            price_data = PriceData()

            price = price_data.get_price("pionex", "USDT", self.utc_time, "EUR")

            self.assertEqual(price, decimal.Decimal("0.91"))
            set_price_db_mock.assert_called_once_with(
                "pionex",
                "USDT",
                "EUR",
                self.utc_time,
                decimal.Decimal("0.91"),
                overwrite=True,
            )

    def test_refreshes_cached_zero_bitget_stablecoin_price(self) -> None:
        with patch("price_data.get_price_db", return_value=decimal.Decimal("0")), patch(
            "price_data.set_price_db"
        ) as set_price_db_mock, patch(
            "price_data.create_price_provider",
            side_effect=lambda platform, get_price_func, missing_symbols=None: RefreshingPionexProvider(
                get_price_func,
                missing_symbols=missing_symbols,
            ),
        ):
            price_data = PriceData()

            price = price_data.get_price("bitget", "USDT", self.utc_time, "EUR")

            self.assertEqual(price, decimal.Decimal("0.91"))
            set_price_db_mock.assert_called_once_with(
                "bitget",
                "USDT",
                "EUR",
                self.utc_time,
                decimal.Decimal("0.91"),
                overwrite=True,
            )


if __name__ == "__main__":
    unittest.main()
