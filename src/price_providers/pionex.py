import datetime
import decimal
from typing import Any

import requests

import log_config
import misc

from .base import FallbackPriceNotFound, PriceProvider

log = log_config.getLogger(__name__)


class PionexPriceProvider(PriceProvider):
    STABLE_USD_ASSETS = {
        "USDT",
        "USDC",
        "BUSD",
        "FDUSD",
        "TUSD",
        "USDP",
        "PYUSD",
        "DAI",
    }

    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        swapped_symbols = kwargs.get("swapped_symbols", False)
        fallback_mode = kwargs.get("fallback_mode", False)

        # Pionex rarely has fiat pairs like USDT_EUR. Use USD as bridge for
        # stable USD assets so EUR-denominated tax values don't collapse to 0.
        if (
            base_asset in self.STABLE_USD_ASSETS
            and quote_asset in self.STABLE_USD_ASSETS
        ):
            return decimal.Decimal("1")
        if base_asset in self.STABLE_USD_ASSETS and quote_asset == "USD":
            return decimal.Decimal("1")
        if quote_asset in self.STABLE_USD_ASSETS and base_asset == "USD":
            return decimal.Decimal("1")
        if base_asset in self.STABLE_USD_ASSETS and quote_asset != "USD":
            try:
                usd_quote = self.get_price("kraken", "USD", utc_time, quote_asset)
            except Exception:
                usd_quote = decimal.Decimal()
            if usd_quote > 0:
                return usd_quote
        if quote_asset in self.STABLE_USD_ASSETS and base_asset != "USD":
            try:
                base_usd = self.get_price("kraken", base_asset, utc_time, "USD")
            except Exception:
                base_usd = decimal.Decimal()
            if base_usd > 0:
                return base_usd

        root_url = "https://api.pionex.com/api/v1/market/tickers"
        symbol = (
            f"{quote_asset}_{base_asset}"
            if swapped_symbols
            else f"{base_asset}_{quote_asset}"
        )
        if self.is_known_missing_symbol(symbol):
            if fallback_mode:
                raise FallbackPriceNotFound
            return decimal.Decimal()

        response = requests.get(root_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        tickers = []
        if isinstance(data, dict):
            tickers = data.get("data", {}).get("tickers", [])

        if not isinstance(tickers, list):
            raise RuntimeError(
                f"Unexpected Pionex response format for {symbol} at {utc_time}: {data}"
            )

        price = decimal.Decimal()
        for ticker in tickers:
            if ticker.get("symbol") == symbol:
                if "close" in ticker:
                    price = misc.force_decimal(ticker["close"])
                elif "last" in ticker:
                    price = misc.force_decimal(ticker["last"])
                break

        if price == 0:
            self.mark_missing_symbol(symbol)
            if not swapped_symbols:
                log.warning(
                    "Pionex symbol %s not found, retrying with swapped symbol %s.",
                    symbol,
                    f"{quote_asset}_{base_asset}",
                )
                return self.fetch_price(
                    base_asset,
                    utc_time,
                    quote_asset,
                    swapped_symbols=True,
                    fallback_mode=fallback_mode,
                )

            if fallback_mode:
                raise FallbackPriceNotFound

            fallback_assets = ["BTC", "USDT", "USDC", "ETH"]
            for fallback_asset in fallback_assets:
                if base_asset != fallback_asset and quote_asset != fallback_asset:
                    try:
                        base = self.get_price(
                            "pionex",
                            base_asset,
                            utc_time,
                            fallback_asset,
                            fallback_mode=True,
                        )
                        quote = self.get_price(
                            "pionex",
                            fallback_asset,
                            utc_time,
                            quote_asset,
                            fallback_mode=True,
                        )
                    except FallbackPriceNotFound:
                        continue
                    else:
                        return base * quote

            log.warning(
                f"Unable to retrieve price for {symbol=} from pionex at "
                f"{utc_time=} even though multiple fallback assets were checked. "
                "Set the price to 0 and consider adding a manual price entry."
            )
            return decimal.Decimal()

        if swapped_symbols:
            return misc.reciprocal(price)
        return price
