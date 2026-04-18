import datetime
import decimal
from typing import Any

import requests

import log_config
import misc

from .base import FallbackPriceNotFound, PriceProvider

log = log_config.getLogger(__name__)


class BitunixPriceProvider(PriceProvider):
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        swapped_symbols = kwargs.get("swapped_symbols", False)
        fallback_mode = kwargs.get("fallback_mode", False)

        root_url = "https://api.bitunix.com/api/v1/market/tickers"
        symbol = (
            f"{quote_asset}_{base_asset}"
            if swapped_symbols
            else f"{base_asset}_{quote_asset}"
        )

        try:
            response = requests.get(
                root_url,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                log.warning(
                    "Bitunix API is blocked or restricted, falling back to Binance for %s.",
                    symbol,
                )
                return self.get_price(
                    "binance",
                    base_asset,
                    utc_time,
                    quote_asset,
                    swapped_symbols=swapped_symbols,
                    fallback_mode=fallback_mode,
                )
            raise

        data = response.json()
        tickers = []
        if isinstance(data, dict):
            tickers = data.get("data", {}).get("tickers", [])

        if not isinstance(tickers, list):
            raise RuntimeError(
                f"Unexpected Bitunix response format for {symbol} at {utc_time}: {data}"
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
            if not swapped_symbols:
                log.warning(
                    "Bitunix symbol %s not found, retrying with swapped symbol %s.",
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
                            "bitunix",
                            base_asset,
                            utc_time,
                            fallback_asset,
                            fallback_mode=True,
                        )
                        quote = self.get_price(
                            "bitunix",
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
                f"Unable to retrieve price for {symbol=} from bitunix at "
                f"{utc_time=} even though multiple fallback assets were checked. "
                "Set the price to 0 and consider adding a manual price entry."
            )
            return decimal.Decimal()

        if swapped_symbols:
            return misc.reciprocal(price)
        return price
