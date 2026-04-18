import datetime
import decimal
import time
from typing import Any

import requests

import log_config
import misc

from .base import FallbackPriceNotFound, PriceProvider

log = log_config.getLogger(__name__)


class BitgetPriceProvider(PriceProvider):
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        swapped_symbols = kwargs.get("swapped_symbols", False)
        fallback_mode = kwargs.get("fallback_mode", False)
        minute_interval = kwargs.get("minute_interval", 1)

        assert base_asset != quote_asset

        root_url = "https://api.bitget.com/api/v2/spot/market/candles"
        symbol = (
            f"{quote_asset}{base_asset}"
            if swapped_symbols
            else f"{base_asset}{quote_asset}"
        )
        end = utc_time.astimezone(datetime.timezone.utc)
        start = end - datetime.timedelta(minutes=minute_interval)

        granularity_map = {
            1: "1min",
            3: "3min",
            5: "5min",
            15: "15min",
            30: "30min",
            60: "1h",
            240: "4h",
            360: "6h",
            720: "12h",
            1440: "1day",
        }
        granularity = granularity_map.get(minute_interval, "1min")

        params = {
            "symbol": symbol,
            "granularity": granularity,
            "startTime": str(int(start.timestamp() * 1000)),
            "endTime": str(int(end.timestamp() * 1000)),
        }

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.get(root_url, params=params, timeout=10)
                response.raise_for_status()
            except (
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ) as e:
                if attempt == max_retries - 1:
                    raise
                sleep_duration = 2 ** attempt
                log.warning(
                    "Bitget request failed for %s at %s (%s). Retrying in %s s...",
                    symbol,
                    utc_time,
                    e.__class__.__name__,
                    sleep_duration,
                )
                time.sleep(sleep_duration)
                continue
            except requests.exceptions.HTTPError as e:
                response_text = e.response.text if e.response is not None else ""
                response_data = {}
                try:
                    if e.response is not None:
                        response_data = e.response.json()
                except ValueError:
                    pass

                invalid_symbol_error = (
                    e.response is not None
                    and e.response.status_code == 400
                    and (
                        "does not exist" in response_text
                        or response_data.get("code") == "40034"
                        or response_data.get("msg", "").lower().startswith(
                            "parameter"
                        )
                    )
                )

                if invalid_symbol_error:
                    if not swapped_symbols:
                        return self.fetch_price(
                            base_asset,
                            utc_time,
                            quote_asset,
                            swapped_symbols=True,
                            fallback_mode=fallback_mode,
                            minute_interval=minute_interval,
                        )

                    data = []
                    break
                raise
            else:
                data = response.json()
                break
        else:
            raise RuntimeError("Bitget API request failed after retries.")

        if isinstance(data, dict) and data.get("code") == "00000":
            data = data.get("data", [])

        if not data:
            if fallback_mode:
                if swapped_symbols:
                    raise FallbackPriceNotFound
                price = self.get_price(
                    "bitget",
                    quote_asset,
                    utc_time,
                    base_asset,
                    swapped_symbols=True,
                    fallback_mode=fallback_mode,
                    minute_interval=minute_interval,
                )
                return misc.reciprocal(price)

            fallback_assets = ["BTC", "USDT", "USDC", "ETH"]
            for fallback_asset in fallback_assets:
                if base_asset != fallback_asset and quote_asset != fallback_asset:
                    try:
                        base = self.get_price(
                            "bitget",
                            base_asset,
                            utc_time,
                            fallback_asset,
                            fallback_mode=True,
                            minute_interval=minute_interval,
                        )
                        quote = self.get_price(
                            "bitget",
                            fallback_asset,
                            utc_time,
                            quote_asset,
                            fallback_mode=True,
                            minute_interval=minute_interval,
                        )
                    except FallbackPriceNotFound:
                        continue
                    else:
                        return base * quote

            log.warning(
                f"Unable to retrieve price for {symbol=} from bitget at "
                f"{utc_time=} even though multiple fallback assets were checked. "
                "Set the price to 0 and consider adding a manual price entry."
            )
            return decimal.Decimal()

        latest = data[-1]
        if isinstance(latest, dict):
            high = misc.force_decimal(latest.get("high", latest.get("highPrice")))
            low = misc.force_decimal(latest.get("low", latest.get("lowPrice")))
        elif len(latest) >= 5:
            high = misc.force_decimal(latest[2])
            low = misc.force_decimal(latest[3])
        else:
            raise RuntimeError(
                f"Unexpected Bitget candle format for {symbol} at {utc_time}: {latest}"
            )

        if high == 0:
            return decimal.Decimal()

        if (high - low) / high > 0.03:
            log.warning("Price spread is greater than 3%%! High: %s, Low: %s", high, low)

        price = (high + low) / 2
        if swapped_symbols:
            price = misc.reciprocal(price)
        return price
