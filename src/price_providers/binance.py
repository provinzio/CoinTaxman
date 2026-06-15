import datetime
import decimal
import json
from typing import Any

import requests

import log_config
import misc

from .base import FallbackPriceNotFound, PriceProvider

log = log_config.getLogger(__name__)


class BinancePriceProvider(PriceProvider):
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
        root_url = "https://api.binance.com/api/v3/aggTrades"
        symbol = f"{base_asset}{quote_asset}"
        start_time, end_time = misc.get_offset_timestamps(
            utc_time, datetime.timedelta(minutes=minute_interval)
        )
        url = f"{root_url}?symbol={symbol}&startTime={start_time}&endTime={end_time}"

        try:
            response = requests.get(url, timeout=10)
        except requests.exceptions.RequestException as e:
            if fallback_mode:
                raise FallbackPriceNotFound from e
            log.warning(
                "Unable to retrieve price for symbol=%s from binance at utc_time=%s due to %s.",
                symbol,
                utc_time,
                e.__class__.__name__,
            )
            return decimal.Decimal()

        data = json.loads(response.text)

        if (
            isinstance(data, dict)
            and data.get("code") == -1121
            and data.get("msg") == "Invalid symbol."
        ) or len(data) == 0:
            fallback_assets = ["BTC", "BNB", "BUSD", "USDT"]
            fallback_intervals = [1, 3, 5, 10, 15, 30, 60]

            if fallback_mode:
                if swapped_symbols:
                    raise FallbackPriceNotFound
                price = self.get_price(
                    "binance",
                    quote_asset,
                    utc_time,
                    base_asset,
                    swapped_symbols=True,
                    fallback_mode=fallback_mode,
                    minute_interval=minute_interval,
                )
                return misc.reciprocal(price)

            for fallback_interval in fallback_intervals:
                for fallback_asset in fallback_assets:
                    if base_asset != fallback_asset and quote_asset != fallback_asset:
                        try:
                            base = self.get_price(
                                "binance",
                                base_asset,
                                utc_time,
                                fallback_asset,
                                fallback_mode=True,
                                minute_interval=fallback_interval,
                            )
                            quote = self.get_price(
                                "binance",
                                fallback_asset,
                                utc_time,
                                quote_asset,
                                fallback_mode=True,
                                minute_interval=fallback_interval,
                            )
                        except FallbackPriceNotFound:
                            continue
                        else:
                            return base * quote

            log.warning(
                "Unable to retrieve price for symbol=%s from binance at utc_time=%s.",
                symbol,
                utc_time,
            )
            return decimal.Decimal()

        try:
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            if fallback_mode:
                raise FallbackPriceNotFound from e
            log.warning(
                "Unable to retrieve price for symbol=%s from binance at utc_time=%s due to %s.",
                symbol,
                utc_time,
                e.__class__.__name__,
            )
            return decimal.Decimal()

        total_cost = decimal.Decimal()
        total_quantity = decimal.Decimal()
        for item in data:
            price = misc.force_decimal(item["p"])
            quantity = misc.force_decimal(item["q"])
            total_cost += price * quantity
            total_quantity += quantity
        return total_cost / total_quantity
