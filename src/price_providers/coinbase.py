import bisect
import datetime
import decimal
import json
from typing import Any

import requests

import log_config
import misc

from .base import PriceProvider

log = log_config.getLogger(__name__)


class CoinbaseProPriceProvider(PriceProvider):
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        minutes_step = kwargs.get("minutes_step", 5)

        root_url = "https://api.pro.coinbase.com"
        pair = f"{base_asset}-{quote_asset}"

        minutes_offset = 0
        while minutes_offset < 120:
            minutes_offset += minutes_step

            start = misc.to_iso_timestamp(
                utc_time - datetime.timedelta(minutes=minutes_offset)
            )
            end = misc.to_iso_timestamp(
                utc_time + datetime.timedelta(minutes=minutes_offset)
            )
            params = f"start={start}&end={end}&granularity=60"
            url = f"{root_url}/products/{pair}/candles?{params}"

            response = requests.get(url)
            response.raise_for_status()
            data = json.loads(response.text)

            if len(data) == 0:
                continue

            target_timestamp = misc.to_ms_timestamp(utc_time)
            data_timestamps_ms = [int(float(d[0]) * 1000) for d in data]
            data_timestamps_ms.reverse()

            closest_match_index = (
                bisect.bisect_left(data_timestamps_ms, target_timestamp) - 1
            )

            if closest_match_index == -1:
                continue

            if closest_match_index == len(data_timestamps_ms) - 1:
                continue

            closest_match = data[closest_match_index]
            open_price = misc.force_decimal(closest_match[3])
            close_price = misc.force_decimal(closest_match[4])

            return (open_price + close_price) / 2

        log.warning(
            "Querying Coinbase Pro candles for %s at %s failed.",
            pair,
            utc_time,
        )
        return decimal.Decimal()


class CoinbasePriceProvider(CoinbaseProPriceProvider):
    pass
