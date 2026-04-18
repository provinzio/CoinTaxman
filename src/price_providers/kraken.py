import bisect
import datetime
import decimal
import json
import time
from typing import Any

import requests

import log_config
import misc
from core import kraken_pair_map

from .base import PriceProvider

log = log_config.getLogger(__name__)


class KrakenPriceProvider(PriceProvider):
    kraken_invalid_pairs: list[str] = []

    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        minutes_step = kwargs.get("minutes_step", 10)

        target_timestamp = misc.to_ms_timestamp(utc_time)
        root_url = "https://api.kraken.com/0/public/Trades"
        inverse = False

        minutes_offset = 0
        while minutes_offset < 120:
            minutes_offset += minutes_step

            since = misc.to_ns_timestamp(
                utc_time - datetime.timedelta(minutes=minutes_offset)
            )

            num_retries = 10
            while num_retries:
                pair = base_asset + quote_asset
                pair = kraken_pair_map.get(pair, pair)

                if pair in self.kraken_invalid_pairs:
                    inverse = not inverse
                    base_asset, quote_asset = quote_asset, base_asset
                    pair = base_asset + quote_asset
                    pair = kraken_pair_map.get(pair, pair)
                    if pair in self.kraken_invalid_pairs:
                        raise RuntimeError(
                            f"Could not retrieve trades for {pair} or inverse pair."
                        )

                url = f"{root_url}?pair={pair}&since={since}"

                response = requests.get(url)
                response.raise_for_status()
                data = json.loads(response.text)

                if not data["error"]:
                    break
                if data["error"] == ["EGeneral:Invalid arguments"]:
                    self.kraken_invalid_pairs.append(pair)
                else:
                    num_retries -= 1
                    sleep_duration = 2 ** (10 - num_retries)
                    time.sleep(sleep_duration)
                    continue
            else:
                raise RuntimeError("Kraken response keeps having error flags.")

            data = data["result"][pair]
            data_timestamps_ms = [int(float(item[2]) * 1000) for item in data]
            closest_match_index = (
                bisect.bisect_left(data_timestamps_ms, target_timestamp) - 1
            )

            if closest_match_index == -1:
                continue

            if closest_match_index == len(data_timestamps_ms) - 1:
                if len(data_timestamps_ms) < 100:
                    now_timestamp = misc.to_ms_timestamp(
                        datetime.datetime.now().astimezone()
                    )
                    if target_timestamp < now_timestamp - 3600 * 1000:
                        log.warning(
                            "Timestamp for %s at %s is older than one hour.",
                            pair,
                            utc_time,
                        )
                elif minutes_step == 1:
                    break
                else:
                    return self.fetch_price(
                        base_asset,
                        utc_time,
                        quote_asset,
                        minutes_step=minutes_step - 1,
                    )

            price = misc.force_decimal(data[closest_match_index][0])
            if inverse:
                price = misc.reciprocal(price)
            return price

        log.warning("Failed to find matching exchange rate for %s at %s.", pair, utc_time)
        return decimal.Decimal()
