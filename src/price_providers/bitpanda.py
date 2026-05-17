import datetime
import decimal
from typing import Any, Union

import requests

import log_config
import misc

from .base import PriceProvider

log = log_config.getLogger(__name__)


class BitpandaProPriceProvider(PriceProvider):
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        baseurl = (
            "https://api.exchange.bitpanda.com/public/v1/"
            f"candlesticks/{base_asset}_{quote_asset}"
        )

        timeframes = [1, 5, 15, 30]

        for timeframe in timeframes:
            num_max_offsets = 12 if timeframe == timeframes[-1] else 1
            for num_offset in range(num_max_offsets):
                window_offset = num_offset * timeframe
                end = utc_time.astimezone(datetime.timezone.utc) - datetime.timedelta(
                    minutes=window_offset
                )
                begin = end - datetime.timedelta(minutes=timeframe)

                params: dict[str, Union[int, str]] = {
                    "unit": "MINUTES",
                    "period": timeframe,
                    "from": begin.isoformat().replace("+00:00", "Z"),
                    "to": end.isoformat().replace("+00:00", "Z"),
                }

                response = requests.get(baseurl, params=params)
                assert response.status_code == 200, "No valid response from Bitpanda API"
                data = response.json()

                if data:
                    break

                if num_offset < num_max_offsets - 1:
                    log.warning(
                        "No price data found for %s / %s at %s.",
                        base_asset,
                        quote_asset,
                        end,
                    )
            if data:
                break
        else:
            raise RuntimeError(
                f"No price data found for {base_asset} / {quote_asset}."
            )

        high = misc.force_decimal(data[-1]["high"])
        low = misc.force_decimal(data[-1]["low"])

        if (high - low) / high > 0.03:
            log.warning("Price spread is greater than 3%%! High: %s, Low: %s", high, low)
        return (high + low) / 2


class BitpandaPriceProvider(BitpandaProPriceProvider):
    pass
