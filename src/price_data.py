# CoinTaxman
# Copyright (C) 2021  Carsten Docktor <https://github.com/provinzio>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import bisect
import datetime
import decimal
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, Union

import requests

import config
import misc
import transaction
from core import kraken_pair_map
from database import (
    get_db_path,
    get_price_db,
    get_tablename,
    mean_price_db,
    set_price_db,
)

log = logging.getLogger(__name__)


# TODO Keep database connection open?
# TODO Combine multiple exchanges in one file?
#      - Add a database for each exchange (added with ATTACH DATABASE)
#      - Tables in database stay the same


class PriceData:
    def get_db_path(self, platform: str) -> Path:
        return Path(config.DATA_PATH, f"{platform}.db")

    def get_tablename(self, coin: str, reference_coin: str) -> str:
        return f"{coin}/{reference_coin}"

    @misc.delayed
    def _get_price_binance(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        swapped_symbols: bool = False,
    ) -> decimal.Decimal:
        """Retrieve price from binance official REST API.

        The price is calculated as the average price in a
        time frame of 1 minute around `utc_time`.

        None existing pairs like `TWTEUR` are calculated as
        `TWTBTC * BTCEUR`.

        Documentation:
        https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md

        Args:
            base_asset (str)
            utc_time (datetime.datetime)
            quote_asset (str)
            swapped_symbols (bool, optional): The function is run with swapped
                                              asset symbols. Defaults to False.

        Raises:
            RuntimeError: Unable to retrieve price data.

        Returns:
            decimal.Decimal: Price of asset pair.
        """
        root_url = "https://api.binance.com/api/v3/aggTrades"
        symbol = f"{base_asset}{quote_asset}"
        startTime, endTime = misc.get_offset_timestamps(
            utc_time, datetime.timedelta(minutes=1)
        )
        url = f"{root_url}?{symbol=:}&{startTime=:}&{endTime=:}"

        log.debug("Calling %s", url)
        response = requests.get(url)
        data = json.loads(response.text)

        # Some combinations do not exist (e.g. `TWTEUR`), but almost anything
        # is paired with BTC. Calculate `TWTEUR` as `TWTBTC * BTCEUR`.
        if (
            isinstance(data, dict)
            and data.get("code") == -1121
            and data.get("msg") == "Invalid symbol."
        ):
            if quote_asset == "BTC":
                # If we are already comparing with BTC, we might have to swap
                # the assets to generate the correct symbol.
                # Check a last time, if we find the pair by changing the symbol
                # order.
                # If this does not help, we need to think of something else.
                if swapped_symbols:
                    raise RuntimeError(f"Can not retrieve {symbol=} from binance")
                # Changing the order of the assets require to
                # invert the price.
                price = self.get_price(
                    "binance", quote_asset, utc_time, base_asset, swapped_symbols=True
                )
                return misc.reciprocal(price)

            btc = self.get_price("binance", base_asset, utc_time, "BTC")
            quote = self.get_price("binance", "BTC", utc_time, quote_asset)
            return btc * quote

        response.raise_for_status()

        if len(data) == 0:
            log.warning(
                "Binance offers no price for %s at %s. Trying %s/USDT and %s/USDT",
                symbol,
                utc_time,
                base_asset,
                quote_asset,
            )
            if quote_asset == "USDT":
                return decimal.Decimal()
            quote = self.get_price("binance", quote_asset, utc_time, "USDT")
            if quote == 0.0:
                return quote
            usdt = self.get_price("binance", base_asset, utc_time, "USDT")
            return usdt / quote

        # Calculate average price.
        total_cost = decimal.Decimal()
        total_quantity = decimal.Decimal()
        for d in data:
            price = misc.force_decimal(d["p"])
            quantity = misc.force_decimal(d["q"])
            total_cost += price * quantity
            total_quantity += quantity
        average_price = total_cost / total_quantity
        return average_price

    @misc.delayed
    def _get_price_coinbase_pro(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        minutes_step: int = 5,
    ) -> decimal.Decimal:
        """Retrieve price from Coinbase Pro official REST API.

        Documentation: https://docs.pro.coinbase.com

        Args:
            base_asset (str): Base asset.
            utc_time (datetime.datetime): Target time (time of the trade).
            quote_asset (str): Quote asset.
            minutes_step (int): Initial time offset for consecutive
                                Coinbase Pro API requests. Defaults to 5.

        Returns:
            decimal.Decimal: Price of asset pair at target time
                   (0 if price couldn't be determined)
        """

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

            log.debug("Calling %s", url)
            log.debug(
                f"Querying Coinbase Pro candles for {pair} at {utc_time} "
                f"(offset={minutes_offset}m): Calling %s",
                url,
            )

            response = requests.get(url)
            response.raise_for_status()
            data = json.loads(response.text)

            # No candles within the time window
            if len(data) == 0:
                continue

            # Find closest timestamp match
            target_timestamp = misc.to_ms_timestamp(utc_time)
            data_timestamps_ms = [int(float(d[0]) * 1000) for d in data]
            data_timestamps_ms.reverse()  # bisect requires ascending order

            closest_match_index = (
                bisect.bisect_left(data_timestamps_ms, target_timestamp) - 1
            )

            # The desired timestamp is in the past
            if closest_match_index == -1:
                continue

            # The desired timestamp is in the future
            if closest_match_index == len(data_timestamps_ms) - 1:
                continue

            closest_match = data[closest_match_index]
            open_price = misc.force_decimal(closest_match[3])
            close_price = misc.force_decimal(closest_match[4])

            return (open_price + close_price) / 2

        log.warning(
            f"Querying Coinbase Pro candles for {pair} at {utc_time}: "
            f"Failed to find matching exchange rate. "
            "Please create an Issue or PR."
        )
        return decimal.Decimal()

    @misc.delayed
    def _get_price_bitpanda(
        self, base_asset: str, utc_time: datetime.datetime, quote_asset: str
    ) -> decimal.Decimal:
        return self._get_price_bitpanda_pro(base_asset, utc_time, quote_asset)

    @misc.delayed
    def _get_price_bitpanda_pro(
        self, base_asset: str, utc_time: datetime.datetime, quote_asset: str
    ) -> decimal.Decimal:
        """Retrieve the price from the Bitpanda Pro API.

        This uses the "candlestricks" API endpoint.
        It returns the highest and lowest price for the COIN in a given time frame.

        Timeframe ends at the requested time.

        Args:
            base_asset (str): The currency to get the price for.
            utc_time (datetime.datetime): Time of the trade to fetch the price for.
            quote_asset (str): The currency for the price.

        Returns:
            decimal.Decimal: Price of the asset pair.
        """

        baseurl = (
            f"https://api.exchange.bitpanda.com/public/v1/"
            f"candlesticks/{base_asset}_{quote_asset}"
        )

        # Bitpanda Pro only supports distinctive arguments for this, *not arbitrary*
        timeframes = [1, 5, 15, 30]

        # Try to find the price in the most detailed timeframe, to get the best matching
        # price for the transaction. If we can not find the price in a timeframe, use
        # the next bigger frame and try again. If we reached the highest timeframe, move
        # the fetched time window into the past, to get the latest transaction from the
        # API. If there were no trades in the requested time frame, the returned data
        # will be empty
        for t in timeframes:
            # Maximum number of allowed offsets into the past to find a valid price
            # before we throw an error. We do not offset the time window as long as we
            # can choose a bigger timeframe instead. num_max_offsets has been determined
            # empirically and may be changed.
            num_max_offsets = 12 if t == timeframes[-1] else 1
            for num_offset in range(num_max_offsets):
                # if no trades can be found, move 30 min window to the past
                window_offset = num_offset * t
                end = utc_time.astimezone(datetime.timezone.utc) - datetime.timedelta(
                    minutes=window_offset
                )
                begin = end - datetime.timedelta(minutes=t)

                # https://github.com/python/mypy/issues/3176
                params: dict[str, Union[int, str]] = {
                    "unit": "MINUTES",
                    "period": t,
                    # convert ISO 8601 format to RFC3339 timestamp
                    "from": begin.isoformat().replace("+00:00", "Z"),
                    "to": end.isoformat().replace("+00:00", "Z"),
                }
                if num_offset:
                    log.debug(
                        f"Calling Bitpanda API for {base_asset} / {quote_asset} price "
                        f"for {t} minute timeframe ending at {end} "
                        f"(includes {window_offset} minutes offset)"
                    )
                else:
                    log.debug(
                        f"Calling Bitpanda API for {base_asset} / {quote_asset} price "
                        f"for {t} minute timeframe ending at {end}"
                    )
                r = requests.get(baseurl, params=params)

                assert r.status_code == 200, "No valid response from Bitpanda API"
                data = r.json()

                # exit loop if data is valid
                if data:
                    break

                # issue warning if time window is moved to the past
                if num_offset < num_max_offsets - 1:
                    log.warning(
                        f"No price data found for {base_asset} / {quote_asset} "
                        f"at {end}, moving {t} minutes window to the past."
                    )

            # exit loop if data is valid
            if data:
                break
        else:
            log.error(
                f"No price data found for {base_asset} / {quote_asset} at {end}. "
                f"You can try to increase num_max_offsets to obtain older price data."
            )
            raise RuntimeError

        # this should never be triggered, but just in case assert received data
        assert data, f"No valid price data for {base_asset} / {quote_asset} at {end}"

        # simply take the average of the latest data element
        high = misc.force_decimal(data[-1]["high"])
        low = misc.force_decimal(data[-1]["low"])

        # if spread is greater than 3%
        if (high - low) / high > 0.03:
            log.warning(f"Price spread is greater than 3%! High: {high}, Low: {low}")
        return (high + low) / 2

    @misc.delayed
    def _get_price_kraken(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        minutes_step: int = 10,
    ) -> decimal.Decimal:
        """Retrieve price from Kraken official REST API.

        We select the data point closest to the desired timestamp (utc_time),
        but not newer than this timestamp.
        For this we fetch one chunk of the trade history, starting
        `minutes_step`minutes before this timestamp.
        We then walk through the history until the closest timestamp match is
        found. Otherwise, we start another 10 minutes earlier and try again.
        (Exiting with a warning and zero price after hitting the arbitrarily
        chosen offset limit of 120 minutes). If the initial offset is already
        too large, recursively retry by reducing the offset step,
        down to 1 minute.

        Documentation: https://www.kraken.com/features/api

        Args:
            base_asset (str): Base asset.
            utc_time (datetime.datetime): Target time (time of the trade).
            quote_asset (str): Quote asset.
            minutes_step (int): Initial time offset for consecutive
                                Kraken API requests. Defaults to 10.

        Returns:
            decimal.Decimal: Price of asset pair at target time
                   (0 if price couldn't be determined)
        """
        target_timestamp = misc.to_ms_timestamp(utc_time)
        root_url = "https://api.kraken.com/0/public/Trades"
        pair = base_asset + quote_asset
        pair = kraken_pair_map.get(pair, pair)

        minutes_offset = 0
        while minutes_offset < 120:
            minutes_offset += minutes_step

            since = misc.to_ns_timestamp(
                utc_time - datetime.timedelta(minutes=minutes_offset)
            )
            url = f"{root_url}?{pair=:}&{since=:}"

            num_retries = 10
            while num_retries:
                log.debug(
                    f"Querying trades for {pair} at {utc_time} "
                    f"(offset={minutes_offset}m): Calling %s",
                    url,
                )
                response = requests.get(url)
                response.raise_for_status()
                data = json.loads(response.text)

                if not data["error"]:
                    break
                else:
                    num_retries -= 1
                    sleep_duration = 2 ** (10 - num_retries)
                    log.warning(
                        f"Querying trades for {pair} at {utc_time} "
                        f"(offset={minutes_offset}m): "
                        f"Could not retrieve trades: {data['error']}. "
                        f"Retry in {sleep_duration} s ..."
                    )
                    time.sleep(sleep_duration)
                    continue
            else:
                log.error(
                    f"Querying trades for {pair} at {utc_time} "
                    f"(offset={minutes_offset}m): "
                    f"Could not retrieve trades: {data['error']}"
                )
                raise RuntimeError("Kraken response keeps having error flags.")

            # Find closest timestamp match
            data = data["result"][pair]
            data_timestamps_ms = [int(float(d[2]) * 1000) for d in data]
            closest_match_index = (
                bisect.bisect_left(data_timestamps_ms, target_timestamp) - 1
            )

            # The desired timestamp is in the past; increase the offset
            if closest_match_index == -1:
                continue

            # The desired timestamp is in the future
            if closest_match_index == len(data_timestamps_ms) - 1:

                if minutes_step == 1:
                    # Cannot remove interval any further; give up
                    break
                else:
                    # We missed the desired timestamp because our initial step
                    # size was too large; reduce step size
                    log.debug(
                        f"Querying trades for {pair} at {utc_time}: " "Reducing step"
                    )
                    return self._get_price_kraken(
                        base_asset, utc_time, quote_asset, minutes_step - 1
                    )

            price = misc.force_decimal(data[closest_match_index][0])
            return price

        log.warning(
            f"Querying trades for {pair} at {utc_time}: "
            f"Failed to find matching exchange rate. "
            "Please create an Issue or PR."
        )
        return decimal.Decimal()

    def get_price(
        self,
        platform: str,
        coin: str,
        utc_time: datetime.datetime,
        reference_coin: str = config.FIAT,
        **kwargs: Any,
    ) -> decimal.Decimal:
        """Get the price of a coin pair from a specific `platform` at `utc_time`.
        The function tries to retrieve the price from the local database first.
        If the price does not exist, its gathered from a platform specific
        function and saved to our local database for future access.
        Args:
            platform (str)
            coin (str)
            utc_time (datetime.datetime)
            reference_coin (str, optional): Defaults to config.FIAT.
        Raises:
            NotImplementedError: Platform specific GET function is not
                                    implemented.
        Returns:
            decimal.Decimal: Price of the coin pair.
        """
        if coin == reference_coin:
            return decimal.Decimal("1")

        db_path = get_db_path(platform)
        tablename = get_tablename(coin, reference_coin)

        # Check if price exists already in our database.
        if (price := get_price_db(db_path, tablename, utc_time)) is None:
            # Price doesn't exists. Fetch price from platform.
            try:
                get_price = getattr(self, f"_get_price_{platform}")
            except AttributeError:
                raise NotImplementedError("Unable to read data from %s", platform)

            price = get_price(coin, utc_time, reference_coin, **kwargs)
            assert isinstance(price, decimal.Decimal)
            set_price_db(
                platform, coin, reference_coin, utc_time, price, db_path=db_path
            )

        if config.MEAN_MISSING_PRICES and price <= 0.0:
            # The price is missing. Check for prices before and after the
            # transaction and estimate the price.
            # Do not save price in database.
            price = mean_price_db(db_path, tablename, utc_time)

        return price

    def get_cost(
        self,
        tr: Union[transaction.Operation, transaction.SoldCoin],
        reference_coin: str = config.FIAT,
    ) -> decimal.Decimal:
        op = tr if isinstance(tr, transaction.Operation) else tr.op
        price = self.get_price(op.platform, op.coin, op.utc_time, reference_coin)
        if isinstance(tr, transaction.Operation):
            return price * tr.change
        if isinstance(tr, transaction.SoldCoin):
            return price * tr.sold
        raise NotImplementedError

    def check_database(self):
        stats = {}

        for db_path in Path(config.DATA_PATH).glob("*.db"):
            if db_path.is_file():
                platform = db_path.stem
                stats[platform] = {"fix": 0, "rem": 0}
                try:
                    get_price = getattr(self, f"_get_price_{platform}")
                except AttributeError:
                    if platform == "coinbase":
                        get_price = self._get_price_coinbase_pro
                    else:
                        raise NotImplementedError(
                            "Unable to read data from %s", platform
                        )

                with sqlite3.connect(db_path) as conn:
                    query = "SELECT name FROM sqlite_master WHERE type='table'"
                    cur = conn.execute(query)
                    tablenames = (result[0] for result in cur.fetchall())
                    for tablename in tablenames:
                        base_asset, quote_asset = tablename.split("/")
                        query = f"SELECT utc_time FROM `{tablename}` WHERE price<=0.0;"
                        cur = conn.execute(query)

                        for row in cur.fetchall():
                            utc_time = datetime.datetime.strptime(
                                row[0], "%Y-%m-%d %H:%M:%S%z"
                            )
                            price = get_price(base_asset, utc_time, quote_asset)

                            if price == 0.0:
                                log.warning(
                                    f"""
                                    Could not fetch price for
                                    pair {tablename} on {platform} at {utc_time}
                                    """
                                )
                                stats[platform]["rem"] += 1
                            else:
                                log.info(
                                    f"Updating {tablename} at {utc_time} to {price}"
                                )
                                query = f"""
                                UPDATE `{tablename}`
                                SET price=?
                                WHERE utc_time=?;"""
                                conn.execute(query, (str(price), utc_time))
                                stats[platform]["fix"] += 1

                    conn.commit()

        log.info("Check Database Result:")
        for platform, result in stats.items():
            fixed, remaining = result.values()
            log.info(f"{platform}: {fixed} fixed, {remaining} remaining")
