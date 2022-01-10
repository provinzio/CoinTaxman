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
import math
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional, Union

import ccxt
import requests

import config
import graph
import misc
import transaction
from core import kraken_pair_map

log = logging.getLogger(__name__)


# TODO Keep database connection open?
# TODO Combine multiple exchanges in one file?
#      - Add a database for each exchange (added with ATTACH DATABASE)
#      - Tables in database stay the same


class PriceData:
    def __init__(self):
        self.path = graph.PricePath()

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
    def _get_price_bitpanda_pro(
        self, base_asset: str, utc_time: datetime.datetime, quote_asset: str
    ) -> decimal.Decimal:
        """Retrieve the price from the Bitpanda Pro API.

        This uses the "candlestricks" API endpoint.
        It returns the highest and lowest price for the COIN in a given time frame.

        Timeframe ends at the requested time.

        Currently, only BEST_EUR is tested.

        Args:
            base_asset (str): The currency to get the price for.
            utc_time (datetime.datetime): Time of the trade to fetch the price for.
            quote_asset (str): The currency for the price.

        Returns:
            decimal.Decimal: Price of the asset pair.
        """

        # other combination should not occur, since I enter them within the trade
        # other pairs need to be tested. Also, they might need different behavior,
        # if there isn't a matching endpoint
        assert (
            base_asset == "BEST" and quote_asset == "EUR"
        ), f"{base_asset}_{quote_asset}"
        baseurl = "https://api.exchange.bitpanda.com/public/v1/candlesticks/BEST_EUR"

        # Bitpanda Pro only supports distinctive arguments for this, *not arbitrary*
        timeframes = [1, 5, 15, 30]

        # get the smallest timeframe possible
        # if there were no trades in the requested time frame, the
        # returned data will be empty
        for t in timeframes:
            end = utc_time
            begin = utc_time - datetime.timedelta(minutes=t)

            # https://github.com/python/mypy/issues/3176
            params: dict[str, Union[int, str]] = {
                "unit": "MINUTES",
                "period": t,
                "from": begin.isoformat(),
                "to": end.isoformat(),
            }
            r = requests.get(baseurl, params=params)

            assert r.status_code == 200

            data = r.json()
            if data:
                break

        # if we didn't get data for the 30 minute frame, give up?
        assert data
        # There actually shouldn't be more than one entry if period and granularity are
        # the same?
        assert len(data) == 1

        # simply take the average
        high = misc.force_decimal(data[0]["high"])
        low = misc.force_decimal(data[0]["low"])

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

    def __get_price_db(
        self,
        db_path: Path,
        tablename: str,
        utc_time: datetime.datetime,
    ) -> Optional[decimal.Decimal]:
        """Try to retrieve the price from our local database.

        Args:
            db_path (Path)
            tablename (str)
            utc_time (datetime.datetime)

        Returns:
            Optional[decimal.Decimal]: Price.
        """
        if db_path.is_file():
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()
                query = f"SELECT price FROM `{tablename}` WHERE utc_time=?;"

                try:
                    cur.execute(query, (utc_time,))
                except sqlite3.OperationalError as e:
                    if str(e) == f"no such table: {tablename}":
                        return None
                    raise e

                if prices := cur.fetchone():
                    return misc.force_decimal(prices[0])

        return None

    def __mean_price_db(
        self,
        db_path: Path,
        tablename: str,
        utc_time: datetime.datetime,
    ) -> decimal.Decimal:
        """Try to retrieve the price right before and after `utc_time`
        from our local database.

        Return 0 if the price could not be estimated.
        The function does not check, if a price for `utc_time` exists.

        Args:
            db_path (Path)
            tablename (str)
            utc_time (datetime.datetime)

        Returns:
            decimal.Decimal: Price.
        """
        if db_path.is_file():
            with sqlite3.connect(db_path) as conn:
                cur = conn.cursor()

                before_query = (
                    f"SELECT utc_time, price FROM `{tablename}` "
                    f"WHERE utc_time<? AND price > 0 "
                    "ORDER BY utc_time DESC "
                    "LIMIT 1"
                )
                try:
                    cur.execute(before_query, (utc_time,))
                except sqlite3.OperationalError as e:
                    if str(e) == f"no such table: {tablename}":
                        return decimal.Decimal()
                    raise e
                if result := cur.fetchone():
                    before_time = misc.parse_iso_timestamp_to_decimal_timestamp(
                        result[0]
                    )
                    before_price = misc.force_decimal(result[1])
                else:
                    return decimal.Decimal()

                after_query = (
                    f"SELECT utc_time, price FROM `{tablename}` "
                    f"WHERE utc_time>? AND price > 0 "
                    "ORDER BY utc_time ASC "
                    "LIMIT 1"
                )
                try:
                    cur.execute(after_query, (utc_time,))
                except sqlite3.OperationalError as e:
                    if str(e) == f"no such table: {tablename}":
                        return decimal.Decimal()
                    raise e
                if result := cur.fetchone():
                    after_time = misc.parse_iso_timestamp_to_decimal_timestamp(
                        result[0]
                    )
                    after_price = misc.force_decimal(result[1])
                else:
                    return decimal.Decimal()

                if before_price and after_price:
                    d_utc_time = misc.to_decimal_timestamp(utc_time)
                    # Linear gradiant between the neighbored transactions.
                    m = (after_price - before_price) / (after_time - before_time)
                    price = before_price + (d_utc_time - before_time) * m
                    return price

        return decimal.Decimal()

    def __set_price_db(
        self,
        db_path: Path,
        tablename: str,
        utc_time: datetime.datetime,
        price: decimal.Decimal,
    ) -> None:
        """Write price to database.

        Create database/table if necessary.

        Args:
            db_path (Path)
            tablename (str)
            utc_time (datetime.datetime)
            price (decimal.Decimal)
        """
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            query = f"INSERT INTO `{tablename}`" "('utc_time', 'price') VALUES (?, ?);"
            try:
                cur.execute(query, (utc_time, str(price)))
            except sqlite3.OperationalError as e:
                if str(e) == f"no such table: {tablename}":
                    create_query = (
                        f"CREATE TABLE `{tablename}`"
                        "(utc_time DATETIME PRIMARY KEY, "
                        "price FLOAT NOT NULL);"
                    )
                    cur.execute(create_query)
                    cur.execute(query, (utc_time, str(price)))
                else:
                    raise e
            conn.commit()

    def set_price_db(
        self,
        platform: str,
        coin: str,
        reference_coin: str,
        utc_time: datetime.datetime,
        price: decimal.Decimal,
    ) -> None:
        """Write price to database.

        Tries to insert a historical price into the local database.

        A warning will be raised, if there is already a different price.

        Args:
            platform (str): [description]
            coin (str): [description]
            reference_coin (str): [description]
            utc_time (datetime.datetime): [description]
            price (decimal.Decimal): [description]
        """
        assert coin != reference_coin
        db_path = self.get_db_path(platform)
        tablename = self.get_tablename(coin, reference_coin)
        try:
            self.__set_price_db(db_path, tablename, utc_time, price)
        except sqlite3.IntegrityError as e:
            if str(e) == f"UNIQUE constraint failed: {tablename}.utc_time":
                price_db = self.get_price(platform, coin, utc_time, reference_coin)
                if price != price_db:
                    rel_error = abs(price - price_db) / price * 100
                    log.warning(
                        f"Tried to write {tablename} price to database, but a "
                        f"different price exists already ({platform} @ {utc_time})"
                    )
                    log.warning(
                        f"price: {price}, database price: {price_db}, "
                        f"relative error: %.6f %%", rel_error
                    )
            else:
                raise e

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

        db_path = self.get_db_path(platform)
        tablename = self.get_tablename(coin, reference_coin)

        # Check if price exists already in our database.
        if (price := self.__get_price_db(db_path, tablename, utc_time)) is None:
            try:
                get_price = getattr(self, f"_get_price_{platform}")
            except AttributeError:
                raise NotImplementedError("Unable to read data from %s", platform)

            price = get_price(coin, utc_time, reference_coin, **kwargs)
            assert isinstance(price, decimal.Decimal)
            self.__set_price_db(db_path, tablename, utc_time, price)

        if config.MEAN_MISSING_PRICES and price <= 0.0:
            # The price is missing. Check for prices before and after the
            # transaction and estimate the price.
            # Do not save price in database.
            price = self.__mean_price_db(db_path, tablename, utc_time)

        return price

    def get_missing_price_operations(
        self,
        operations: list[transaction.Operation],
        coin: str,
        platform: str,
        reference_coin: str = config.FIAT,
    ) -> list[transaction.Operation]:
        """Return operations for which no price was found in the database.

        Requires the `operations` to have the same `coin` and `platform`.

        Args:
            operations (list[transaction.Operation])
            coin (str)
            platform (str)
            reference_coin (str): Defaults to `config.FIAT`.

        Returns:
            list[transaction.Operation]
        """
        assert all(op.coin == coin for op in operations)
        assert all(op.platform == platform for op in operations)

        # We do not have to calculate the price, if there are no operations or the
        # coin is the same as the reference coin.
        if not operations or coin == reference_coin:
            return []

        db_path = self.get_db_path(platform)
        # If the price database does not exist, we need to query all prices.
        if not db_path.is_file():
            return operations

        tablename = self.get_tablename(coin, reference_coin)
        utc_time_values = ",".join(f"('{op.utc_time}')" for op in operations)

        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            # The query returns a list with 0 and 1's.
            # - 0: a price exists.
            # - 1: the price is missing.
            query = (
                "SELECT t.utc_time IS NULL "
                f"FROM (VALUES {utc_time_values}) "
                f"LEFT JOIN `{tablename}` t ON t.utc_time = COLUMN1;"
            )

            # Execute the query.
            try:
                cur.execute(query)
            except sqlite3.OperationalError as e:
                if str(e) == f"no such table: {tablename}":
                    # The corresponding price table does not exist yet.
                    # We need to query all prices.
                    return operations
                raise e

            # Evaluate the result.
            result = (bool(is_missing) for is_missing, in cur.fetchall())
            missing_prices_operations = [
                op for op, is_missing in zip(operations, result) if is_missing
            ]
            return missing_prices_operations

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

    def get_candles(
        self, start: int, stop: int, symbol: str, exchange_id: str
    ) -> list[tuple[int, float, float, float, float, float]]:
        """Return list with candles starting 2 minutes before start.

        Args:
            start (int): Start time in milliseconds since epoch.
            stop (int): End time in milliseconds since epoch.
            symbol (str)
            exchange_id (str)

        Returns:
            list: List of OHLCV candles gathered from ccxt containing:

                timestamp (int): Timestamp of candle in milliseconds since epoch.
                open_price (float)
                lowest_price (float)
                highest_price (float)
                close_price (float)
                volume (float)
        """
        assert stop >= start, f"`stop` must be after `start` {stop} !>= {start}."

        exchange_class = getattr(ccxt, exchange_id)
        exchange = exchange_class()
        assert isinstance(exchange, ccxt.Exchange)

        # Technically impossible. Unsupported exchanges should be detected earlier.
        assert exchange.has["fetchOHLCV"]

        # time.sleep wants seconds
        self.path.RateLimit.limit(exchange)

        # Get candles 2 min before and after start/stop.
        since = start - 2 * 60 * 1000
        # `fetch_ohlcv` has no stop value but only a limit (amount of candles fetched).
        # Calculate the amount of candles in the 1 min timeframe,
        # so that we get enough candles.
        # Most exchange have an upper limit (e.g. binance 1000, coinbasepro 300).
        # `ccxt` throws an error if we exceed this limit.
        limit = math.ceil((stop - start) / (1000 * 60)) + 2
        try:
            candles = exchange.fetch_ohlcv(symbol, "1m", since, limit)
        except ccxt.RateLimitExceeded:
            # sometimes the ratelimit gets exceeded for kraken dunno why
            logging.warning("Ratelimit exceeded sleeping 10 seconds and retrying")
            time.sleep(10)
            self.path.RateLimit.limit(exchange)
            candles = exchange.fetch_ohlcv(symbol, "1m", since, limit)

        assert isinstance(candles, list)
        return candles

    def get_avg_candle_prices(
        self, start: int, stop: int, symbol: str, exchange_id: str, invert: bool = False
    ) -> list[tuple[int, decimal.Decimal]]:
        """Return average price from ohlcv candles.

        The average price of the candle is calculated as the average from the
        open and close price.

        Further information about candle-function can be found in `get_candles`.

        Args:
            start (int)
            stop (int)
            symbol (str)
            exchange_id (str)
            invert (bool, optional): Defaults to False.

        Returns:
            list: Timestamp and average prices of candles containing:

                timestamp (int): Timestamp of candle in milliseconds since epoch.
                avg_price (decimal.Decimal): Average price of candle.
        """
        avg_candle_prices = []
        candle_prices = self.get_candles(start, stop, symbol, exchange_id)

        for timestamp_ms, _open, _high, _low, _close, _volume in candle_prices:
            open = misc.force_decimal(_open)
            close = misc.force_decimal(_close)

            avg_price = (open + close) / 2

            if invert and avg_price != 0:
                avg_price = 1 / avg_price

            avg_candle_prices.append((timestamp_ms, avg_price))
        return avg_candle_prices

    # TODO preferredexchange default is only for debug purposes and should be
    #      removed later on.
    def _get_bulk_pair_data_path(
        self,
        operations: list,
        coin: str,
        reference_coin: str,
        preferredexchange: str = "binance",
    ) -> list:
        def merge_prices(a: list, b: Optional[list] = None) -> list:
            if not b:
                return a

            prices = []
            for i in a:
                factor = next(j[1] for j in b if i[0] == j[0])
                prices.append((i[0], i[1] * factor))

            return prices

        # TODO Set `max_difference` to the platform specific ohlcv-limit.
        max_difference = 300  # coinbasepro
        # TODO Set `max_size` to the platform specific ohlcv-limit.
        max_size = 300  # coinbasepro
        time_batches = transaction.time_batches(
            operations, max_difference=max_difference, max_size=max_size
        )

        datacomb = []

        for batch in time_batches:
            # ccxt works with timestamps in milliseconds
            first = misc.to_ms_timestamp(batch[0])
            last = misc.to_ms_timestamp(batch[-1])
            firststr = batch[0].strftime("%d-%b-%Y (%H:%M)")
            laststr = batch[-1].strftime("%d-%b-%Y (%H:%M)")
            log.info(
                f"getting data from {str(firststr)} to {str(laststr)} for {str(coin)}"
            )
            path = self.path.get_path(
                coin, reference_coin, first, last, preferredexchange=preferredexchange
            )
            # Todo Move the path calculation out of the for loop
            # and only filter after time
            for p in path:
                tempdatalis: list = []
                printstr = [f"{a[1]['symbol']} ({a[1]['exchange']})" for a in p[1]]
                log.debug(f"found path over {' -> '.join(printstr)}")
                for i in range(len(p[1])):
                    tempdatalis.append([])
                    symbol = p[1][i][1]["symbol"]
                    exchange = p[1][i][1]["exchange"]
                    invert = p[1][i][1]["inverted"]

                    if tempdata := self.get_avg_candle_prices(
                        first, last, symbol, exchange, invert
                    ):
                        for operation in batch:
                            # TODO discuss which candle is picked
                            # current is closest to original date
                            # (often off by about 1-20s, but can be after the Trade)
                            # times do not always line up perfectly so take one nearest
                            ts = list(
                                map(
                                    lambda x: (
                                        abs(misc.to_ms_timestamp(operation) - x[0]),
                                        x,
                                    ),
                                    tempdata,
                                )
                            )
                            tempdatalis[i].append(
                                (operation, min(ts, key=lambda x: x[0])[1][1])
                            )
                    else:
                        tempdatalis = []
                        # do not try already failed again
                        self.path.change_prio(printstr, 0.2)
                        break
                if tempdatalis:
                    wantedlen = len(tempdatalis[0])
                    for li in tempdatalis:
                        if not len(li) == wantedlen:
                            self.path.change_prio(printstr, 0.2)
                            break
                    else:
                        prices: list = []
                        for d in tempdatalis:
                            prices = merge_prices(d, prices)
                        datacomb.extend(prices)
                        break
                log.debug("path failed trying new path")

        return datacomb

    def preload_prices(
        self,
        operations: list[transaction.Operation],
        coin: str,
        platform: str,
        reference_coin: str = config.FIAT,
    ) -> None:
        """Preload price data.

        Requires the operations to have the same `coin` and `exchange`.

        Args:
            operations (list[transaction.Operation])
            coin (str)
            platform (str)
            reference_coin (str): Defaults to `config.FIAT`.
        """
        assert all(op.coin == coin for op in operations)
        assert all(op.platform == platform for op in operations)

        # We do not have to preload prices, if there are no operations or the coin is
        # the same as the reference coin.
        if not operations or coin == reference_coin:
            return

        if platform == "kraken":
            log.warning(
                f"Will not preload prices for {platform}, reverting to default API."
            )
            return

        # Only consider the operations for which we have no prices in the database.
        missing_prices_operations = self.get_missing_price_operations(
            operations, coin, platform, reference_coin
        )

        # Preload the prices.
        if missing_prices_operations:
            data = self._get_bulk_pair_data_path(
                missing_prices_operations,
                coin,
                reference_coin,
                preferredexchange=platform,
            )

            # TODO Use bulk insert to write all prices at once into the database.
            for p in data:
                self.set_price_db(platform, coin, reference_coin, p[0], p[1])

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
