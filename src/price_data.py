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
import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional, Union

import requests

import config
from core import kraken_pair_map
import misc
from transaction import *

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
    def _get_price_binance(self, base_asset: str, utc_time: datetime.datetime, quote_asset: str, swapped_symbols: bool = False) -> float:
        """Retrieve price from binance official REST API.

        The price is calculated as the average price in a
        time frame of 1 minute around `utc_time`.

        None existing pairs like `TWTEUR` are calculated as
        `TWTBTC * BTCEUR`.

        Documentation: https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md

        Args:
            base_asset (str)
            utc_time (datetime.datetime)
            quote_asset (str)
            swapped_symbols (bool, optional): The function is run with swapped asset symbols.
                                              Defaults to False.

        Raises:
            RuntimeError: Unable to retrieve price data.

        Returns:
            float: Price of asset pair.
        """
        root_url = "https://api.binance.com/api/v3/aggTrades"
        symbol = f"{base_asset}{quote_asset}"
        startTime, endTime = misc.get_offset_timestamps(
            utc_time, datetime.timedelta(minutes=1))
        url = f"{root_url}?{symbol=:}&{startTime=:}&{endTime=:}"

        log.debug("Calling %s", url)
        response = requests.get(url)
        data = json.loads(response.text)

        # Some combinations do not exist (e.g. `TWTEUR`), but almost anything
        # is paired with BTC. Calculate `TWTEUR` as `TWTBTC * BTCEUR`.
        if isinstance(data, dict) and data.get("code") == -1121 and data.get("msg") == "Invalid symbol.":
            if quote_asset == "BTC":
                # If we are already comparing with BTC, we might have to swap
                # the assets to generate the correct symbol.
                # Check a last time, if we find the pair by changing the symbol
                # order.
                # If this does not help, we need to think of something else.
                if swapped_symbols:
                    raise RuntimeError(
                        f"Can not retrieve {symbol=} from binance")
                # Changeing the order of the assets require to invert the price.
                price = self.get_price(
                    "binance", quote_asset, utc_time, base_asset, swapped_symbols=True)
                return 0 if price == 0 else 1 / price

            btc = self.get_price("binance", base_asset, utc_time, "BTC")
            quote = self.get_price("binance", "BTC", utc_time, quote_asset)
            return btc * quote

        response.raise_for_status()

        if len(data) == 0:
            log.warning(
                "Binance offers no price for `%s` at %s", symbol, utc_time)
            return 0

        # Calculate average price.
        total_cost = 0.0
        total_quantity = 0.0
        for d in data:
            price = float(d["p"])
            quantity = float(d["q"])
            total_cost += price * quantity
            total_quantity += quantity
        average_price = total_cost / total_quantity
        return average_price

    @misc.delayed
    def _get_price_kraken(self, base_asset: str, utc_time: datetime.datetime, quote_asset: str, minutes_step: int = 10) -> float:
        """Retrieve price from Kraken official REST API.

        We select the data point closest to the desired timestamp (utc_time), but not newer than this timestamp.
        For this we fetch one chunk of the trade history, starting `minutes_step` minutes before this timestamp.
        We then walk through the history until the closest timestamp match is found.
        Otherwise, we start another 10 minutes earlier and try again, etc. …
        (Exiting with a warning and zero price after hitting the arbitrarily chosen offset limit of 120 minutes.)
        If the initial offset is already too large, recursively retry by reducing the offset step, down to 1 minute.

        Documentation: https://www.kraken.com/features/api

        Args:
            base_asset (str): Base asset.
            utc_time (datetime.datetime): Target time (time of the trade).
            quote_asset (str): Quote asset.
            minutes_step (int): Initial time offset for consecutive Kraken API requests. Defaults to 10.

        Returns:
            float: Price of asset pair at target time (0 if price couldn't be determined)
        """
        target_timestamp = misc.to_ms_timestamp(utc_time)
        root_url = "https://api.kraken.com/0/public/Trades"
        pair = base_asset + quote_asset
        pair = kraken_pair_map.get(pair, pair)

        minutes_offset = 0
        while minutes_offset < 120:
            minutes_offset += minutes_step

            since = misc.to_ns_timestamp(utc_time - datetime.timedelta(minutes=minutes_offset))
            url = f"{root_url}?{pair=:}&{since=:}"

            log.debug(f"Querying trades for {pair} at {utc_time} (offset={minutes_offset}m): Calling %s", url)
            response = requests.get(url)
            response.raise_for_status()
            data = json.loads(response.text)

            if data["error"]:
                log.warning(f"Querying trades for {pair} at {utc_time} (offset={minutes_offset}m): "
                            f"Could not retrieve trades: {data['error']}")
                return 0

            # Find closest timestamp match
            data = data["result"][pair]
            data_timestamps_ms = [int(float(d[2]) * 1000) for d in data]
            closest_match_index = bisect.bisect_left(data_timestamps_ms, target_timestamp) - 1

            # The desired timestamp is in the past; increase the offset
            if closest_match_index == - 1:
                continue

            # The desired timestamp is in the future
            if closest_match_index == len(data_timestamps_ms) - 1:

                if minutes_step == 1:
                    # Cannot remove interval any further; give up
                    break
                else:
                    # We missed the desired timestamp because our initial step size was too large; reduce step size
                    log.debug(f"Querying trades for {pair} at {utc_time}: Reducing step")
                    return self._get_price_kraken(base_asset, utc_time, quote_asset, minutes_step - 1)

            price = float(data[closest_match_index][0])
            return price

        log.warning(f"Querying trades for {pair} at {utc_time}: "
                    f"Failed to find matching exchange rate. Please create an Issue or PR.")
        return 0

    def __get_price_db(self, db_path: Path, tablename: str, utc_time: datetime.datetime) -> Optional[float]:
        """Try to retrieve the price from our local database.

        Args:
            db_path (Path)
            tablename (str)
            utc_time (datetime.datetime)

        Returns:
            Optional[float]: Price.
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

                if price := cur.fetchone():
                    return price[0]

        return None

    def __set_price_db(self, db_path: Path, tablename: str, utc_time: datetime.datetime, price: float) -> None:
        """Write price to database.

        Create database/table if necessary.

        Args:
            db_path (Path)
            tablename (str)
            utc_time (datetime.datetime)
            price (float)
        """
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            query = f"INSERT INTO `{tablename}` ('utc_time', 'price') VALUES (?, ?);"
            try:
                cur.execute(query, (utc_time, price))
            except sqlite3.OperationalError as e:
                if str(e) == f"no such table: {tablename}":
                    create_query = f"CREATE TABLE `{tablename}` (utc_time DATETIME PRIMARY KEY, price FLOAT NOT NULL);"
                    cur.execute(create_query)
                    cur.execute(query, (utc_time, price))
                else:
                    raise e
            conn.commit()

    def set_price_db(self, platform: str, coin: str, reference_coin: str, utc_time: datetime.datetime, price: float) -> None:
        """Write price to database.

        Tries to insert a historical price into the local database.

        A warning will be raised, if there is already a different price.

        Args:
            platform (str): [description]
            coin (str): [description]
            reference_coin (str): [description]
            utc_time (datetime.datetime): [description]
            price (float): [description]
        """
        assert coin != reference_coin
        db_path = self.get_db_path(platform)
        tablename = self.get_tablename(coin, reference_coin)
        try:
            self.__set_price_db(db_path, tablename, utc_time, price)
        except sqlite3.IntegrityError as e:
            if str(e) == f"UNIQUE constraint failed: {tablename}.utc_time":
                price_db = self.get_price(
                    platform, coin, utc_time, reference_coin)
                if price != price_db:
                    log.warning(
                        "Tried to write price to database, but a different price exists already."
                        f"({platform=}, {tablename=}, {utc_time=}, {price=})"
                    )
            else:
                raise e

    def get_price(self, platform: str, coin: str, utc_time: datetime.datetime, reference_coin: str = config.FIAT, **kwargs) -> float:
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
            NotImplementedError: Platform specific GET function is not implemented.

        Returns:
            float: Price of the coin pair.
        """
        if coin == reference_coin:
            return 1.0

        db_path = self.get_db_path(platform)
        tablename = self.get_tablename(coin, reference_coin)

        # Check if price exists already in our database.
        if (price := self.__get_price_db(db_path, tablename, utc_time)) is not None:
            return price

        try:
            get_price = getattr(self, f"_get_price_{platform}")
        except AttributeError:
            raise NotImplementedError(
                "Unable to read data from %s", platform)

        price = get_price(coin, utc_time, reference_coin, **kwargs)
        self.__set_price_db(db_path, tablename, utc_time, price)
        return price

    def get_cost(self, tr: Union[Operation, SoldCoin], reference_coin: str = config.FIAT) -> float:
        op = tr if isinstance(tr, Operation) else tr.op
        price = self.get_price(op.platform, op.coin,
                               op.utc_time, reference_coin)
        if isinstance(tr, Operation):
            return price * tr.change
        if isinstance(tr, SoldCoin):
            return price * tr.sold
        raise NotImplementedError
