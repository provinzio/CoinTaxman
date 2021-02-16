# CoinTaxman
# Copyright (C) 2021  Carsten Docktor

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

import datetime
import json
import logging
import sqlite3
from pathlib import Path
from typing import Optional, Union

import requests

from book import Book
import config
import misc
from transaction import *

log = logging.getLogger(__name__)


# TODO Keep database connection open?
# TODO Combine multiple exchanges in one file?
#      - Add a database for each exchange (added with ATTACH DATABASE)
#      - Tables in database stay the same

class PriceData:

    @misc.delayed
    def _get_price_binance(self, base_asset: str, utc_time: datetime.datetime, quote_asset: str, swapped_symbols: bool = False) -> float:
        """Retrieve price from binance official REST API.

        The price is calculated as the average price in a
        time frame of 1 minute around `utc_time`.

        None existing pairs like `TWTEUR` are calculated as
        `TWTBTC * BTCEUR`.

        Documentation: https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md
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
        """Write Price to database.

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

        db_path = Path(config.DATA_PATH, f"{platform}.db")
        tablename = f"{coin}/{reference_coin}"

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

    def gather_data(self, book: Book) -> None:
        """Gather data in advance.

        Args:
            book (Book)
        """
        reference_coin = config.FIAT
        for op in book.operations:
            # We do not need to gather prices for these operation types.
            if isinstance(op, (Airdrop, Deposit, Withdraw)):
                continue
            # By getting the cost, we confirm that data exists or it will
            # be retrieved and writting to our local database.
            self.get_cost(op, reference_coin)
