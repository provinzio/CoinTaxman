import datetime
import decimal
import logging
import sqlite3
from pathlib import Path
import misc
from typing import Optional

import config

log = logging.getLogger(__name__)


class Database:
    def get_version(self, db_path: Path) -> int:
        """Get database version from a database file.

        If the version table is missing, one is created.

        Args:
            db_path (str): Path to database file.

        Raises:
            RuntimeError: The database version is ambiguous.

        Returns:
            int: Version of database file.
        """
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            try:
                cur.execute("SELECT version FROM §version;")
                versions = [int(v[0]) for v in cur.fetchall()]
            except sqlite3.OperationalError as e:
                if str(e) == "no such table: §version":
                    # The §version table doesn't exist. Create one.
                    cur.execute("CREATE TABLE §version(version INT);")
                    cur.execute("INSERT INTO §version (version) VALUES (0);")
                    return 0
                else:
                    raise e

            if len(versions) == 1:
                version = versions[0]
                return version
            else:
                raise RuntimeError(
                    f"The database version of the file `{db_path.name}` is ambigious. "
                    f"The table `§version` should have one entry, but has {len(versions)}."
                )

    def get_price(
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

    def mean_price(
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

    def set_price_db(
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
        # TODO if db_path doesn't exists. Create db with §version table and
        #      newest version number. It would be nicer, if this could be done
        #      as a preprocessing step. see book.py
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
                        "price STR NOT NULL);"
                    )
                    cur.execute(create_query)
                    cur.execute(query, (utc_time, str(price)))
                else:
                    raise e
            conn.commit()

    def get_tablename(self, coin: str, reference_coin: str) -> str:
        return f"{coin}/{reference_coin}"

    def get_tablenames_from_db(self, cur: sqlite3.Cursor) -> list[str]:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tablenames = [result[0] for result in cur.fetchall()]
        return tablenames


class Databases:
    def __init__(self) -> None:
        platforms = self.get_all_dbs()

    def get_all_dbs():
        pass

    def get_db_path(self, platform: str) -> Path:
        return Path(config.DATA_PATH, f"{platform}.db")
