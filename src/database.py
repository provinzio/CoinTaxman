import datetime
import decimal
import sqlite3
from pathlib import Path
from typing import Optional, Tuple

import config
import log_config
import misc

log = log_config.getLogger(__name__)


def get_version(db_path: Path) -> int:
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


def __get_price_db(
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
                price = misc.force_decimal(prices[0])
                return price

    return None


def get_price_db(
    platform: str,
    coin: str,
    reference_coin: str,
    utc_time: datetime.datetime,
    db_path: Optional[Path] = None,
) -> Optional[decimal.Decimal]:
    """Try to retrieve the price from our local database.

    Args:
        platform (str)
        coin (str)
        reference_coin (str)
        utc_time (datetime.datetime)
        db_path (Optional[Path]): Defaults to None.

    Returns:
        Optional[decimal.Decimal]: Price.
    """
    tablename, inverted = get_sorted_tablename(coin, reference_coin)
    db_path = get_db_path(platform, db_path)

    price = __get_price_db(db_path, tablename, utc_time)

    if price is None:
        return None

    if not price and config.REFETCH_MISSING_PRICES:
        # Return None instead of price=0, so that our tool refetches the price.
        return None

    if inverted:
        price = misc.reciprocal(price)

    return price


def __mean_price_db(
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
                before_time = misc.parse_iso_timestamp_to_decimal_timestamp(result[0])
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
                after_time = misc.parse_iso_timestamp_to_decimal_timestamp(result[0])
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


def mean_price_db(
    platform: str,
    coin: str,
    reference_coin: str,
    utc_time: datetime.datetime,
    db_path: Optional[Path] = None,
) -> decimal.Decimal:
    """Try to retrieve the price right before and after `utc_time`
    from our local database.

    Return 0 if the price could not be estimated.
    The function does not check, if a price for `utc_time` exists.

    Args:
        platform (str)
        coin (str)
        reference_coin (str)
        utc_time (datetime.datetime)
        db_path (Optional[Path]): Defaults to None.

    Returns:
        decimal.Decimal: Price
    """
    tablename, inverted = get_sorted_tablename(coin, reference_coin)
    db_path = get_db_path(platform, db_path)

    if price := __mean_price_db(db_path, tablename, utc_time):
        if inverted:
            price = misc.reciprocal(price)

    return price


def __delete_price_db(
    db_path: Path,
    tablename: str,
    utc_time: datetime.datetime,
) -> None:
    """Delete price from database

    Args:
        db_path (Path)
        tablename (str)
        utc_time (datetime.datetime)
    """

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        query = f"DELETE FROM `{tablename}` WHERE utc_time=?;"
        cur.execute(query, (utc_time,))
        conn.commit()


def __set_price_db(
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
    if not db_path.exists():
        from patch_database import create_new_database

        create_new_database(db_path)

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        query = f"INSERT INTO `{tablename}` ('utc_time', 'price') VALUES (?, ?);"
        try:
            cur.execute(query, (utc_time, str(price)))
        except sqlite3.OperationalError as e:
            if str(e) == f"no such table: {tablename}":
                create_query = (
                    f"CREATE TABLE `{tablename}`"
                    "(utc_time DATETIME PRIMARY KEY, "
                    "price VARCHAR(255) NOT NULL);"
                )
                cur.execute(create_query)
                cur.execute(query, (utc_time, str(price)))
            else:
                raise e
        conn.commit()


def set_price_db(
    platform: str,
    coin: str,
    reference_coin: str,
    utc_time: datetime.datetime,
    price: decimal.Decimal,
    db_path: Optional[Path] = None,
    overwrite: bool = False,
) -> None:
    """Write price to database.

    Tries to insert a historical price into the local database.

    A warning will be raised, if there is already a different price.

    Args:
        platform (str)
        coin (str)
        reference_coin (str)
        utc_time (datetime.datetime)
        price (decimal.Decimal)
        db_path (Optional[Path]): Defaults to None.
        overwrite (bool): Default to False.
    """
    assert coin != reference_coin

    tablename, inverted = get_sorted_tablename(coin, reference_coin)
    db_path = get_db_path(platform, db_path)

    if inverted:
        price = misc.reciprocal(price)

    try:
        __set_price_db(db_path, tablename, utc_time, price)
    except sqlite3.IntegrityError as e:
        if f"UNIQUE constraint failed: {tablename}.utc_time" in str(e):
            # Trying to add an already existing price in db.
            # Check price from db and issue warning, if prices do not match.
            price_db = __get_price_db(db_path, tablename, utc_time)

            assert isinstance(price_db, decimal.Decimal)
            assert isinstance(price, decimal.Decimal)

            # Always overwrite missing prices in database.
            if price_db == 0:
                overwrite = True

            # Calculate the relative error between new price and price in database.
            if price == price_db:
                rel_error = decimal.Decimal(0)
            elif price == 0:
                rel_error = decimal.Decimal(1)
            else:
                rel_error = abs(price - price_db) / price

            if abs(rel_error) > decimal.Decimal("1E-16"):
                log.debug(
                    f"Tried to write {tablename} price to database, but a "
                    f"different price exists already ({platform} @ {utc_time})"
                )
                if overwrite:
                    # Overwrite price.
                    log.info(
                        f"Relative error: %.6f %%, using new price: {price}, "
                        f"overwriting database price: {price_db}",
                        rel_error * 100,
                    )
                    __delete_price_db(db_path, tablename, utc_time)
                    __set_price_db(db_path, tablename, utc_time, price)
                else:
                    log.warning(
                        f"Relative error: %.6f %%, discarding new price: {price}, "
                        f"using database price: {price_db}",
                        rel_error * 100,
                    )
        else:
            raise e


def _sort_pair(coin: str, reference_coin: str) -> Tuple[str, str, bool]:
    """Sort the coin pair in alphanumerical order.

    Args:
        coin (str)
        reference_coin (str)

    Returns:
        Tuple[str, str, bool]: First coin, second coin, inverted
    """
    if inverted := coin > reference_coin:
        coin_a = reference_coin
        coin_b = coin
    else:
        coin_a = coin
        coin_b = reference_coin
    return coin_a, coin_b, inverted


def get_sorted_tablename(coin: str, reference_coin: str) -> tuple[str, bool]:
    coin_a, coin_b, inverted = _sort_pair(coin, reference_coin)
    tablename = f"{coin_a}/{coin_b}"
    return tablename, inverted


def get_tablenames_from_db(
    cur: sqlite3.Cursor, ignore_version_table: bool = True
) -> list[str]:
    query = "SELECT name FROM sqlite_master WHERE type='table'"
    if ignore_version_table:
        query += " AND name != '§version'"
    cur.execute(f"{query};")
    tablenames = [result[0] for result in cur.fetchall()]
    return tablenames


def get_db_path(platform: str, db_path: Optional[Path] = None) -> Path:
    if db_path is None and platform:
        db_path = Path(config.DATA_PATH, f"{platform}.db")

    assert isinstance(db_path, Path), "DB path is no valid path"
    return db_path


def check_database_or_create(platform: str) -> None:
    from patch_database import create_new_database

    db_path = get_db_path(platform)
    if not db_path.exists():
        create_new_database(db_path)
