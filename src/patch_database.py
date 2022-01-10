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

import datetime
import decimal
import logging
import sqlite3
import sys
from inspect import getmembers, isfunction
from pathlib import Path

import config
from database import set_price_db

FUNC_PREFIX = "__patch_"
log = logging.getLogger(__name__)


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


def update_version(db_path: Path, version: int) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute("TRUNCATE §version;")
        assert isinstance(version, int)
        cur.execute(f"INSERT INTO §version (version) VALUES ({version});")


def get_patch_func_version(func_name: str) -> int:
    assert func_name.startswith(
        FUNC_PREFIX
    ), f"Patch function `{func_name}` should start with {FUNC_PREFIX}."
    len_func_prefix = len(FUNC_PREFIX)
    version_str = func_name[len_func_prefix:]
    version = int(version_str)
    return version


def get_tablenames(cur: sqlite3.Cursor) -> list[str]:
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablenames = [result[0] for result in cur.fetchall()]
    return tablenames


def __patch_001(db_path: Path) -> None:
    """Convert prices from float to string

    Args:
        db_path (Path): [description]
    """
    logging.info("applying patch 001")
    with sqlite3.connect(db_path) as conn:
        query = "SELECT name,sql FROM sqlite_master WHERE type='table'"
        cur = conn.execute(query)
        for tablename, sql in cur.fetchall():
            if not sql.lower().contains("price str"):
                query = f"""
                CREATE TABLE "sql_temp_table" (
                "utc_time" DATETIME PRIMARY KEY,
                "price"	STR NOT NULL
                );
                INSERT INTO "sql_temp_table" ("price","utc_time")
                SELECT "price","utc_time" FROM "{tablename}";
                DROP TABLE "{tablename}";
                ALTER TABLE "sql_temp_table" "{tablename}";
                """


def __patch_002(db_path: Path) -> None:
    """Group tablenames, so that the symbols are alphanumerical.

    Args:
        db_path (Path)
    """
    logging.info("applying patch 002")
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        tablenames = get_tablenames(cur)
        # Iterate over all tables.
        for tablename in tablenames:
            base_asset, quote_asset = tablename.split("/")

            # Adjust the order, when the symbols aren't ordered alphanumerical.
            if base_asset > quote_asset:

                # Query all prices from the table.
                cur = conn.execute(f"Select utc_time, price FROM `{tablename}`;")

                for _utc_time, _price in list(cur.fetchall()):
                    # Convert the data.
                    utc_time = datetime.datetime.strptime(
                        _utc_time, "%Y-%m-%d %H:%M:%S%z"
                    )
                    price = decimal.Decimal(_price)
                    set_price_db("", base_asset, quote_asset, utc_time, price, db_path)
                cur = conn.execute(f"DROP TABLE `{tablename}`;")


def patch_databases() -> None:
    # Check if any database paths exist.
    database_paths = [p for p in Path(config.DATA_PATH).glob("*.db") if p.is_file()]
    if not database_paths:
        return

    # Patch all databases separatly.
    for db_path in database_paths:
        # Read version from database.
        current_version = get_version(db_path)

        # Determine all necessary patch functions.
        patch_func_names = [
            func[0]
            for func in getmembers(sys.modules[__name__], isfunction)
            if func[0].startswith(FUNC_PREFIX)
            if get_patch_func_version(func[0]) > current_version
        ]

        # Sort patch functions chronological.
        patch_func_names.sort(key=get_patch_func_version)

        # Run the patch functions.
        for patch_func_name in patch_func_names:
            patch_func = eval(patch_func_name)
            patch_func(db_path)

        # Update version.
        new_version = get_patch_func_version(patch_func_name)
        update_version(db_path, new_version)
