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
import json
import sqlite3
from pathlib import Path
from typing import Any, Union

import config
import log_config
import transaction as tr
from database import get_price_db, get_tablenames_from_db, mean_price_db, set_price_db
from price_providers import create_price_provider

log = log_config.getLogger(__name__)


class PriceData:
    _PIONEX_USD_STABLE_ASSETS = {
        "USDT",
        "USDC",
        "BUSD",
        "FDUSD",
        "TUSD",
        "USDP",
        "PYUSD",
        "DAI",
    }

    def __init__(self) -> None:
        self._providers = {}
        self._missing_symbols_cache_path = Path(
            config.DATA_PATH) / "missing_price_symbols.json"
        self._provider_missing_symbols = self._load_missing_symbols_cache()

    def _load_missing_symbols_cache(self) -> dict[str, set[str]]:
        try:
            with open(self._missing_symbols_cache_path, encoding="utf8") as f:
                raw_data = json.load(f)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            log.warning(
                "Unable to parse missing symbol cache at %s. Starting with empty cache.",
                self._missing_symbols_cache_path,
            )
            return {}

        if not isinstance(raw_data, dict):
            log.warning(
                "Unexpected missing symbol cache format at %s. Starting with empty cache.",
                self._missing_symbols_cache_path,
            )
            return {}

        cache: dict[str, set[str]] = {}
        for platform, symbols in raw_data.items():
            if isinstance(platform, str) and isinstance(symbols, list):
                cache[platform] = {
                    symbol for symbol in symbols if isinstance(symbol, str)}
        return cache

    def _save_missing_symbols_cache(self) -> None:
        Path(config.DATA_PATH).mkdir(parents=True, exist_ok=True)
        serializable_cache = {
            platform: sorted(symbols)
            for platform, symbols in sorted(self._provider_missing_symbols.items())
            if symbols
        }
        with open(self._missing_symbols_cache_path, "w", encoding="utf8") as f:
            json.dump(serializable_cache, f, indent=2, sort_keys=True)

    def _get_provider(self, platform: str):
        provider = self._providers.get(platform)
        if provider is not None:
            return provider

        provider = create_price_provider(
            platform,
            self.get_price,
            missing_symbols=self._provider_missing_symbols.get(platform, set()),
        )
        if provider is not None:
            self._providers[platform] = provider
        return provider

    def _persist_provider_missing_symbols(self, platform: str, provider) -> None:
        if not provider.has_missing_symbols_update():
            return

        symbols = provider.get_missing_symbols()
        if symbols:
            self._provider_missing_symbols[platform] = symbols
        else:
            self._provider_missing_symbols.pop(platform, None)
        self._save_missing_symbols_cache()
        provider.mark_missing_symbols_persisted()

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

        # Check if price exists already in our database.
        price = get_price_db(platform, coin, reference_coin, utc_time)
        if price is None:
            # Price doesn't exists. Fetch price from platform.
            provider = self._get_provider(platform)
            if provider is None:
                raise NotImplementedError(f"Unable to read data from {platform=}")

            get_price = provider.fetch_price
            try:
                price = get_price(coin, utc_time, reference_coin, **kwargs)
            finally:
                self._persist_provider_missing_symbols(platform, provider)
            assert isinstance(price, decimal.Decimal)
            set_price_db(platform, coin, reference_coin, utc_time, price)
        elif (
            price <= 0
            and platform == "pionex"
            and coin in self._PIONEX_USD_STABLE_ASSETS
            and reference_coin != "USD"
        ):
            # Recover from legacy cached zero prices for Pionex stablecoins
            # (e.g. USDT/EUR) by re-running provider fallback logic.
            provider = self._get_provider(platform)
            if provider is not None:
                get_price = provider.fetch_price
                try:
                    refreshed = get_price(coin, utc_time, reference_coin)
                finally:
                    self._persist_provider_missing_symbols(platform, provider)

                if refreshed > 0:
                    price = refreshed
                    set_price_db(
                        platform,
                        coin,
                        reference_coin,
                        utc_time,
                        price,
                        overwrite=True,
                    )

        if config.MEAN_MISSING_PRICES and price <= 0.0:
            # The price is missing. Check for prices before and after the
            # transaction and estimate the price.
            # Do not save price in database.
            price = mean_price_db(platform, coin, reference_coin, utc_time)

        return price

    def get_cost(
        self,
        op_sc: Union[tr.Operation, tr.SoldCoin],
        reference_coin: str = config.FIAT,
    ) -> decimal.Decimal:
        op = op_sc if isinstance(op_sc, tr.Operation) else op_sc.op
        price = self.get_price(op.platform, op.coin, op.utc_time, reference_coin)
        if isinstance(op_sc, tr.Operation):
            return price * op_sc.change
        if isinstance(op_sc, tr.SoldCoin):
            return price * op_sc.sold
        raise NotImplementedError

    def get_partial_cost(
        self,
        op_sc: Union[tr.Operation, tr.SoldCoin],
        percent: decimal.Decimal,
        reference_coin: str = config.FIAT,
    ) -> decimal.Decimal:
        return percent * self.get_cost(op_sc, reference_coin=reference_coin)

    def check_database(self):
        stats = {}

        for db_path in Path(config.DATA_PATH).glob("*.db"):
            if db_path.is_file():
                platform = db_path.stem
                stats[platform] = {"fix": 0, "rem": 0}
                provider = self._get_provider(platform)
                if provider is None:
                    log.warning(
                        "No price provider registered for %s. "
                        "Database check for this platform will be skipped.",
                        platform,
                    )
                    del stats[platform]
                    continue

                get_price = provider.fetch_price

                with sqlite3.connect(db_path) as conn:
                    cur = conn.cursor()
                    tablenames = get_tablenames_from_db(cur)
                    for tablename in tablenames:
                        base_asset, quote_asset = tablename.split("/")
                        query = f"SELECT utc_time FROM `{tablename}` WHERE price<=0.0;"
                        cur = conn.execute(query)
                        data = cur.fetchall()

                        for row in data:
                            try:
                                utc_time = datetime.datetime.strptime(
                                    row[0], "%Y-%m-%d %H:%M:%S%z"
                                )
                                timezone_aware = True
                            except ValueError:
                                utc_time = datetime.datetime.strptime(
                                    row[0], "%Y-%m-%d %H:%M:%S"
                                )
                                timezone_aware = False

                            price = get_price(base_asset, utc_time, quote_asset)

                            if not timezone_aware:
                                timezone_aware_utc_time = utc_time.astimezone(
                                    datetime.timezone.utc
                                )
                                timezone_aware_price = get_price(
                                    base_asset,
                                    timezone_aware_utc_time,
                                    quote_asset,
                                )
                                if timezone_aware_price:
                                    log.info(
                                        "Delete timezone unaware price of "
                                        f"{tablename=} on {platform=} at {utc_time=} "
                                        "because there already exists a timezone "
                                        "aware price for the same (utc) time"
                                    )
                                    query = (
                                        f"DELETE FROM `{tablename}`" "WHERE utc_time=?"
                                    )
                                    conn.execute(query, (utc_time,))
                                    conn.commit()
                                else:
                                    log.info(
                                        "Update timezone unaware price of "
                                        f"{tablename=} on {platform=} at {utc_time=}"
                                        "to utc-timezone aware price"
                                    )
                                    query = (
                                        f"UPDATE `{tablename}` "
                                        "SET utc_time=? "
                                        "WHERE utc_time=?;"
                                    )
                                    conn.execute(
                                        query,
                                        (
                                            timezone_aware_utc_time,
                                            utc_time,
                                        ),
                                    )
                                    conn.commit()
                                continue

                            if price == 0.0:
                                log.warning(
                                    "Could not fetch price for pair "
                                    f"{tablename} on {platform} at {utc_time}"
                                )
                                stats[platform]["rem"] += 1
                            else:
                                log.info(
                                    f"Updating {tablename} at {utc_time} to {price}"
                                )
                                query = (
                                    f"UPDATE `{tablename}` "
                                    "SET price=? "
                                    "WHERE utc_time=?;"
                                )
                                conn.execute(query, (str(price), utc_time))
                                conn.commit()
                                stats[platform]["fix"] += 1

                    conn.commit()

                self._persist_provider_missing_symbols(platform, provider)

        log.info("Check Database Result:")
        for platform, result in stats.items():
            fixed, remaining = result.values()
            log.info(f"{platform}: {fixed} fixed, {remaining} remaining")
