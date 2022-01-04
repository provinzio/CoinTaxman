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

import csv
import datetime
import decimal
import logging
import re
from pathlib import Path
from typing import Optional

import config
import misc
import transaction as tr
from core import kraken_asset_map
from price_data import PriceData

log = logging.getLogger(__name__)


class Book:
    def __init__(self, price_data: PriceData) -> None:
        self.price_data = price_data

        self.operations: list[tr.Operation] = []

    def __bool__(self) -> bool:
        return bool(self.operations)

    def append_operation(
        self,
        operation: str,
        utc_time: datetime.datetime,
        platform: str,
        change: decimal.Decimal,
        coin: str,
        row: int,
        file_path: Path,
    ) -> None:

        try:
            Op = getattr(tr, operation)
        except AttributeError:
            log.warning(
                "Could not recognize operation `%s` in  %s file `%s:%i`.",
                operation,
                platform,
                file_path,
                row,
            )
            return

        o = Op(utc_time, platform, change, coin, row, file_path)
        self.operations.append(o)

    def _read_binance(self, file_path: Path, version: int = 1) -> None:
        platform = "binance"
        operation_mapping = {
            "Distribution": "Airdrop",
            "Savings Interest": "CoinLendInterest",
            "Savings purchase": "CoinLend",
            "Savings Principal redemption": "CoinLendEnd",
            "Commission History": "Commission",
            "Commission Fee Shared With You": "Commission",
            "Referrer rebates": "Commission",
            "Launchpool Interest": "StakingInterest",
            "Cash Voucher distribution": "Airdrop",
            "Super BNB Mining": "StakingInterest",
            "Liquid Swap add": "CoinLend",
            "Liquid Swap remove": "CoinLendEnd",
            "POS savings interest": "StakingInterest",
            "POS savings purchase": "Staking",
            "POS savings redemption": "StakingEnd",
            "Withdraw": "Withdrawal",
        }

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for rowlist in reader:
                if version == 1:
                    _utc_time, account, operation, coin, _change, remark = rowlist
                elif version == 2:
                    (
                        _,
                        _utc_time,
                        account,
                        operation,
                        coin,
                        _change,
                        remark,
                    ) = rowlist
                else:
                    log.error("File version not Supported " + str(file_path))
                    raise NotImplementedError

                row = reader.line_num

                # Parse data.
                utc_time = datetime.datetime.strptime(_utc_time, "%Y-%m-%d %H:%M:%S")
                utc_time = utc_time.replace(tzinfo=datetime.timezone.utc)
                change = misc.force_decimal(_change)
                operation = operation_mapping.get(operation, operation)
                if operation in (
                    "The Easiest Way to Trade",
                    "Small assets exchange BNB",
                    "Transaction Related",
                    "Large OTC trading",
                    "Sell",
                    "Buy",
                ):
                    operation = "Sell" if change < 0 else "Buy"

                if operation == "Commission" and account != "Spot":
                    # All comissions will be handled the same way.
                    # As of now, only Spot Binance Operations are supported,
                    # so we have to change the account type to Spot.
                    account = "Spot"

                if account in ("Spot", "P2P") and operation in (
                    "transfer_in",
                    "transfer_out",
                ):
                    # Ignore transfer from and to P2P market.
                    continue

                change = abs(change)

                # Validate data.
                assert account == "Spot", (
                    "Other types than Spot are currently not supported. "
                    "Please create an Issue or PR."
                )
                assert operation
                assert coin
                assert change

                # Check for problems.
                if remark:
                    log.warning(
                        "I may have missed a remark in %s:%i: `%s`.",
                        file_path,
                        row,
                        remark,
                    )

                self.append_operation(
                    operation, utc_time, platform, change, coin, row, file_path
                )

    def _read_binance_v2(self, file_path: Path) -> None:
        self._read_binance(file_path=file_path, version=2)

    def _read_coinbase(self, file_path: Path) -> None:
        platform = "coinbase"
        operation_mapping = {
            "Receive": "Deposit",
            "Send": "Withdrawal",
            "Coinbase Earn": "Buy",
        }

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            try:
                assert next(reader)  # header line
                assert next(reader) == []
                assert next(reader) == []
                assert next(reader) == []
                assert next(reader) == ["Transactions"]
                assert next(reader)  # user row
                assert next(reader) == []

                fields = next(reader)
                num_columns = len(fields)
                # Coinbase export format from late 2021 and ongoing
                if num_columns == 10:
                    assert fields == [
                        "Timestamp",
                        "Transaction Type",
                        "Asset",
                        "Quantity Transacted",
                        "Spot Price Currency",
                        "Spot Price at Transaction",
                        "Subtotal",
                        "Total (inclusive of fees)",
                        "Fees",
                        "Notes",
                    ]
                # Coinbase export format from mid 2021 and before
                elif num_columns == 9:
                    assert fields == [
                        "Timestamp",
                        "Transaction Type",
                        "Asset",
                        "Quantity Transacted",
                        "EUR Spot Price at Transaction",
                        "EUR Subtotal",
                        "EUR Total (inclusive of fees)",
                        "EUR Fees",
                        "Notes",
                    ]
                else:
                    raise RuntimeError(
                        "Unknown Coinbase format: "
                        "Number of rows do not match known versions: "
                        f"{file_path}."
                    )
            except AssertionError as e:
                msg = (
                    "Unable to read coinbase file: Malformed header. "
                    f"Skipping {file_path}."
                )
                e.args += (msg,)
                log.exception(e)
                return

            for columns in reader:

                # Coinbase export format from late 2021 and ongoing
                if num_columns == 10:
                    (
                        _utc_time,
                        operation,
                        coin,
                        _change,
                        _currency_spot,
                        _eur_spot,
                        _eur_subtotal,
                        _eur_total,
                        _eur_fee,
                        remark,
                    ) = columns

                # Coinbase export format from mid 2021 and before
                elif num_columns == 9:
                    (
                        _utc_time,
                        operation,
                        coin,
                        _change,
                        _eur_spot,
                        _eur_subtotal,
                        _eur_total,
                        _eur_fee,
                        remark,
                    ) = columns
                    _currency_spot = "EUR"

                row = reader.line_num

                # Parse data.
                utc_time = datetime.datetime.strptime(_utc_time, "%Y-%m-%dT%H:%M:%SZ")
                utc_time = utc_time.replace(tzinfo=datetime.timezone.utc)
                operation = operation_mapping.get(operation, operation)
                change = misc.force_decimal(_change)
                #  Current price from exchange.
                eur_spot = misc.force_decimal(_eur_spot)
                #  Cost without fees.
                eur_subtotal = misc.xdecimal(_eur_subtotal)
                eur_fee = misc.xdecimal(_eur_fee)

                # Validate data.
                assert operation
                assert coin
                assert change
                assert _currency_spot == "EUR"

                # Save price in our local database for later.
                self.price_data.set_price_db(platform, coin, "EUR", utc_time, eur_spot)

                if operation == "Convert":
                    # Parse change + coin from remark, which is
                    # in format "Converted 0,123 ETH to 0,456 BTC".
                    match = re.match(
                        r"^Converted [0-9,\.]+ [A-Z]+ to "
                        r"(?P<change>[0-9,\.]+) (?P<coin>[A-Z]+)$",
                        remark,
                    )
                    assert match

                    _convert_change = match.group("change").replace(",", ".")
                    convert_change = misc.force_decimal(_convert_change)
                    convert_coin = match.group("coin")

                    eur_total = misc.force_decimal(_eur_total)
                    convert_eur_spot = eur_total / convert_change

                    self.append_operation(
                        "Sell", utc_time, platform, change, coin, row, file_path
                    )
                    self.append_operation(
                        "Buy",
                        utc_time,
                        platform,
                        convert_change,
                        convert_coin,
                        row,
                        file_path,
                    )

                    # Save convert price in local database, too.
                    self.price_data.set_price_db(
                        platform, convert_coin, "EUR", utc_time, convert_eur_spot
                    )
                else:
                    self.append_operation(
                        operation, utc_time, platform, change, coin, row, file_path
                    )

                    if operation == "Sell":
                        assert isinstance(eur_subtotal, decimal.Decimal)
                        self.append_operation(
                            "Buy",
                            utc_time,
                            platform,
                            eur_subtotal,
                            "EUR",
                            row,
                            file_path,
                        )
                    elif operation == "Buy":
                        assert isinstance(eur_subtotal, decimal.Decimal)
                        self.append_operation(
                            "Sell",
                            utc_time,
                            platform,
                            eur_subtotal,
                            "EUR",
                            row,
                            file_path,
                        )

                    if eur_fee:
                        self.append_operation(
                            "Fee", utc_time, platform, eur_fee, "EUR", row, file_path
                        )

    def _read_coinbase_pro(self, file_path: Path) -> None:
        platform = "coinbase_pro"
        operation_mapping = {
            "BUY": "Buy",
            "SELL": "Sell",
        }

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for (
                portfolio,
                trade_id,
                product,
                operation,
                _utc_time,
                _size,
                size_unit,
                _price,
                _fee,
                total,
                price_fee_total_unit,
            ) in reader:
                row = reader.line_num

                # Parse data.
                utc_time = datetime.datetime.strptime(
                    _utc_time, "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                utc_time = utc_time.replace(tzinfo=datetime.timezone.utc)
                operation = operation_mapping.get(operation, operation)
                size = misc.force_decimal(_size)
                price = misc.force_decimal(_price)
                fee = misc.xdecimal(_fee)
                total_price = size * price

                # Unused variables.
                del portfolio
                del trade_id
                del product
                del total

                # Validate data.
                assert operation
                assert size
                assert size_unit
                assert price_fee_total_unit

                self.append_operation(
                    operation, utc_time, platform, size, size_unit, row, file_path
                )

                if operation == "Sell":
                    self.append_operation(
                        "Buy",
                        utc_time,
                        platform,
                        total_price,
                        price_fee_total_unit,
                        row,
                        file_path,
                    )
                elif operation == "Buy":
                    self.append_operation(
                        "Sell",
                        utc_time,
                        platform,
                        total_price,
                        price_fee_total_unit,
                        row,
                        file_path,
                    )
                if fee:
                    self.append_operation(
                        "Fee",
                        utc_time,
                        platform,
                        fee,
                        price_fee_total_unit,
                        row,
                        file_path,
                    )

    def _read_kraken_trades(self, file_path: Path) -> None:
        log.error(
            f"{file_path.name}: "
            "Looks like this is a Kraken 'Trades' history, "
            "but we need the 'Ledgers' history. "
            "(See: Wiki - Exchange Kraken)"
        )

    def _read_kraken_ledgers(self, file_path: Path) -> None:
        platform = "kraken"
        operation_mapping = {
            "spend": "Sell",  # Sell ordered via 'Buy Crypto' button
            "receive": "Buy",  # Buy ordered via 'Buy Crypto' button
            "reward": "StakingInterest",
            "staking": "StakingInterest",
            "deposit": "Deposit",
            "withdrawal": "Withdrawal",
        }

        # Need to track state of duplicate entries
        # for deposits / withdrawals based on refid
        refids = []
        margin_warnings = 0

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for columns in reader:

                num_columns = len(columns)
                # Kraken ledgers export format from October 2020 and ongoing
                if num_columns == 10:
                    (
                        txid,
                        refid,
                        _utc_time,
                        _type,
                        subtype,
                        aclass,
                        _asset,
                        _amount,
                        _fee,
                        balance,
                    ) = columns

                # Kraken ledgers export format from September 2020 and before
                elif num_columns == 9:
                    (
                        txid,
                        refid,
                        _utc_time,
                        _type,
                        aclass,
                        _asset,
                        _amount,
                        _fee,
                        balance,
                    ) = columns
                else:
                    raise RuntimeError(
                        "Unknown Kraken ledgers format: "
                        "Number of rows do not match known versions."
                    )

                row = reader.line_num

                # Skip duplicate entries for deposits / withdrawals and
                # additional deposit / withdrawals lines for
                # staking / unstaking / staking reward actions
                if _type in ["deposit", "withdrawal"]:
                    if refid not in refids:
                        refids.append(refid)
                        continue

                # Parse data.
                utc_time = datetime.datetime.strptime(_utc_time, "%Y-%m-%d %H:%M:%S")
                utc_time = utc_time.replace(tzinfo=datetime.timezone.utc)
                change = misc.force_decimal(_amount)
                # remove the appended .S for staked assets
                _asset = _asset.removesuffix(".S")
                coin = kraken_asset_map.get(_asset, _asset)
                fee = misc.force_decimal(_fee)
                operation = operation_mapping.get(_type)
                if operation is None:
                    if _type == "trade":
                        operation = "Sell" if change < 0 else "Buy"
                    elif _type in ["margin trade", "rollover", "settled", "margin"]:
                        margin_warnings += 1
                        continue
                    elif _type == "transfer":
                        if num_columns == 9:
                            # for backwards compatibility assume Airdrop for staking
                            log.warning(
                                f"{file_path}: {row}: Staking is not supported for old"
                                " Kraken ledger formats. Please create an Issue or PR."
                            )
                            operation = "Airdrop"
                        elif subtype == "stakingfromspot":
                            operation = "Staking"
                        elif subtype == "stakingtospot":
                            operation = "StakingEnd"
                        elif subtype in ["spottostaking", "spotfromstaking"]:
                            # duplicate entries for staking actions
                            continue
                        else:
                            log.error(
                                f"{file_path}: {row}: Order subtype '{subtype}' is "
                                "currently not supported. Please create an Issue or PR."
                            )
                            raise RuntimeError
                    else:
                        log.error(
                            f"{file_path}: {row}: Other order type '{_type}' is "
                            "currently not supported. Please create an Issue or PR."
                        )
                        raise RuntimeError
                change = abs(change)

                # Validate data.
                assert operation
                assert coin
                assert change

                self.append_operation(
                    operation, utc_time, platform, change, coin, row, file_path
                )

                if fee != 0:
                    self.append_operation(
                        "Fee", utc_time, platform, fee, coin, row, file_path
                    )

        if margin_warnings:
            log.warning(
                f"{file_path}: {margin_warnings} margin entries. Margin trading is "
                "currently not supported. Please create an Issue or PR."
            )

    def _read_kraken_ledgers_old(self, file_path: Path) -> None:

        self._read_kraken_ledgers(file_path)

    def _read_bitpanda_pro_trades(self, file_path: Path) -> None:
        """Reads a trade statement from Bitpanda Pro.

        Args:
            file_path (Path): Path to Bitpanda trade history.
        """

        platform = "bitpanda_pro"
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # skip header
            line = next(reader)

            line = next(reader)

            transaction_file_warn = (
                f"{file_path} looks like a Bitpanda transaction file."
                " Skipping. Please download the trade history instead."
            )

            # for transactions, it's currently written "id" (small)
            if line[0].startswith("Account id :"):
                log.warning(transaction_file_warn)
                return

            assert line[0].startswith("Account ID:")
            line = next(reader)
            # empty line - still keep this check in case Bitpanda changes the
            # transaction file to match the trade header (casing)
            if not line:
                log.warning(transaction_file_warn)
                return

            elif line[0] != "Bitpanda Pro trade history":
                log.warning(
                    f"{file_path} doesn't look like a Bitpanda trade file. Skipping."
                )
                return

            line = next(reader)
            assert line in [
                [
                    "Order ID",
                    "Trade ID",
                    "Type",
                    "Market",
                    "Amount",
                    "Amount Currency",
                    "Price",
                    "Price Currency",
                    "Fee",
                    "Fee Currency",
                    "Time (UTC)",
                ],
                [
                    "Order ID",
                    "Trade ID",
                    "Type",
                    "Market",
                    "Amount",
                    "Amount Currency",
                    "Price",
                    "Price Currency",
                    "Fee",
                    "Fee Currency",
                    "Time (UTC)",
                    "BEST_EUR Rate",
                ],
            ]

            for current_line in reader:
                if len(current_line) == 11:
                    (
                        _order_id,
                        _trace_id,
                        operation,
                        trade_pair,
                        amount,
                        amount_currency,
                        _price,
                        price_currency,
                        fee,
                        fee_currency,
                        _utc_time,
                    ) = current_line
                    best_price = None
                elif len(current_line) == 12:
                    (
                        _order_id,
                        _trace_id,
                        operation,
                        trade_pair,
                        amount,
                        amount_currency,
                        _price,
                        price_currency,
                        fee,
                        fee_currency,
                        _utc_time,
                        best_price,
                    ) = current_line
                else:
                    raise NotImplementedError

                row = reader.line_num

                # trade pair is of form e.g. BTC_EUR
                assert [amount_currency, price_currency] == trade_pair.split("_")

                # At the time of writing (2021-05-02),
                # there were only these two operations
                assert operation in ["BUY", "SELL"], "Unsupported operation"

                change = misc.force_decimal(amount)
                assert change > 0, "Unexpected value for 'Amount' column"

                # see _get_price_bitpanda_pro in price_data.py
                assert price_currency == "EUR", (
                    "Only Euro is supported as 'price' currency, "
                    "since price fetching is not fully implemented yet."
                )

                # sanity checks
                assert (
                    fee_currency == "BEST"
                    or (operation == "SELL" and fee_currency == price_currency)
                    or (operation == "BUY" and fee_currency == amount_currency)
                ), "Invalid fee currency"

                utc_time = misc.parse_iso_timestamp(_utc_time)

                coin = amount_currency

                self.append_operation(
                    operation.title(), utc_time, platform, change, coin, row, file_path
                )

                # Save price in our local database for later.
                price = misc.force_decimal(_price)
                self.price_data.set_price_db(platform, coin, "EUR", utc_time, price)
                if best_price:
                    self.price_data.set_price_db(
                        platform,
                        "BEST",
                        "EUR",
                        utc_time,
                        misc.force_decimal(best_price),
                    )

                self.append_operation(
                    "Fee",
                    utc_time,
                    platform,
                    misc.force_decimal(fee),
                    fee_currency,
                    row,
                    file_path,
                )

    def detect_exchange(self, file_path: Path) -> Optional[str]:
        if file_path.suffix == ".csv":
            with open(file_path, encoding="utf8") as f:
                reader = csv.reader(f)
                header = next(reader, None)

            expected_headers = {
                "binance": [
                    "UTC_Time",
                    "Account",
                    "Operation",
                    "Coin",
                    "Change",
                    "Remark",
                ],
                "binance_v2": [
                    "User_ID",
                    "UTC_Time",
                    "Account",
                    "Operation",
                    "Coin",
                    "Change",
                    "Remark",
                ],
                "coinbase": [
                    "You can use this transaction report to inform your "
                    "likely tax obligations. For US customers, Sells, "
                    "Converts, and Rewards Income, and Coinbase Earn "
                    "transactions are taxable events. For final tax "
                    "obligations, please consult your tax advisor."
                ],
                "coinbase_pro": [
                    "portfolio",
                    "trade id",
                    "product",
                    "side",
                    "created at",
                    "size",
                    "size unit",
                    "price",
                    "fee",
                    "total",
                    "price/fee/total unit",
                ],
                "kraken_ledgers_old": [
                    "txid",
                    "refid",
                    "time",
                    "type",
                    "aclass",
                    "asset",
                    "amount",
                    "fee",
                    "balance",
                ],
                "kraken_ledgers": [
                    "txid",
                    "refid",
                    "time",
                    "type",
                    "subtype",
                    "aclass",
                    "asset",
                    "amount",
                    "fee",
                    "balance",
                ],
                "kraken_trades": [
                    "txid",
                    "ordertxid",
                    "pair",
                    "time",
                    "type",
                    "ordertype",
                    "price",
                    "cost",
                    "fee",
                    "vol",
                    "margin",
                    "misc",
                    "ledgers",
                ],
                "bitpanda_pro_trades": [
                    "Disclaimer: All data is without guarantee,"
                    " errors and changes are reserved."
                ],
            }
            for exchange, expected in expected_headers.items():
                if header == expected:
                    return exchange

        return None

    def read_file(self, file_path: Path) -> None:
        """Import transactions form an account statement.

        Detect the exchange of the file. The file will be ignored with a
        warning, if the detecting or reading functionality is not implemented.

        Args:
            file_path (Path): Path to account statment.
        """
        assert file_path.is_file()

        if exchange := self.detect_exchange(file_path):
            try:
                read_file = getattr(self, f"_read_{exchange}")
            except AttributeError:
                log.warning(
                    f"Unable to read files from the exchange `{exchange}`. "
                    f"Skipping `{file_path}`."
                )
                return

            log.info("Reading file from exchange %s at %s", exchange, file_path)
            read_file(file_path)
        else:
            log.warning(
                f"Unable to detect the exchange of file `{file_path}`. "
                "Skipping file."
            )

    def get_account_statement_paths(self, statements_dir: Path) -> list[Path]:
        """Return file paths of all account statements in `statements_dir`.

        Args:
            statements_dir (str): Folder in which account statements
                                  will be searched.

        Returns:
            list[Path]: List of account statement file paths.
        """
        file_paths: list[Path] = []

        if statements_dir.is_dir():
            for file_path in statements_dir.iterdir():
                # Ignore .gitkeep and temporary exel files.
                filename = file_path.stem
                if filename == ".gitkeep" or filename.startswith("~$"):
                    continue

                file_paths.append(file_path)
        return file_paths

    def read_files(self) -> bool:
        """Read all account statements from the folder specified in the config.

        Returns:
            bool: Return True if everything went as expected.
        """
        paths = self.get_account_statement_paths(config.ACCOUNT_STATMENTS_PATH)

        if not paths:
            log.warning(
                "No account statement files located in %s.",
                config.ACCOUNT_STATMENTS_PATH,
            )
            return False

        for file_path in paths:
            self.read_file(file_path)

        if not bool(self):
            log.warning("Unable to import any data.")
            return False

        return True
