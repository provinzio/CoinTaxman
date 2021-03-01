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
import logging
from pathlib import Path
from typing import Optional

import config
import misc
from price_data import PriceData
import transaction as tr

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
        change: float,
        coin: str,
        row: int,
        file_path: Path,
    ) -> None:

        try:
            Op = getattr(tr, operation)
        except AttributeError:
            log.warning(
                "Could not recognize operation `%s` in  %s file `%s:%i`.", operation, platform, file_path, row)
            return

        o = Op(utc_time, platform, change, coin, row, file_path)
        self.operations.append(o)

    def _read_binance(self, file_path: Path) -> None:
        platform = "binance"
        operation_mapping = {
            "Distribution": "Airdrop",
            "Savings Interest": "CoinLendInterest",
            "Savings purchase": "CoinLend",
            "Savings Principal redemption": "CoinLendEnd",
            "Commission History": "Commission",
            "Commission Fee Shared With You": "Commission",
            "Launchpool Interest": "StakingInterest",
        }

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for _utc_time, account, operation, coin, _change, remark in reader:
                row = reader.line_num

                # Parse data.
                utc_time = datetime.datetime.strptime(
                    _utc_time, "%Y-%m-%d %H:%M:%S")
                utc_time = utc_time.replace(tzinfo=datetime.timezone.utc)
                change = float(_change)
                operation = operation_mapping.get(operation, operation)
                if operation in (
                    "The Easiest Way to Trade",
                    "Small assets exchange BNB",
                    "Transaction Related",
                ):
                    operation = "Sell" if change < 0 else "Buy"
                change = abs(change)

                # Validate data.
                assert account == "Spot", "Other types than Spot are currently not supported. Please create an Issue or PR."
                assert operation
                assert coin
                assert change

                # Check for problems.
                if remark:
                    log.warning(
                        "I may have missed a remark in %s:%i: `%s`.", file_path, row, remark)

                self.append_operation(operation, utc_time, platform,
                                      change, coin, row, file_path)

    def _read_coinbase(self, file_path: Path) -> None:
        platform = "coinbase"
        operation_mapping = {
            "Send": "Withdraw",
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
                assert next(reader) == ['Timestamp', 'Transaction Type', 'Asset', 'Quantity Transacted',
                                        'EUR Spot Price at Transaction', 'EUR Subtotal', 'EUR Total (inclusive of fees)', 'EUR Fees', 'Notes']
            except AssertionError as e:
                msg = f"Unable to read coinbase file: Malformed header. Skipping {file_path}."
                e.args += (msg,)
                log.exception(e)
                return

            for _utc_time, operation, coin, _change, _eur_spot, _eur_subtotal, _eur_total, _eur_fee, remark in reader:
                row = reader.line_num

                # Parse data.
                utc_time = datetime.datetime.strptime(
                    _utc_time, "%Y-%m-%dT%H:%M:%SZ")
                utc_time = utc_time.replace(tzinfo=datetime.timezone.utc)
                operation = operation_mapping.get(operation, operation)
                change = float(_change)
                #  Current price from exchange.
                eur_spot = float(_eur_spot)
                #  Cost without fees.
                eur_subtotal = misc.xfloat(_eur_subtotal)
                #  Cost with fees.
                eur_total = misc.xfloat(_eur_total)
                eur_fee = misc.xfloat(_eur_fee)

                # Unused variables.
                del eur_total
                del remark

                # Validate data.
                assert operation
                assert coin
                assert change
                assert eur_spot

                self.append_operation(operation, utc_time, platform,
                                      change, coin, row, file_path)

                # Save price in our local database for later.
                self.price_data.set_price_db(
                    platform, coin, "EUR", utc_time, eur_spot)

                if operation == "Sell":
                    self.append_operation("Buy", utc_time, platform,
                                          eur_subtotal, "EUR", row, file_path)
                elif operation == "Buy":
                    self.append_operation("Sell", utc_time, platform,
                                          eur_subtotal, "EUR", row, file_path)

                if eur_fee:
                    self.append_operation("Fee", utc_time, platform,
                                          eur_fee, "EUR", row, file_path)

    def detect_exchange(self, file_path: Path) -> Optional[str]:
        if file_path.suffix == ".csv":
            with open(file_path, encoding="utf8") as f:
                reader = csv.reader(f)
                header = next(reader, None)

            expected_headers = {
                "binance": ['UTC_Time', 'Account',
                            'Operation', 'Coin', 'Change', 'Remark'],
                "coinbase": ['You can use this transaction report to inform your likely tax obligations. For US customers, Sells, Converts, and Rewards Income, and Coinbase Earn transactions are taxable events. For final tax obligations, please consult your tax advisor.'],
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
                    "Unable to read files from the exchange `%s`. Skipping `%s`.", exchange, file_path)
                return

            log.info("Reading file from exchange %s at %s",
                     exchange, file_path)
            read_file(file_path)
        else:
            log.warning(
                f"Unable to detect the exchange of file `{file_path}`. Skipping file.")

    def get_account_statement_paths(self, statements_dir: Path) -> list[Path]:
        """Return file paths of all account statements in `statements_dir`.

        Args:
            statements_dir (str): Folder in which account statements will be searched.

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
            log.warning("No account statement files located in %s.",
                        config.ACCOUNT_STATMENTS_PATH)
            return False

        for file_path in paths:
            self.read_file(file_path)

        if not bool(self):
            log.warning("Unable to import any data.")
            return False

        return True
