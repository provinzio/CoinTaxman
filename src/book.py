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

import collections
import csv
import datetime
import logging
from pathlib import Path
import re
from typing import Optional

import openpyxl

import config
import transaction as tr

log = logging.getLogger(__name__)


class Book:

    def __init__(self) -> None:
        self.operations: list[tr.Operation] = []

    def __bool__(self) -> bool:
        return bool(self.operations)

    def _read_binance(self, file_path: Path) -> None:
        platform = "binance"
        operation_mapping = {
            "Distribution": "Airdrop",
            "Savings Interest": "CoinLendInterest",
            "Savings purchase": "CoinLend",
            "Savings Principal redemption": "CoinLendEnd",
        }

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for row, (utc_time, account, operation, coin, change, remark) in enumerate(reader, 2):
                # Parse data.
                utc_time = datetime.datetime.strptime(
                    utc_time, "%Y-%m-%d %H:%M:%S")
                change = abs(float(change))
                operation = operation_mapping.get(operation, operation)

                # Validate data.
                assert account == "Spot"
                assert operation
                assert coin
                assert change

                # Check for problems.
                if remark:
                    log.warning(
                        "I may missed a remark in %s:%i: `%s`.", file_path, row, remark)

                # Append operation to the correct list.
                try:
                    Operation = getattr(tr, operation)
                except AttributeError:
                    log.warning(
                        "Could not recognize operation `%s` in binance file `%s`.", operation, file_path)
                    continue

                o = Operation(utc_time, platform, change, coin)
                self.operations.append(o)

    # def _read_etoro(self, file_path: Path) -> None:
    #     platform = "etoro"
    #     etoro_fiat = "USD"
    #     wb = openpyxl.load_workbook(filename=file_path, read_only=True)
    #     ws = wb["Transactions Report"]

    #     deposit_regex = re.compile("(\d+(\.\d+)?) (\W+) (.*)")

    #     for row, (utc_time, account_balance, operation, details, position_id, amount, realized_equity_change, realized_equity, nwa) in enumerate(ws.iter_rows(min_row=2, values_only=True), 2):
    #         # Parse data.
    #         utc_time = datetime.datetime.strptime(
    #             utc_time, "%Y-%m-%d %H:%M:%S")
    #         account_balance = float(account_balance)
    #         position_id = misc.xint(position_id)
    #         amount = float(amount)
    #         realized_equity_change = float(realized_equity_change)
    #         realized_equity = float(realized_equity)

    #         # Validate data.
    #         assert amount

    #         # Append fees.

    #         # Append operation to the correct list.
    #         if operation == "Deposit":
    #             m = deposit_regex.match(details)
    #             assert m
    #             # TODO fees depending on description (paypal/wiretransfer)
    #             change, coin, description = m.groups()
    #             if coin == etoro_fiat:
    #                 deposit = tr.Deposit(utc_time, platform, change)
    #                 self.operations[etoro_fiat] = deposit
    #             else:
    #                 sell = tr.Sell(utc_time, platform, -change)
    #                 self.operations[coin].append(sell)
    #                 buy = tr.Buy(utc_time, platform, amount)
    #                 self.operations[etoro_fiat].append(buy)

    #         elif operation == "Open Position":
    #             buy_coin, sell_coin = details.split("/")

    #             # TODO amount in etoro fiat. calc how many
    #             if buy_coin == etoro_fiat:
    #                 buy_change = amount
    #             else:
    #                 # TODO get pricedata
    #             if sell_coin == etoro_fiat:
    #                 sell_change = amount
    #             else:
    #                 # TODO get pricedata

    #             sell = tr.Sell(utc_time, platform, -sell_change)
    #             self.operations[buy_coin].append(sell)
    #             buy = tr.Buy(utc_time, platform, buy_change)
    #             self.operations[sell_coin].append(buy)

    #     wb.close()

    def detect_exchange(self, file_path: Path) -> Optional[str]:
        if file_path.suffix == ".csv":
            with open(file_path, encoding="utf8") as f:
                reader = csv.reader(f)
                header = next(reader, None)

            expected_headers = {
                "binance": ['UTC_Time', 'Account',
                            'Operation', 'Coin', 'Change', 'Remark']
            }
            for exchange, expected in expected_headers.items():
                if header == expected:
                    return exchange

        elif file_path.suffix == ".xlsx":
            wb = openpyxl.load_workbook(filename=file_path, read_only=True)
            sheets = wb.sheetnames
            sheet_headers = {
                sheet: next(wb[sheet].iter_rows(values_only=True), None)
                for sheet in sheets
            }
            wb.close()

            expected_sheet_headers = {
                "etoro": {
                    'Account Details': ('Details', None),
                    'Closed Positions': ('Position ID', 'Action', 'Copy Trader Name', 'Amount', 'Units', 'Open Rate', 'Close Rate', 'Spread', 'Profit', 'Open Date', 'Close Date', 'Take Profit Rate', 'Stop Loss Rate', 'Rollover Fees And Dividends', 'Is Real', 'Leverage', 'Notes'),
                    'Transactions Report': ('Date', 'Account Balance', 'Type', 'Details', 'Position ID', 'Amount', 'Realized Equity Change', 'Realized Equity', 'NWA'),
                    'Financial Summary': ('Figure', 'Amount in USD', 'Tax Rate'),
                },
            }
            for exchange, expected in expected_sheet_headers.items():
                if sheet_headers == expected:
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

    def get_account_statement_paths(self, statements_dir: str) -> list[Path]:
        """Return file paths of all account statements in `statements_dir`.

        Args:
            statements_dir (str): Folder in which account statements will be searched.

        Returns:
            list[Path]: List of account statement file paths.
        """
        statements_dir = Path(statements_dir)
        assert statements_dir.is_dir()

        file_paths: list[Path] = []
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
        paths = self.get_account_statement_paths(config.ACCOUNT_STATMENTS_DIR)

        if not paths:
            log.warning("No account statement files in %s located.",
                        config.ACCOUNT_STATMENTS_DIR)
            return False

        for file_path in paths:
            self.read_file(file_path)

        if not bool(self):
            log.warning("Unable to import any data.")
            return False

        return True
