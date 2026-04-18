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

import base64
import collections
import datetime
import decimal
import hashlib
import hmac
import json
import os
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, NamedTuple, Optional
from urllib.parse import urlencode

import requests

import config
import log_config
import misc
import transaction as tr
from core import kraken_asset_map
from database import set_price_db
from exchanges.base import ExchangeReader
from exchanges.bitget_api import BitgetApiReader
from exchanges.registry import detect_exchange_reader
from price_data import PriceData

log = log_config.getLogger(__name__)


class MissingOperation(NamedTuple):
    platform: str
    operation: str

    def repr(self) -> str:
        return f"- {self.platform}: {self.operation}"


class Book:
    # Need to track state of duplicate deposit/withdrawal entries
    # All deposits/withdrawals are held back until they occur a second time
    # Initialize non-existing fields with None once they're called
    kraken_held_ops: defaultdict[str, defaultdict[str, Any]] = defaultdict(
        lambda: defaultdict(lambda: None)
    )

    def __init__(self, price_data: PriceData) -> None:
        self.price_data = price_data

        self.operations: list[tr.Operation] = []
        self._missing_operation_mappings: set[MissingOperation] = set()

    def __bool__(self) -> bool:
        return bool(self.operations)

    def _create_operation(
        self,
        operation: str,
        utc_time: datetime.datetime,
        platform: str,
        change: decimal.Decimal,
        coin: str,
        row: int,
        file_path: Path,
        remark: Optional[str] = None,
    ) -> Optional[tr.Operation]:

        try:
            Op = getattr(tr, operation)
        except AttributeError:
            log.error(
                f"Could not recognize {operation=} from {platform=} in "
                f"{file_path=} {row=}. "
                "The operation type might have been removed or renamed. "
                "Please open an issue or PR."
            )
            self._missing_operation_mappings.add(MissingOperation(platform, operation))
            return None

        kwargs = {}
        if remark:
            kwargs["remarks"] = [remark]

        op = Op(utc_time, platform, change, coin, [row], file_path, **kwargs)
        assert isinstance(op, tr.Operation)
        return op

    def _append_operation(
        self,
        op: tr.Operation,
    ) -> None:
        # Discard operations after the `TAX_YEAR`.
        # Ignore operations which make no change.
        if op.utc_time.year <= config.TAX_YEAR and op.change != 0:
            self.operations.append(op)

    def append_operation(
        self,
        operation: str,
        utc_time: datetime.datetime,
        platform: str,
        change: decimal.Decimal,
        coin: str,
        row: int,
        file_path: Path,
        remark: Optional[str] = None,
    ) -> None:
        # Discard operations after the `TAX_YEAR`.
        # Ignore operations which make no change.
        if utc_time.year <= config.TAX_YEAR and change != 0:
            op = self._create_operation(
                operation,
                utc_time,
                platform,
                change,
                coin,
                row,
                file_path,
                remark=remark,
            )

            if op is not None:
                self._append_operation(op)

    def _get_bitget_record_types_from_env(self) -> Optional[list[str]]:
        """Parse selected Bitget API record groups from environment.

        The env var `BITGET_API_RECORD_TYPES` accepts a comma separated list,
        for example: `spot,future,margin,p2p`.
        """
        configured = os.environ.get("BITGET_API_RECORD_TYPES", "").strip()
        if not configured:
            return None
        return [value.strip() for value in configured.split(",") if value.strip()]

    def import_api_records(self) -> None:
        """Import records from configured exchange APIs."""
        if not (
            config.BITGET_API_KEY
            and config.BITGET_API_SECRET
            and config.BITGET_API_PASSPHRASE
        ):
            return

        record_types = self._get_bitget_record_types_from_env()
        bitget_reader = BitgetApiReader()
        log.info("Importing Bitget records from API for tax year %s.", config.TAX_YEAR)
        bitget_reader.import_tax_year_records(
            self,
            config.TAX_YEAR,
            record_types=record_types,
        )

    def detect_exchange(self, file_path: Path) -> Optional[ExchangeReader]:
        return detect_exchange_reader(file_path)

    def resolve_deposits(self) -> None:
        """Match withdrawals to deposits.

        A match is found when:
            A. The coin is the same  and
            B. The deposit amount is between 0.99 and 1 times the withdrawal amount.
        """
        transfer_operations = (
            op for op in self.operations if isinstance(op, (tr.Deposit, tr.Withdrawal))
        )
        # Sort deposit and withdrawal operations by time so that deposits
        # come after withdrawal.
        sorted_transfer_operations = sorted(
            transfer_operations,
            key=lambda op: (isinstance(op, tr.Deposit), op.utc_time),
        )

        tolerance = decimal.Decimal("0.99")

        def is_match(withdrawal: tr.Withdrawal, deposit: tr.Deposit) -> bool:
            # A deposit is considered a match for a withdrawal when the
            # coins are identical and the deposit amount is close to the
            # withdrawal amount.
            #
            # The tolerance accounts for small differences caused by fees,
            # rounding, or statement rounding conventions.
            return (
                withdrawal.coin == deposit.coin
                and withdrawal.change * tolerance
                <= deposit.change
                <= withdrawal.change
            )

        withdrawal_queue: list[tr.Withdrawal] = []
        unmatched_deposits: list[tr.Deposit] = []

        for op in sorted_transfer_operations:
            if op.coin == config.FIAT:
                # Do not match home fiat deposit/withdrawals.
                continue

            if isinstance(op, tr.Withdrawal):
                # Add new withdrawal to queue.
                withdrawal_queue.append(op)

            elif isinstance(op, tr.Deposit):
                try:
                    # Find a matching withdrawal for this deposit.
                    # If multiple are found, take the first (regarding utc_time).
                    match = next(w for w in withdrawal_queue if is_match(w, op))
                except StopIteration:
                    log.debug(
                        "No withdrawal matched deposit %s %s at %s on %s. "
                        "Queue length=%s coins=%s",
                        op.change,
                        op.coin,
                        op.utc_time,
                        op.platform,
                        len(withdrawal_queue),
                        sorted({w.coin for w in withdrawal_queue}),
                    )
                    unmatched_deposits.append(op)
                else:
                    # Match the found withdrawal and remove it from queue.
                    op.link = match
                    match.has_link = True
                    withdrawal_queue.remove(match)
                    log.debug(
                        "Linking withdrawal with deposit: "
                        f"{match.change} {match.coin} "
                        f"({match.platform}, {match.utc_time}) "
                        f"-> {op.change} {op.coin} "
                        f"({op.platform}, {op.utc_time})"
                    )

        if unmatched_deposits:
            log.warning(
                "Unable to match all deposits with withdrawals. "
                "Have you added all account statements? "
                "Following deposits couldn't be matched:\n"
                + (
                    "\n".join(
                        f" - {op.change} {op.coin} to {op.platform} at {op.utc_time}"
                        for op in unmatched_deposits
                    )
                )
            )
            for op in unmatched_deposits:
                op.remarks.append("Herkunft der Einzahlung unbekannt")
        if withdrawal_queue:
            log.warning(
                "Unable to match all withdrawals with deposits. "
                "Have you added all account statements? "
                "Following withdrawals couldn't be matched:\n"
                + (
                    "\n".join(
                        f" - {op.change} {op.coin} from {op.platform} at {op.utc_time}"
                        for op in withdrawal_queue
                    )
                )
            )
            for op in withdrawal_queue:
                op.remarks.append("Ziel der Auszahlung unbekannt")

        log.info("Finished withdrawal/deposit matching")

    def get_price_from_csv(self) -> None:
        """Calculate coin prices from buy/sell operations in CSV files.

        When exactly one buy and sell happend at the exact same time,
        these two operations might belong together and we can calculate
        the paid price for this transaction.
        """
        # Group operations by platform.
        for platform, platform_operations in misc.group_by(
            self.operations, "platform"
        ).items():
            # Group operations by time.
            # Look at all operations which happend at the same time.
            for timestamp, time_operations in misc.group_by(
                platform_operations, "utc_time"
            ).items():
                buytr = selltr = None
                buycount = sellcount = 0

                # Extract the buy and sell operation.
                for operation in time_operations:
                    if isinstance(operation, tr.Buy):
                        buytr = operation
                        buycount += 1
                    elif isinstance(operation, tr.Sell):
                        selltr = operation
                        sellcount += 1

                # Skip the operations of this timestamp when there aren't
                # exactly one buy and one sell operation.
                # We can only match the buy and sell operations, when there
                # are exactly one buy and one sell operation.
                if not (buycount == 1 and sellcount == 1):
                    continue

                assert isinstance(timestamp, datetime.datetime)
                assert isinstance(buytr, tr.Buy)
                assert isinstance(selltr, tr.Sell)

                # Price definition example for buying BTC with EUR:
                # Symbol: BTCEUR
                # coin: BTC (buytr.coin)
                # reference coin: EUR (selltr.coin)
                # price = traded EUR / traded BTC
                price = decimal.Decimal(selltr.change / buytr.change)

                log.debug(
                    f"Adding {buytr.coin}/{selltr.coin} price from CSV: "
                    f"{price} for {platform} at {timestamp}"
                )

                set_price_db(
                    platform,
                    buytr.coin,
                    selltr.coin,
                    timestamp,
                    price,
                    overwrite=True,
                )

    def merge_identical_operations(self) -> None:
        grouped_ops = misc.group_by(self.operations, tr.Operation.identical_columns)
        self.operations = [tr.Operation.merge(*ops) for ops in grouped_ops.values()]

    def match_fees(self) -> None:
        # Split operations in fees and other operations.
        operations = []
        all_fees: list[tr.Fee] = []

        for op in self.operations:
            if isinstance(op, tr.Fee):
                all_fees.append(op)
            else:
                operations.append(op)

        # Only keep none fee operations in book.
        self.operations = operations

        # Match fees to book operations.
        for platform, _fees in misc.group_by(all_fees, "platform").items():
            for utc_time, fees in misc.group_by(_fees, "utc_time").items():

                # Find matching transactions by platform and time.
                matching_transactions = {
                    idx: op
                    for idx, op in enumerate(self.operations)
                    if op.platform == platform and op.utc_time == utc_time
                    if isinstance(op, tr.Transaction)
                }

                # Group matching operations in dict with
                # { operation typename: list of indices }
                t_op = collections.defaultdict(list)
                for idx, op in matching_transactions.items():
                    t_op[op.type_name].append(idx)

                # Check if this is a buy/sell-pair.
                # Fees might occure by other operation types,
                # but this is currently not implemented.
                is_buy_sell_pair = all(
                    (
                        len(matching_transactions) == 2,
                        len(t_op[tr.Buy.type_name_c()]) == 1,
                        len(t_op[tr.Sell.type_name_c()]) == 1,
                    )
                )
                if is_buy_sell_pair:
                    # Fees have to be added to all buys and sells.
                    # 1. Fees on sells are the transaction cost,
                    #    which might be fully tax relevant for this sell
                    #    and which gets removed from the account balance
                    # 2. Fees on buys increase the buy-in price of the coins
                    #    which is relevant when selling these (not buying)
                    (sell_idx,) = t_op[tr.Sell.type_name_c()]
                    (buy_idx,) = t_op[tr.Buy.type_name_c()]
                    assert self.operations[sell_idx].fees is None
                    assert self.operations[buy_idx].fees is None
                    self.operations[sell_idx].fees = fees
                    self.operations[buy_idx].fees = fees
                else:
                    log.debug(
                        "Unsupported fee matching group for platform=%s utc_time=%s: "
                        "%s fees, %s transactions",
                        platform,
                        utc_time,
                        len(fees),
                        len(matching_transactions),
                    )
                    log.debug(
                        "Matching transactions: %s",
                        [
                            {
                                "type": op.type_name,
                                "coin": op.coin,
                                "change": str(op.change),
                                "remarks": op.remarks,
                            }
                            for op in matching_transactions.values()
                        ],
                    )
                    log.warning(
                        "Fee matching is not implemented for this case. "
                        "Your fees will be discarded and are not evaluated in "
                        "the tax evaluation.\n"
                        "Please create an Issue or PR.\n\n"
                        f"{matching_transactions=}\n{fees=}\n"
                    )

    def resolve_trades(self) -> None:
        # Match trades which belong together (traded at same time).
        for _, _operations in misc.group_by(self.operations, "platform").items():
            for _, matching_operations in misc.group_by(
                _operations, "utc_time"
            ).items():
                # Count matching operations by type with dict
                # { operation typename: list of operations }
                t_op = collections.defaultdict(list)
                for op in matching_operations:
                    t_op[op.type_name].append(op)

                # Check if this is a buy/sell-pair.
                # Fees might occure by other operation types,
                # but this is currently not implemented.
                is_buy_sell_pair = all(
                    (
                        len(
                            [
                                op
                                for op in matching_operations
                                if isinstance(op, tr.Transaction)
                            ]
                        )
                        == 2,
                        len(t_op[tr.Buy.type_name_c()]) == 1,
                        len(t_op[tr.Sell.type_name_c()]) == 1,
                    )
                )
                if is_buy_sell_pair:
                    # Add link that this is a trade pair.
                    (buy_op,) = t_op[tr.Buy.type_name_c()]
                    assert isinstance(buy_op, tr.Buy)
                    (sell_op,) = t_op[tr.Sell.type_name_c()]
                    assert isinstance(sell_op, tr.Sell)
                    assert buy_op.link is None
                    assert buy_op.buying_cost is None
                    buy_op.link = sell_op
                    assert sell_op.link is None
                    assert sell_op.selling_value is None
                    sell_op.link = buy_op
                    continue

                # Binance allows to convert small assets in one go to BNB.
                # Our `merge_identical_column` function merges all BNB which
                # gets bought at that time together.
                # BUG Trade connection can not be established with our current
                #     method.
                # Calculate the buying cost of this type of operation by all
                # small asset sells.
                is_binance_bnb_small_asset_transfer = all(
                    (
                        all(op.platform == "binance" for op in matching_operations),
                        len(t_op[tr.Buy.type_name_c()]) == 1,
                        len(t_op[tr.Sell.type_name_c()]) >= 1,
                        len(t_op.keys()) == 2,
                    )
                )

                if is_binance_bnb_small_asset_transfer:
                    (buy_op,) = t_op[tr.Buy.type_name_c()]
                    assert isinstance(buy_op, tr.Buy)
                    sell_ops = t_op[tr.Sell.type_name_c()]
                    assert all(isinstance(op, tr.Sell) for op in sell_ops)
                    assert buy_op.link is None
                    assert buy_op.buying_cost is None
                    buying_costs = [self.price_data.get_cost(op) for op in sell_ops]
                    buy_op.buying_cost = misc.dsum(buying_costs)
                    assert len(sell_ops) == len(buying_costs)
                    for sell_op, buying_cost in zip(sell_ops, buying_costs):
                        assert isinstance(sell_op, tr.Sell)
                        assert sell_op.link is None
                        assert sell_op.selling_value is None
                        percent = buying_cost / buy_op.buying_cost
                        sell_op.selling_value = self.price_data.get_partial_cost(
                            buy_op, percent
                        )
                    continue

    def read_file(self, file_path: Path) -> None:
        """Import transactions form an account statement.

        Detect the exchange of the file. The file will be ignored with a
        warning, if the detecting or reading functionality is not implemented.

        Args:
            file_path (Path): Path to account statement.
        """
        assert file_path.is_file()

        bitget_api_configured = all(
            (
                config.BITGET_API_KEY,
                config.BITGET_API_SECRET,
                config.BITGET_API_PASSPHRASE,
            )
        )

        if reader := self.detect_exchange(file_path):
            log.info("Reading file from exchange %s at %s", reader.platform, file_path)
            reader.read_file(file_path, self)
        elif file_path.suffix not in (
            ".zip",
            ".rar",
        ):
            lower_file_path = str(file_path).lower()
            if bitget_api_configured and "bitget" in lower_file_path:
                log.info(
                    "Skipping undetected Bitget CSV file %s because Bitget API import is enabled.",
                    file_path,
                )
                return

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
            for file_path in statements_dir.rglob("*"):
                if file_path.is_file():
                    # Ignore .gitkeep and temporary excel files.
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
            if not bool(self):
                log.warning(
                    "No account statement files located in %s.",
                    config.ACCOUNT_STATMENTS_PATH,
                )
                return False
            return True

        for file_path in paths:
            self.read_file(file_path)

        if not bool(self):
            log.warning("Unable to import any data.")
            return False

        if self._missing_operation_mappings:
            raise RuntimeError(
                "Some operations couldn't been mapped. "
                "Please adjust the operational mapping "
                "for the following exchanges/operations:\n"
                + "\n".join(
                    sorted(map(MissingOperation.repr, self._missing_operation_mappings))
                )
            )
        return True
