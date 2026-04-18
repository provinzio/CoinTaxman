"""Bitunix exchange reader."""

import csv
import datetime
import decimal
from pathlib import Path
from typing import Optional

import log_config

from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time, xdecimal

log = log_config.getLogger(__name__)


class BitunixReader(ExchangeReader):
    """Reader for Bitunix CSV files."""

    def __init__(self):
        super().__init__("bitunix")
        self.operation_mapping = {
            "Deposit": "Deposit",
            "Withdraw": "Withdrawal",
            "Spot Trade": "Trade",
            "Futures Profit": "Profit",
            "Futures Loss": "Loss",
            "Rebate (Agent)": "Commission",
        }

    def read_file(self, file_path: Path, book) -> None:
        """Read Bitunix CSV file."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for columns in reader:
                if len(columns) != 10:
                    log.warning(
                        f"{file_path}: Expected 10 columns, got {len(columns)}. "
                        "Skipping row."
                    )
                    continue

                (
                    _utc_time,
                    label,
                    outgoing_asset,
                    _outgoing_amount,
                    incoming_asset,
                    _incoming_amount,
                    fee_asset,
                    _fee_amount,
                    _trx_id,
                    _comment,
                ) = columns

                row = reader.line_num

                # Parse data.
                utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S")
                outgoing_amount = xdecimal(_outgoing_amount) or decimal.Decimal(0)
                incoming_amount = xdecimal(_incoming_amount) or decimal.Decimal(0)
                fee_amount = xdecimal(_fee_amount) or decimal.Decimal(0)

                operation = self.operation_mapping.get(label)

                if operation is None:
                    log.warning(
                        f"{file_path} row {row}: Unknown operation type '{label}'. "
                        "Skipping row."
                    )
                    continue

                # Handle different operation types
                if operation == "Deposit":
                    # Deposit: incoming_asset and incoming_amount
                    if incoming_amount > 0:
                        self.append_operation(
                            book,
                            "Deposit",
                            utc_time,
                            incoming_amount,
                            incoming_asset,
                            row,
                            file_path,
                        )
                    if fee_amount > 0:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            fee_amount,
                            fee_asset,
                            row,
                            file_path,
                        )

                elif operation == "Withdrawal":
                    # Withdrawal: outgoing_asset and outgoing_amount
                    if outgoing_amount > 0:
                        self.append_operation(
                            book,
                            "Withdrawal",
                            utc_time,
                            outgoing_amount,
                            outgoing_asset,
                            row,
                            file_path,
                        )
                    if fee_amount > 0:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            fee_amount,
                            fee_asset,
                            row,
                            file_path,
                        )

                elif operation == "Trade":
                    # Spot Trade: sell outgoing, buy incoming
                    if outgoing_amount > 0 and outgoing_asset:
                        self.append_operation(
                            book,
                            "Sell",
                            utc_time,
                            outgoing_amount,
                            outgoing_asset,
                            row,
                            file_path,
                        )

                    if incoming_amount > 0 and incoming_asset:
                        self.append_operation(
                            book,
                            "Buy",
                            utc_time,
                            incoming_amount,
                            incoming_asset,
                            row,
                            file_path,
                        )

                    if fee_amount > 0 and fee_asset:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            fee_amount,
                            fee_asset,
                            row,
                            file_path,
                        )

                elif operation == "Profit":
                    # Futures Profit: incoming amount is the profit
                    if incoming_amount > 0 and incoming_asset:
                        self.append_operation(
                            book,
                            "Profit",
                            utc_time,
                            incoming_amount,
                            incoming_asset,
                            row,
                            file_path,
                        )

                    if fee_amount > 0 and fee_asset:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            fee_amount,
                            fee_asset,
                            row,
                            file_path,
                        )

                elif operation == "Loss":
                    # Futures Loss: outgoing amount is the loss
                    # Record as a sell operation (loss is like selling at unfavorable rate)
                    if outgoing_amount > 0 and outgoing_asset:
                        self.append_operation(
                            book,
                            "Sell",
                            utc_time,
                            outgoing_amount,
                            outgoing_asset,
                            row,
                            file_path,
                        )

                    if fee_amount > 0 and fee_asset:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            fee_amount,
                            fee_asset,
                            row,
                            file_path,
                        )

                elif operation == "Commission":
                    # Rebate (Agent): incoming amount is the commission/rebate
                    if incoming_amount > 0 and incoming_asset:
                        self.append_operation(
                            book,
                            "Commission",
                            utc_time,
                            incoming_amount,
                            incoming_asset,
                            row,
                            file_path,
                        )
