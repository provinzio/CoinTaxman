"""Custom EUR exchange reader."""

import csv
import datetime
import decimal
import re
from pathlib import Path
from typing import Optional

import log_config

from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time, xdecimal

log = log_config.getLogger(__name__)


class CustomEurReader(ExchangeReader):
    """Reader for custom EUR CSV files."""

    def __init__(self):
        super().__init__("custom_eur")
        self.fiat = "EUR"

    def read_file(self, file_path: Path, book) -> None:
        """Read custom EUR CSV file."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for line in reader:
                row = reader.line_num

                # Skip empty lines.
                if not line:
                    continue

                (
                    operation_type,
                    _buy_quantity,
                    buy_asset,
                    _buy_value_in_fiat,
                    _sell_quantity,
                    sell_asset,
                    _sell_value_in_fiat,
                    _fee_quantity,
                    fee_asset,
                    _fee_value_in_fiat,
                    platform,
                    _timestamp,
                    remark,
                ) = line

                # Parse data.
                try:
                    utc_time = parse_utc_time(_timestamp, "%m/%d/%Y %H:%M:%S")
                except ValueError:
                    utc_time = parse_utc_time(_timestamp, "%m/%d/%Y %H:%M:%S.%f")
                buy_quantity = xdecimal(_buy_quantity)
                buy_value_in_fiat = xdecimal(_buy_value_in_fiat)
                sell_quantity = xdecimal(_sell_quantity)
                sell_value_in_fiat = xdecimal(_sell_value_in_fiat)
                fee_quantity = xdecimal(_fee_quantity)
                fee_value_in_fiat = xdecimal(_fee_value_in_fiat)

                # ... and define which operation to add.
                add_operations: list[
                    tuple[str, decimal.Decimal, str, Optional[decimal.Decimal]]
                ] = []
                if operation_type != "Withdrawal":
                    assert buy_asset

                if operation_type not in ("Deposit", "Airdrop"):
                    assert sell_asset

                if fee_asset:
                    assert fee_quantity

                # Map operation_type to standard operation
                operation = operation_type  # or use a mapping if needed

                if buy_quantity and buy_quantity != 0:
                    add_operations.append(
                        (operation, buy_quantity, buy_asset, buy_value_in_fiat))

                if sell_quantity and sell_quantity != 0:
                    # For sell, quantity is negative
                    add_operations.append(
                        (operation, -sell_quantity, sell_asset, sell_value_in_fiat))

                if fee_quantity and fee_quantity != 0:
                    add_operations.append(
                        ("Fee", -fee_quantity, fee_asset, fee_value_in_fiat))

                for operation, change, coin, change_in_fiat in add_operations:
                    assert change
                    assert coin
                    self.append_operation(
                        book,
                        operation,
                        utc_time,
                        change,
                        coin,
                        row,
                        file_path,
                        change_in_fiat=change_in_fiat,
                    )
