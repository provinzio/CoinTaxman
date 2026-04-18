"""Coinbase Pro exchange reader."""

import csv
import datetime
import decimal
from pathlib import Path
from typing import Optional

import log_config

from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time, xdecimal

log = log_config.getLogger(__name__)


class CoinbaseProReader(ExchangeReader):
    """Reader for Coinbase Pro CSV files."""

    def __init__(self):
        super().__init__("coinbase_pro")
        self.operation_mapping = {
            "BUY": "Buy",
            "SELL": "Sell",
        }

    def read_file(self, file_path: Path, book) -> None:
        """Read Coinbase Pro CSV file."""
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
                utc_time = parse_utc_time(_utc_time, "%Y-%m-%dT%H:%M:%S.%fZ")
                operation = self.operation_mapping.get(operation, operation)
                size = force_decimal(_size)
                price = force_decimal(_price)
                fee = xdecimal(_fee)
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
                    book, operation, utc_time, size, size_unit, row, file_path
                )

                if operation == "Sell":
                    self.append_operation(
                        book,
                        "Buy",
                        utc_time,
                        total_price,
                        price_fee_total_unit,
                        row,
                        file_path,
                    )
                elif operation == "Buy":
                    self.append_operation(
                        book,
                        "Sell",
                        utc_time,
                        total_price,
                        price_fee_total_unit,
                        row,
                        file_path,
                    )
                if fee:
                    self.append_operation(
                        book,
                        "Fee",
                        utc_time,
                        fee,
                        price_fee_total_unit,
                        row,
                        file_path,
                    )
