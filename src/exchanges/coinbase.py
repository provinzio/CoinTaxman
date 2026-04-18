"""Coinbase exchange reader."""

import csv
import datetime
import decimal
import re
from pathlib import Path
from typing import Optional

import log_config
from database import set_price_db

from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time, xdecimal

log = log_config.getLogger(__name__)


class CoinbaseReader(ExchangeReader):
    """Reader for Coinbase CSV files."""

    def __init__(self, version: int = 1):
        super().__init__("coinbase")
        self.version = version
        self.operation_mapping = {
            "Receive": "Deposit",
            "Send": "Withdrawal",
            "Coinbase Earn": "Buy",
            "Learning Reward": "Buy",
            "Rewards Income": "Staking",
        }

    def read_file(self, file_path: Path, book) -> None:
        """Read Coinbase CSV file."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            try:
                if self.version == 4:
                    assert next(reader) == []
                    assert next(reader) == ["Transactions"]
                    assert next(reader)  # user row
                else:
                    assert next(reader)  # header line
                    assert next(reader) == []
                    assert next(reader) == []
                    assert next(reader) == []
                    assert next(reader) == ["Transactions"]
                    assert next(reader)  # user row
                    assert next(reader) == []

                fields = next(reader)
                num_columns = len(fields)
                # Coinbase export format from 2023/2024 and ongoing
                if num_columns == 11:
                    assert self.version == 4
                    assert fields == [
                        "ID",
                        "Timestamp",
                        "Transaction Type",
                        "Asset",
                        "Quantity Transacted",
                        "Price Currency",
                        "Price at Transaction",
                        "Subtotal",
                        "Total (inclusive of fees and/or spread)",
                        "Fees and/or Spread",
                        "Notes",
                    ]
                # Coinbase export format from late 2021 until 2023/2024
                elif num_columns == 10:
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
                    ] or fields == [
                        "Timestamp",
                        "Transaction Type",
                        "Asset",
                        "Quantity Transacted",
                        "Spot Price Currency",
                        "Spot Price at Transaction",
                        "Subtotal",
                        "Total (inclusive of fees and/or spread)",
                        "Fees and/or Spread",
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

                # Coinbase export format from 2023/2024 and ongoing
                if num_columns == 11:
                    (
                        _id,
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
                    _eur_spot = _eur_spot.replace("€", "")
                    _eur_subtotal = _eur_subtotal.replace("€", "")
                    _eur_total = _eur_total.replace("€", "")
                    _eur_fee = _eur_fee.replace("€", "")

                # Coinbase export format from late 2021 until 2023/2024
                elif num_columns == 10:
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
                        _eur_spot,  # Rounded price from CSV, unused
                        _eur_subtotal,  # Cost without fees
                        _eur_total,
                        _eur_fee,
                        remark,
                    ) = columns
                    _currency_spot = "EUR"

                row = reader.line_num

                # Parse data.
                if self.version == 4:
                    utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S UTC")
                else:
                    utc_time = parse_utc_time(_utc_time, "%Y-%m-%dT%H:%M:%SZ")
                operation = self.operation_mapping.get(operation, operation)
                change = force_decimal(_change)
                # `eur_subtotal` and `eur_fee` are None for withdrawals.
                eur_subtotal = xdecimal(_eur_subtotal)
                if self.version == 4:
                    change = abs(change)
                    eur_subtotal = abs(eur_subtotal) if eur_subtotal else None
                if eur_subtotal is None:
                    # Cost without fees from CSV is missing. This can happen for
                    # old transactions (<2018), event though something was bought.
                    # Calculate the `eur_subtotal` from `eur_spot`.
                    if eur_spot := xdecimal(_eur_spot):
                        eur_subtotal = eur_spot * change
                eur_fee = xdecimal(_eur_fee)

                # Validate data.
                assert operation
                assert coin
                assert change
                assert _currency_spot == "EUR"

                # Calculated price
                if eur_subtotal:
                    assert isinstance(eur_subtotal, decimal.Decimal)
                    price_calc = eur_subtotal / change
                    # Save price in our local database for later.
                    set_price_db(self.platform, coin, "EUR", utc_time, price_calc)

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
                    convert_change = force_decimal(_convert_change)
                    convert_coin = match.group("coin")

                    eur_total = force_decimal(_eur_total)
                    if self.version == 4:
                        eur_total = abs(eur_total)
                    convert_eur_spot = eur_total / convert_change

                    self.append_operation(
                        book, "Sell", utc_time, change, coin, row, file_path
                    )
                    self.append_operation(
                        book,
                        "Buy",
                        utc_time,
                        convert_change,
                        convert_coin,
                        row,
                        file_path,
                    )

                    # Save convert price in local database, too.
                    set_price_db(
                        self.platform, convert_coin, "EUR", utc_time, convert_eur_spot
                    )
                else:
                    # Add operation normally to the list.
                    self.append_operation(
                        book, operation, utc_time, change, coin, row, file_path
                    )

                    # If it's a sell, add the corresponding buy to complement
                    # the trading pair.
                    if operation == "Sell":
                        assert isinstance(eur_subtotal, decimal.Decimal)
                        self.append_operation(
                            book,
                            "Buy",
                            utc_time,
                            eur_subtotal,
                            "EUR",
                            row,
                            file_path,
                        )
                    # If it's a buy, add the corresponding sell to complement
                    # the trading pair.
                    elif operation == "Buy":
                        assert isinstance(eur_subtotal, decimal.Decimal)
                        self.append_operation(
                            book,
                            "Sell",
                            utc_time,
                            eur_subtotal,
                            "EUR",
                            row,
                            file_path,
                        )

                # Add paid fees to the list.
                if eur_fee:
                    assert isinstance(eur_fee, decimal.Decimal)
                    self.append_operation(
                        book, "Fee", utc_time, eur_fee, "EUR", row, file_path
                    )
