"""Bitpanda exchange readers."""

import csv
import datetime
import decimal
from pathlib import Path
from typing import Optional

import config
import database
import log_config
import misc
from database import set_price_db

from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time, xdecimal

log = log_config.getLogger(__name__)


class BitpandaProTradesReader(ExchangeReader):
    """Reader for Bitpanda Pro trades CSV files."""

    def __init__(self):
        super().__init__("bitpanda_pro")

    def read_file(self, file_path: Path, book) -> None:
        """Read Bitpanda Pro trades CSV file."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # skip header
            next(reader)
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

                change = force_decimal(amount)
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
                    book, operation.title(), utc_time, change, coin, row, file_path
                )

                # Save price in our local database for later.
                price = force_decimal(_price)
                set_price_db(self.platform, coin, price_currency, utc_time, price)
                if best_price:
                    set_price_db(self.platform, coin, "BEST",
                                 utc_time, force_decimal(best_price))

                self.append_operation(
                    book,
                    "Fee",
                    utc_time,
                    force_decimal(fee),
                    fee_currency,
                    row,
                    file_path,
                )


class BitpandaReader(ExchangeReader):
    """Reader for Bitpanda CSV files."""

    def __init__(self):
        super().__init__("bitpanda")
        self.operation_mapping = {
            "deposit": "Deposit",
            "withdrawal": "Withdrawal",
            "buy": "Buy",
            "sell": "Sell",
        }

    def read_file(self, file_path: Path, book) -> None:
        """Read Bitpanda CSV file."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)
            line = next(reader)

            # skip header, there are multiple lines
            while line != [
                "Transaction ID",
                "Timestamp",
                "Transaction Type",
                "In/Out",
                "Amount Fiat",
                "Fiat",
                "Amount Asset",
                "Asset",
                "Asset market price",
                "Asset market price currency",
                "Asset class",
                "Product ID",
                "Fee",
                "Fee asset",
                "Spread",
                "Spread Currency",
            ]:
                try:
                    line = next(reader)
                except StopIteration:
                    log.error(f"{file_path}: Could not find header line.")
                    return

            for (
                _tx_id,
                csv_utc_time,
                operation,
                _inout,
                amount_fiat,
                fiat,
                amount_asset,
                asset,
                _asset_price,
                asset_price_currency,
                asset_class,
                _product_id,
                fee,
                fee_currency,
                _spread,
                _spread_currency,
            ) in reader:
                row = reader.line_num

                # make RFC3339 timestamp ISO 8601 parseable
                if csv_utc_time[-1] == "Z":
                    csv_utc_time = csv_utc_time[:-1] + "+00:00"

                # timezone information is already taken care of with this
                utc_time = datetime.datetime.fromisoformat(csv_utc_time)

                # transfer ops seem to be akin to airdrops. In my case I got a
                # CocaCola transfer, which I don't want to track. Would need to
                # be implemented if need be.
                if operation == "transfer":
                    continue

                # fail for unknown ops
                try:
                    operation = self.operation_mapping[operation]
                except KeyError:
                    log.warning(
                        f"{file_path} row {row}: Unknown operation '{operation}'. "
                        "Skipping row."
                    )
                    continue

                if operation in ["Deposit", "Withdrawal"]:
                    if operation == "Deposit":
                        change = force_decimal(amount_asset)
                        coin = asset
                    else:
                        change = force_decimal(amount_asset)
                        coin = asset
                elif operation in ["Buy", "Sell"]:
                    change = force_decimal(amount_asset)
                    coin = asset
                    change_fiat = force_decimal(amount_fiat)

                if change < 0:
                    operation = "Sell" if operation == "Buy" else "Buy"
                    change = -change

                self.append_operation(
                    book, operation, utc_time, change, asset, row, file_path
                )

                # add buy / sell operation for fiat currency
                if operation == "Buy":
                    self.append_operation(
                        book,
                        "Sell",
                        utc_time,
                        change_fiat,
                        config.FIAT,
                        row,
                        file_path,
                    )
                elif operation == "Sell":
                    self.append_operation(
                        book,
                        "Buy",
                        utc_time,
                        change_fiat,
                        config.FIAT,
                        row,
                        file_path,
                    )

                if fee != "-":
                    self.append_operation(
                        book,
                        "Fee",
                        utc_time,
                        force_decimal(fee),
                        fee_currency,
                        row,
                        file_path,
                    )
