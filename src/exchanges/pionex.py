"""Pionex exchange reader."""

import csv
import datetime
import decimal
from pathlib import Path
from typing import Optional

import log_config

from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time, xdecimal

log = log_config.getLogger(__name__)


class PionexReader(ExchangeReader):
    """Reader for Pionex CSV files."""

    def __init__(self):
        super().__init__("pionex")

    def read_file(self, file_path: Path, book) -> None:
        """Read Pionex CSV file by dispatching based on filename."""
        filename = file_path.name

        if filename == "deposit-withdraw.csv":
            self._read_deposit_withdraw(file_path, book)
        elif filename == "trading.csv":
            self._read_trading(file_path, book)
        elif filename == "staking.csv":
            self._read_staking(file_path, book)
        elif filename == "others.csv":
            self._read_others(file_path, book)
        else:
            log.warning(
                f"Unknown Pionex file format: {filename}. "
                "Skipping file."
            )

    def _read_deposit_withdraw(self, file_path: Path, book) -> None:
        """Reads deposit/withdrawal records from Pionex."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for columns in reader:
                if len(columns) != 7:
                    log.warning(
                        f"{file_path}: Expected 7 columns, got {len(columns)}. "
                        "Skipping row."
                    )
                    continue

                (
                    _utc_time,
                    tx_type,
                    _amount,
                    coin,
                    _network,
                    _txid,
                    _fee,
                ) = columns

                row = reader.line_num

                # Parse data.
                utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S")
                amount = force_decimal(_amount)
                fee = xdecimal(_fee) or decimal.Decimal(0)

                # Map transaction type
                if tx_type == "DEPOSIT":
                    self.append_operation(
                        book,
                        "Deposit",
                        utc_time,
                        amount,
                        coin,
                        row,
                        file_path,
                    )
                elif tx_type == "WITHDRAW":
                    self.append_operation(
                        book,
                        "Withdrawal",
                        utc_time,
                        amount,
                        coin,
                        row,
                        file_path,
                    )
                    # Add withdrawal fee if present
                    if fee > 0:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            fee,
                            coin,
                            row,
                            file_path,
                        )
                else:
                    log.warning(
                        f"{file_path} row {row}: Unknown tx_type '{tx_type}'. "
                        "Skipping row."
                    )

    def _read_trading(self, file_path: Path, book) -> None:
        """Reads trading records from Pionex (spot and futures)."""
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
                    _executed_qty,
                    _amount,
                    _price,
                    side,
                    symbol,
                    _fee,
                    fee_coin,
                    _market_type,
                    _tax_id,
                ) = columns

                row = reader.line_num

                # Parse data.
                utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S")
                executed_qty = force_decimal(_executed_qty)
                amount = force_decimal(_amount)
                fee = force_decimal(_fee)

                # Extract base and quote coin from symbol (e.g. DOT_USDT_PERP -> DOT, USDT)
                # Format: BASE_QUOTE or BASE_QUOTE_PERP
                symbol_parts = symbol.split("_")
                if len(symbol_parts) >= 2:
                    base_coin = symbol_parts[0]
                    quote_coin = symbol_parts[1]
                else:
                    log.warning(
                        f"{file_path} row {row}: Could not parse symbol '{symbol}'. "
                        "Skipping row."
                    )
                    continue

                # Record buy/sell transactions
                if side == "BUY":
                    # Buying: spending quote coin, getting base coin
                    self.append_operation(
                        book,
                        "Buy",
                        utc_time,
                        executed_qty,
                        base_coin,
                        row,
                        file_path,
                    )
                    self.append_operation(
                        book,
                        "Sell",
                        utc_time,
                        amount,
                        quote_coin,
                        row,
                        file_path,
                    )
                elif side == "SELL":
                    # Selling: spending base coin, getting quote coin
                    self.append_operation(
                        book,
                        "Sell",
                        utc_time,
                        executed_qty,
                        base_coin,
                        row,
                        file_path,
                    )
                    self.append_operation(
                        book,
                        "Buy",
                        utc_time,
                        amount,
                        quote_coin,
                        row,
                        file_path,
                    )
                else:
                    log.warning(
                        f"{file_path} row {row}: Unknown side '{side}'. "
                        "Skipping row."
                    )
                    continue

                # Add trading fee if present
                if fee > 0:
                    self.append_operation(
                        book,
                        "Fee",
                        utc_time,
                        fee,
                        fee_coin,
                        row,
                        file_path,
                    )

    def _read_staking(self, file_path: Path, book) -> None:
        """Reads staking records from Pionex."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for columns in reader:
                if len(columns) != 6:
                    log.warning(
                        f"{file_path}: Expected 6 columns, got {len(columns)}. "
                        "Skipping row."
                    )
                    continue

                (
                    _utc_time,
                    _received_qty,
                    received_currency,
                    _sent_qty,
                    sent_currency,
                    _tag,
                ) = columns

                row = reader.line_num

                # Skip empty rows
                if not _received_qty.strip() or _received_qty == "0":
                    continue

                # Parse data.
                utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S")
                received_qty = xdecimal(_received_qty) or decimal.Decimal(0)
                sent_qty = xdecimal(_sent_qty) or decimal.Decimal(0)

                # Staking record: sent asset (being staked) and received interest
                if sent_qty > 0 and sent_currency:
                    self.append_operation(
                        book,
                        "Staking",
                        utc_time,
                        sent_qty,
                        sent_currency,
                        row,
                        file_path,
                    )

                if received_qty > 0 and received_currency:
                    self.append_operation(
                        book,
                        "StakingInterest",
                        utc_time,
                        received_qty,
                        received_currency,
                        row,
                        file_path,
                    )

    def _read_others(self, file_path: Path, book) -> None:
        """Reads other transaction records from Pionex (fees, funding, etc)."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for columns in reader:
                if len(columns) != 5:
                    log.warning(
                        f"{file_path}: Expected 5 columns, got {len(columns)}. "
                        "Skipping row."
                    )
                    continue

                (
                    _utc_time,
                    coin,
                    _amount,
                    tag,
                    _comment,
                ) = columns

                row = reader.line_num

                # Parse data.
                utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S")
                amount = force_decimal(_amount)
                amount_abs = abs(amount)

                # Handle different tag types
                if tag == "FundingFee":
                    # Funding fees are costs, record as Fee
                    if amount_abs > 0:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            amount_abs,
                            coin,
                            row,
                            file_path,
                        )
                elif tag == "Commission":
                    # Commission/rewards
                    if amount > 0:
                        self.append_operation(
                            book,
                            "Commission",
                            utc_time,
                            amount,
                            coin,
                            row,
                            file_path,
                        )
                elif tag == "Airdrop":
                    # Airdrop/reward distribution
                    if amount > 0:
                        self.append_operation(
                            book,
                            "Airdrop",
                            utc_time,
                            amount,
                            coin,
                            row,
                            file_path,
                        )
                else:
                    # Log unknown tags for reference
                    if amount > 0:
                        log.debug(
                            f"{file_path} row {row}: Unknown tag '{tag}' "
                            f"with amount {amount}. Recording as income."
                        )
                        self.append_operation(
                            book,
                            "Airdrop",
                            utc_time,
                            amount,
                            coin,
                            row,
                            file_path,
                        )
