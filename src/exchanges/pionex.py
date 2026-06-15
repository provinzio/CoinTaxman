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
        elif filename == "position_futures.csv":
            self._read_position_futures(file_path, book)
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
            heuristic_futures_rows: list[int] = []
            skipped_futures_rows: list[int] = []
            has_position_futures = (file_path.parent / "position_futures.csv").exists()

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
                market_type = _market_type.strip().lower()

                if "future" in market_type or symbol.endswith("_PERP"):
                    if has_position_futures:
                        skipped_futures_rows.append(row)
                        continue

                    # Pionex trading exports do not expose a dedicated realized
                    # PnL column in this format. We map futures cashflow
                    # heuristically into profit/loss without touching inventory.
                    if side == "BUY":
                        op_type = "FuturesLoss"
                        heuristic_futures_rows.append(row)
                    elif side == "SELL":
                        op_type = "FuturesProfit"
                        heuristic_futures_rows.append(row)
                    elif amount < 0:
                        op_type = "FuturesLoss"
                    elif amount > 0:
                        op_type = "FuturesProfit"
                    else:
                        log.warning(
                            f"{file_path} row {row}: Unknown futures side '{side}'. "
                            "Skipping row."
                        )
                        continue

                    cashflow = abs(amount)
                    if cashflow == 0:
                        log.warning(
                            f"{file_path} row {row}: Futures cashflow is zero. "
                            "Skipping row."
                        )
                        continue

                    # Use quote coin from symbol as settlement coin.
                    symbol_parts = symbol.split("_")
                    if len(symbol_parts) < 2:
                        log.warning(
                            f"{file_path} row {row}: Could not parse symbol '{symbol}'. "
                            "Skipping row."
                        )
                        continue
                    quote_coin = symbol_parts[1]

                    self.append_operation(
                        book,
                        op_type,
                        utc_time,
                        cashflow,
                        quote_coin,
                        row,
                        file_path,
                        remark=f"Pionex futures {side} {symbol}",
                    )

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
                    continue

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

            if heuristic_futures_rows:
                log.warning(
                    "%s: Used side-based heuristic for %s Pionex futures rows: %s",
                    file_path,
                    len(heuristic_futures_rows),
                    heuristic_futures_rows[:10],
                )

            if skipped_futures_rows:
                log.info(
                    "%s: Skipped %s futures rows in trading.csv because "
                    "position_futures.csv is available: %s",
                    file_path,
                    len(skipped_futures_rows),
                    skipped_futures_rows[:10],
                )

    def _read_position_futures(self, file_path: Path, book) -> None:
        """Reads realized futures positions from Pionex."""
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for columns in reader:
                if len(columns) != 8:
                    log.warning(
                        f"{file_path}: Expected 8 columns, got {len(columns)}. "
                        "Skipping row."
                    )
                    continue

                (
                    _position_id,
                    symbol,
                    _position_side,
                    _open_time,
                    _close_time,
                    _pnl,
                    _fee,
                    _funding_fee,
                ) = columns

                row = reader.line_num
                close_time = parse_utc_time(_close_time, "%Y-%m-%d %H:%M:%S")

                symbol_parts = symbol.split("_")
                if len(symbol_parts) < 2:
                    log.warning(
                        f"{file_path} row {row}: Could not parse symbol '{symbol}'. "
                        "Skipping row."
                    )
                    continue
                settlement_coin = symbol_parts[1]

                pnl = force_decimal(_pnl)
                fee = force_decimal(_fee)
                funding_fee = force_decimal(_funding_fee)

                if pnl > 0:
                    self.append_operation(
                        book,
                        "FuturesProfit",
                        close_time,
                        pnl,
                        settlement_coin,
                        row,
                        file_path,
                        remark=f"Pionex futures position {symbol}",
                    )
                elif pnl < 0:
                    self.append_operation(
                        book,
                        "FuturesLoss",
                        close_time,
                        abs(pnl),
                        settlement_coin,
                        row,
                        file_path,
                        remark=f"Pionex futures position {symbol}",
                    )

                # A negative value is a paid fee, positive values are rebates.
                for amount, source in ((fee, "trading fee"), (funding_fee, "funding")):
                    if amount < 0:
                        self.append_operation(
                            book,
                            "Fee",
                            close_time,
                            abs(amount),
                            settlement_coin,
                            row,
                            file_path,
                            remark=f"Pionex futures {source} {symbol}",
                        )
                    elif amount > 0:
                        self.append_operation(
                            book,
                            "FuturesProfit",
                            close_time,
                            amount,
                            settlement_coin,
                            row,
                            file_path,
                            remark=f"Pionex futures {source} rebate {symbol}",
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
