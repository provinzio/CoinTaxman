"""Bitget CSV export reader."""

import csv
import datetime
import decimal
import re
from pathlib import Path
from typing import Optional

import config
import log_config

from .base import ExchangeReader
from .bitget_api import BitgetApiReader
from .utils import force_decimal

log = log_config.getLogger(__name__)


class BitgetCsvReader(ExchangeReader):
    """Reader for Bitget CSV exports as an alternative to API import."""

    def __init__(self):
        super().__init__("bitget")
        # Reuse proven type-mapping logic from the API reader.
        self._api_mapper = BitgetApiReader()

    def read_file(self, file_path: Path, book) -> None:
        filename = file_path.name.lower()

        if "withdrawal records" in filename:
            self._read_deposit_withdrawal_records(file_path, book)
            return

        if "spot transactions" in filename:
            self._read_spot_transactions(file_path, book)
            return

        if "futures transactions" in filename:
            self._read_futures_transactions(file_path, book)
            return

        if "margin transactions" in filename:
            self._read_margin_transactions(file_path, book)
            return

        if "spot order details" in filename:
            self._read_spot_order_details(file_path, book)
            return

        if "spot order history" in filename:
            self._read_spot_order_history(file_path, book)
            return

        if "margin order history" in filename:
            self._read_margin_order_history(file_path, book)
            return

        if "onchain transactions" in filename:
            self._read_onchain_transactions(file_path, book)
            return

        if "onchain history" in filename:
            self._read_onchain_history(file_path, book)
            return

        if "transactions of unified trading account" in filename:
            self._read_unified_account_transactions(file_path, book)
            return

        if "order history of unified trading account" in filename:
            self._read_unified_account_order_history(file_path, book)
            return

        if "position history of unified trading account" in filename:
            self._read_unified_account_position_history(file_path, book)
            return

        if "convert history of unified trading account" in filename:
            self._read_unified_account_convert_history(file_path, book)
            return

        if "small balance conversion history" in filename:
            self._read_small_balance_conversion_history(file_path, book)
            return

        if "futures order details" in filename:
            self._read_futures_order_details(file_path, book)
            return

        if "futures order history" in filename:
            self._read_futures_order_history(file_path, book)
            return

        if "futures position history" in filename:
            self._read_futures_position_history(file_path, book)
            return

        if "earn" in filename:
            self._read_earn_records(file_path, book)
            return

        log.warning("Bitget CSV file type not supported yet: %s", file_path)

    def _parse_utc_time(self, value: str) -> datetime.datetime:
        return datetime.datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=datetime.timezone.utc
        )

    def _parse_amount_and_coin(self, value: str) -> tuple[decimal.Decimal, str]:
        amount_str, coin = value.strip().split(maxsplit=1)
        return force_decimal(amount_str), coin.strip()

    def _parse_concatenated_amount_and_coin(
        self, value: str
    ) -> tuple[decimal.Decimal, str]:
        match = re.fullmatch(
            r"\s*([+-]?[0-9]+(?:\.[0-9]+)?)\s*([A-Za-z0-9._-]+)\s*", value)
        if not match:
            raise ValueError(f"Unable to parse amount+coin value: {value!r}")
        amount_str, coin = match.groups()
        return force_decimal(amount_str), coin

    def _normalize_future_direction(self, direction: str) -> str:
        return direction.strip().lower().replace(" ", "_")

    def _has_sibling_filename(self, file_path: Path, needle: str) -> bool:
        needle = needle.lower()
        return any(
            sibling.is_file() and needle in sibling.name.lower()
            for sibling in file_path.parent.iterdir()
            if sibling != file_path
        )

    def _append_trade_pair(
        self,
        book,
        utc_time: datetime.datetime,
        direction: str,
        base_coin: str,
        base_amount: decimal.Decimal,
        quote_coin: str,
        quote_amount: decimal.Decimal,
        row_num: int,
        file_path: Path,
        remark: str,
    ) -> None:
        normalized = direction.strip().lower()
        if normalized == "buy":
            first_operation = ("Buy", base_amount, base_coin)
            second_operation = ("Sell", quote_amount, quote_coin)
        elif normalized == "sell":
            first_operation = ("Sell", base_amount, base_coin)
            second_operation = ("Buy", quote_amount, quote_coin)
        else:
            raise ValueError(f"Unknown trade direction: {direction!r}")

        for operation, amount, coin in (first_operation, second_operation):
            if amount:
                self.append_operation(
                    book,
                    operation,
                    utc_time,
                    abs(amount),
                    coin,
                    row_num,
                    file_path,
                    remark=remark,
                )

    def _append_realized_future_result(
        self,
        book,
        direction: str,
        utc_time: datetime.datetime,
        coin: str,
        realized_pnl: decimal.Decimal,
        fee: decimal.Decimal,
        row_num: int,
        file_path: Path,
        remark: str,
    ) -> None:
        normalized_direction = self._normalize_future_direction(direction)
        operation = self._api_mapper._map_future_tax_type(normalized_direction)
        if operation is not None and realized_pnl:
            self.append_operation(
                book,
                operation,
                utc_time,
                abs(realized_pnl),
                coin,
                row_num,
                file_path,
                remark=remark,
            )
        self._append_fee_if_present(
            book,
            abs(fee),
            utc_time,
            coin,
            row_num,
            file_path,
            operation or "Fee",
        )

    def _append_fee_if_present(
        self,
        book,
        fee: decimal.Decimal,
        utc_time: datetime.datetime,
        coin: str,
        row_num: int,
        file_path: Path,
        operation: str,
    ) -> None:
        if fee and operation not in ("Fee", "Commission"):
            self.append_operation(
                book,
                "Fee",
                utc_time,
                fee,
                coin,
                row_num,
                file_path,
            )

    def _map_simple_earn_type(self, event_type: str) -> Optional[str]:
        normalized = event_type.strip().lower()
        mapping = {
            "subscription": "CoinLend",
            "redemption": "CoinLendEnd",
            "interest": "CoinLendInterest",
            "penalty interest": "Fee",
        }
        return mapping.get(normalized)

    def _map_onchain_earn_type(self, event_type: str) -> Optional[str]:
        normalized = event_type.strip().lower()
        if normalized == "staking":
            return "Staking"
        if "redemption" in normalized:
            return "StakingEnd"
        return None

    def _map_structured_earn_type(self, event_type: str) -> Optional[str]:
        normalized = event_type.strip().lower()
        if "subscription" in normalized:
            return "CoinLend"
        if "settlement" in normalized or "redemption" in normalized:
            return "CoinLendEnd"
        if "profit" in normalized or "interest" in normalized:
            return "CoinLendInterest"
        return None

    def _read_deposit_withdrawal_records(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            for row_num, row in enumerate(reader, start=2):
                status = (row.get("Status") or "").strip().lower()
                if status and status not in ("successful", "completed"):
                    continue

                tx_type = (row.get("Type") or "").strip().lower()
                if tx_type == "deposit":
                    operation = "Deposit"
                elif tx_type == "withdraw":
                    operation = "Withdrawal"
                else:
                    log.warning(
                        "%s row %s: Unknown Bitget deposit/withdraw type '%s'. Skipping.",
                        file_path,
                        row_num,
                        row.get("Type", ""),
                    )
                    continue

                coin = (row.get("Coin") or "").strip()
                if not coin:
                    continue

                quantity = abs(force_decimal(row.get("Quantity", "0")))
                tx_id = (row.get("TxID") or "").strip()
                remark = f"Bitget {tx_type} {tx_id}".strip()

                self.append_operation(
                    book,
                    operation,
                    self._parse_utc_time(row.get("Date", "")),
                    quantity,
                    coin,
                    row_num,
                    file_path,
                    remark=remark,
                )

    def _read_spot_transactions(self, file_path: Path, book) -> None:
        has_withdrawal_records = self._has_sibling_filename(
            file_path, "withdrawal records"
        )

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            for row_num, row in enumerate(reader, start=2):
                tax_type = (row.get("Type") or "").strip()
                amount_raw = force_decimal(row.get("Amount", "0"))
                fee = abs(force_decimal(row.get("Fee", "0")))

                operation = self._api_mapper._map_spot_tax_type(tax_type)

                # Bitget often exports on-chain withdrawals in both
                # "spot transactions" and "withdrawal records" CSVs.
                # Prefer the dedicated withdrawal export to avoid duplicates.
                if has_withdrawal_records and tax_type == "Ordinary Withdrawal":
                    log.info(
                        "%s row %s: Ignoring Bitget spot type '%s' because withdrawal records export is available.",
                        file_path,
                        row_num,
                        tax_type,
                    )
                    continue

                if operation is None and tax_type == "Fiat":
                    # "Fiat" rows represent spot buy/sell legs (e.g. EUR -> USDT),
                    # not external wallet transfers.
                    if amount_raw > 0:
                        operation = "Buy"
                    elif amount_raw < 0:
                        operation = "Sell"

                if operation is None and self._api_mapper._is_internal_spot_transfer_tax_type(tax_type):
                    log.info(
                        "%s row %s: Ignoring Bitget spot type '%s' as internal transfer.",
                        file_path,
                        row_num,
                        tax_type,
                    )
                    continue

                if operation is None and tax_type in (
                    "Opening of trading bot position",
                    "Closing of trading bot position",
                ):
                    log.info(
                        "%s row %s: Ignoring Bitget spot type '%s'.",
                        file_path,
                        row_num,
                        tax_type,
                    )
                    continue

                if operation is None:
                    log.warning(
                        "%s row %s: Unknown Bitget spot type '%s'. Skipping.",
                        file_path,
                        row_num,
                        tax_type,
                    )
                    continue

                coin = (row.get("Coin") or "").strip() or "UNKNOWN"
                order_id = (row.get("order") or "").strip()
                remark = f"Bitget spot {tax_type} {order_id}".strip()

                self.append_operation(
                    book,
                    operation,
                    self._parse_utc_time(row.get("Date", "")),
                    abs(amount_raw),
                    coin,
                    row_num,
                    file_path,
                    remark=remark,
                )

                self._append_fee_if_present(
                    book,
                    fee,
                    self._parse_utc_time(row.get("Date", "")),
                    coin,
                    row_num,
                    file_path,
                    operation,
                )

    def _read_futures_transactions(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            for row_num, row in enumerate(reader, start=2):
                tax_type = (row.get("Type") or "").strip()
                amount_raw = force_decimal(row.get("Amount", "0"))
                fee = abs(force_decimal(row.get("Fee", "0")))

                operation = self._api_mapper._map_future_tax_type(tax_type)
                if operation is None and tax_type.lower().startswith("adjust_margin"):
                    if amount_raw > 0:
                        operation = "Deposit"
                    elif amount_raw < 0:
                        operation = "Withdrawal"

                if operation is None:
                    log.warning(
                        "%s row %s: Unknown Bitget futures type '%s'. Skipping.",
                        file_path,
                        row_num,
                        tax_type,
                    )
                    continue

                if operation == "FuturesPnlSigned":
                    if amount_raw > 0:
                        operation = "FuturesProfit"
                    elif amount_raw < 0:
                        operation = "FuturesLoss"
                    else:
                        continue

                coin = (row.get("Coin") or "").strip() or "UNKNOWN"
                order_id = (row.get("Order") or "").strip()
                futures_symbol = (row.get("Futures") or "").strip()
                remark = f"Bitget futures {tax_type} {futures_symbol} {order_id}".strip(
                )

                self.append_operation(
                    book,
                    operation,
                    self._parse_utc_time(row.get("Date", "")),
                    abs(amount_raw),
                    coin,
                    row_num,
                    file_path,
                    remark=remark,
                )

                self._append_fee_if_present(
                    book,
                    fee,
                    self._parse_utc_time(row.get("Date", "")),
                    coin,
                    row_num,
                    file_path,
                    operation,
                )

    def _read_margin_transactions(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            for row_num, row in enumerate(reader, start=2):
                tax_type = (row.get("Type") or "").strip()
                amount_raw = force_decimal(row.get("Amount", "0"))
                fee = abs(force_decimal(row.get("fee", "0")))

                operation = self._api_mapper._map_margin_tax_type(tax_type)
                if operation is None and tax_type.startswith("transfer"):
                    if amount_raw > 0:
                        operation = "Deposit"
                    elif amount_raw < 0:
                        operation = "Withdrawal"

                if operation is None:
                    log.warning(
                        "%s row %s: Unknown Bitget margin type '%s'. Skipping.",
                        file_path,
                        row_num,
                        tax_type,
                    )
                    continue

                coin = (row.get("Coin") or "").strip() or "UNKNOWN"
                pair = (row.get("Pair") or "").strip()
                remark = f"Bitget margin {tax_type} {pair}".strip()

                self.append_operation(
                    book,
                    operation,
                    self._parse_utc_time(row.get("Time", "")),
                    abs(amount_raw),
                    coin,
                    row_num,
                    file_path,
                    remark=remark,
                )

                self._append_fee_if_present(
                    book,
                    fee,
                    self._parse_utc_time(row.get("Time", "")),
                    coin,
                    row_num,
                    file_path,
                    operation,
                )

    def _read_spot_order_details(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "spot transactions"):
            log.info(
                "Skipping Bitget spot order details %s because spot transactions export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                utc_time = self._parse_utc_time(row.get("Date", ""))
                direction = (row.get("Direction") or "").strip()
                base_coin = (row.get("Base Asset") or "").strip()
                quote_coin = (row.get("Quote Asset") or "").strip()
                base_amount = abs(force_decimal(row.get("Amount", "0")))
                quote_amount = abs(force_decimal(row.get("Total", "0")))
                fee = abs(force_decimal(row.get("Fee", "0")))
                fee_coin = (row.get("Fee Coin") or base_coin or quote_coin).strip()
                pair = (row.get("Trading pair") or "").strip()
                remark = f"Bitget spot order detail {pair}".strip()

                self._append_trade_pair(
                    book,
                    utc_time,
                    direction,
                    base_coin,
                    base_amount,
                    quote_coin,
                    quote_amount,
                    row_num,
                    file_path,
                    remark,
                )
                self._append_fee_if_present(
                    book,
                    fee,
                    utc_time,
                    fee_coin,
                    row_num,
                    file_path,
                    "Trade",
                )

    def _read_spot_order_history(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "spot transactions") or self._has_sibling_filename(file_path, "spot order details"):
            log.info(
                "Skipping Bitget spot order history %s because a more detailed spot export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                status = (row.get("Status") or "").strip().lower()
                if status and status != "fully executed":
                    continue
                self._append_trade_pair(
                    book,
                    self._parse_utc_time(row.get("Date", "")),
                    (row.get("Direction") or "").strip(),
                    (row.get("Base Asset") or "").strip(),
                    abs(force_decimal(row.get("Executed", "0"))),
                    (row.get("Quote Asset") or "").strip(),
                    abs(force_decimal(row.get("Trading volume", "0"))),
                    row_num,
                    file_path,
                    f"Bitget spot order history {(row.get('Order Id') or '').strip()}".strip(
                    ),
                )

    def _read_margin_order_history(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "margin transactions"):
            log.info(
                "Skipping Bitget margin order history %s because margin transactions export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                status = (row.get("Status") or "").strip().lower()
                if status and status != "fully executed":
                    continue
                self._append_trade_pair(
                    book,
                    self._parse_utc_time(row.get("Date", "")),
                    (row.get("Direction") or "").strip(),
                    (row.get("Base Asset") or "").strip(),
                    abs(force_decimal(row.get("Executed", "0"))),
                    (row.get("Quote Asset") or "").strip(),
                    abs(force_decimal(row.get("Trading volume", "0"))),
                    row_num,
                    file_path,
                    f"Bitget margin order history {(row.get('Order ID') or '').strip()}".strip(
                    ),
                )

    def _read_onchain_transactions(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            for row_num, row in enumerate(reader, start=2):
                event_type = (row.get("Type") or "").strip()
                quantity = force_decimal(row.get("Quantity", "0"))
                operation = self._api_mapper._map_spot_tax_type(event_type)

                if operation is None and event_type.lower().startswith("transfer"):
                    operation = "Deposit" if quantity > 0 else "Withdrawal"

                if operation is None:
                    log.warning(
                        "%s row %s: Unknown Bitget onchain type '%s'. Skipping.",
                        file_path,
                        row_num,
                        event_type,
                    )
                    continue

                coin = (row.get("Coin") or "").strip() or "UNKNOWN"
                remark = f"Bitget onchain {event_type}".strip()
                self.append_operation(
                    book,
                    operation,
                    self._parse_utc_time(row.get("Time", "")),
                    abs(quantity),
                    coin,
                    row_num,
                    file_path,
                    remark=remark,
                )

    def _read_onchain_history(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            first_row = next(reader, None)

        if first_row is not None:
            log.info(
                "Bitget onchain history currently has no safe tax mapping and is ignored: %s",
                file_path,
            )

    def _read_unified_account_transactions(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            for row_num, row in enumerate(reader, start=2):
                event_type = (row.get("Transaction Type") or "").strip()
                amount = force_decimal(row.get("Amount", "0"))
                fee = abs(force_decimal(row.get("Fee", "0")))
                operation = self._api_mapper._map_future_tax_type(event_type)

                if operation is None:
                    operation = self._api_mapper._map_spot_tax_type(event_type)

                if operation is None and event_type.lower().startswith("adjust_margin"):
                    operation = "Deposit" if amount > 0 else "Withdrawal"

                if operation is None and event_type.lower().startswith("transfer"):
                    operation = "Deposit" if amount > 0 else "Withdrawal"

                if operation is None:
                    log.warning(
                        "%s row %s: Unknown Bitget unified transaction type '%s'. Skipping.",
                        file_path,
                        row_num,
                        event_type,
                    )
                    continue

                if operation == "FuturesPnlSigned":
                    if amount > 0:
                        operation = "FuturesProfit"
                    elif amount < 0:
                        operation = "FuturesLoss"
                    else:
                        continue

                coin = (row.get("Coin") or "").strip() or "UNKNOWN"
                utc_time = self._parse_utc_time(row.get("Date", ""))
                order_id = (row.get("Order ID") or "").strip()
                trading_pair = (row.get("Trading Pair") or "").strip()
                trade_type = (row.get("Trade Type") or "").strip()
                remark = (
                    f"Bitget unified {trade_type} {event_type} {trading_pair} {order_id}"
                ).strip()

                self.append_operation(
                    book,
                    operation,
                    utc_time,
                    abs(amount),
                    coin,
                    row_num,
                    file_path,
                    remark=remark,
                )
                self._append_fee_if_present(
                    book,
                    fee,
                    utc_time,
                    coin,
                    row_num,
                    file_path,
                    operation,
                )

    def _read_unified_account_convert_history(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)

            for row_num, row in enumerate(reader, start=2):
                utc_time = self._parse_utc_time(row.get("Date", ""))
                order_id = (row.get("Order ID") or "").strip()
                sell_coin = (row.get("Coin to Sell") or "").strip()
                buy_coin = (row.get("Coin to Buy") or "").strip()
                sell_quantity = abs(force_decimal(row.get("Sell Quantity", "0")))
                buy_quantity = abs(force_decimal(row.get("Buy Quantity", "0")))
                remark = f"Bitget convert {order_id}".strip()

                if sell_coin and sell_quantity:
                    self.append_operation(
                        book,
                        "Sell",
                        utc_time,
                        sell_quantity,
                        sell_coin,
                        row_num,
                        file_path,
                        remark=remark,
                    )

    def _read_unified_account_order_history(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "transactions of unified trading account"):
            log.info(
                "Skipping Bitget unified order history %s because unified transactions export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                status = (row.get("Status") or "").strip().lower()
                if status and status != "fully executed":
                    continue
                direction = (row.get("Direction") or "").strip()
                if direction.lower() not in ("buy", "sell"):
                    continue
                utc_time = self._parse_utc_time(row.get("Date", ""))
                self._append_trade_pair(
                    book,
                    utc_time,
                    direction,
                    (row.get("Base Asset") or "").strip(),
                    abs(force_decimal(row.get("Executed", "0"))),
                    (row.get("Quote Asset") or "").strip(),
                    abs(force_decimal(row.get("Trading Volume", "0"))),
                    row_num,
                    file_path,
                    f"Bitget unified order history {(row.get('Order ID') or '').strip()}".strip(
                    ),
                )
                fee = abs(force_decimal(row.get("Fee", "0")))
                self._append_fee_if_present(
                    book,
                    fee,
                    utc_time,
                    (row.get("Quote Asset") or row.get("Base Asset") or "UNKNOWN").strip(),
                    row_num,
                    file_path,
                    "Trade",
                )

    def _read_unified_account_position_history(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "transactions of unified trading account"):
            log.info(
                "Skipping Bitget unified position history %s because unified transactions export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                realized_raw = (row.get("Position PnL") or row.get(
                    "Realized PnL") or "").strip()
                if not realized_raw:
                    continue
                realized_pnl, coin = self._parse_concatenated_amount_and_coin(
                    realized_raw)
                if not realized_pnl:
                    continue
                futures = (row.get("Trading Pair") or row.get(
                    "Futures") or "").strip().lower()
                direction = "close_long" if "long" in futures else "close_short"
                fee_total = decimal.Decimal(0)
                for fee_key in ("Funding Fee", "Opening Fee", "Closing Fee", "Fees"):
                    fee_value = (row.get(fee_key) or "").strip()
                    if fee_value:
                        try:
                            fee_total += abs(
                                self._parse_concatenated_amount_and_coin(fee_value)[0])
                        except ValueError:
                            fee_total += abs(force_decimal(fee_value))
                utc_time = self._parse_utc_time(row.get("Closed Time", ""))
                self._append_realized_future_result(
                    book,
                    direction,
                    utc_time,
                    coin,
                    realized_pnl,
                    fee_total,
                    row_num,
                    file_path,
                    f"Bitget unified position history {(row.get('Trading Pair') or row.get('Futures') or '').strip()}".strip(
                    ),
                )

    def _read_small_balance_conversion_history(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                status = (row.get("Status") or "").strip().lower()
                if status and status not in ("successful", "completed"):
                    continue
                conversion_quantity = (row.get("Conversion quantity") or "").strip()
                if not conversion_quantity:
                    continue
                sold_amount, sold_coin = self._parse_amount_and_coin(
                    conversion_quantity)
                received_amount = abs(force_decimal(row.get("BGB received", "0")))
                fee = abs(force_decimal(row.get("Fee (BGB)", "0")))
                utc_time = datetime.datetime(
                    config.TAX_YEAR, 1, 1, tzinfo=datetime.timezone.utc)
                remark = "Bitget small balance conversion"
                self.append_operation(book, "Sell", utc_time, abs(
                    sold_amount), sold_coin, row_num, file_path, remark=remark)
                if received_amount:
                    self.append_operation(
                        book, "Buy", utc_time, received_amount, "BGB", row_num, file_path, remark=remark)
                self._append_fee_if_present(
                    book, fee, utc_time, "BGB", row_num, file_path, "Trade")

    def _read_futures_order_details(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "futures transactions"):
            log.info(
                "Skipping Bitget futures order details %s because futures transactions export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                utc_time = self._parse_utc_time(row.get("Date", ""))
                realized_pnl = force_decimal(row.get("Realized P/L", "0"))
                fee = force_decimal(row.get("Fee", "0"))
                self._append_realized_future_result(
                    book,
                    (row.get("Direction") or "").strip(),
                    utc_time,
                    (row.get("Coin") or "").strip() or "UNKNOWN",
                    realized_pnl,
                    fee,
                    row_num,
                    file_path,
                    f"Bitget futures order detail {(row.get('Futures') or '').strip()}".strip(
                    ),
                )

    def _read_futures_order_history(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "futures transactions") or self._has_sibling_filename(file_path, "futures order details"):
            log.info(
                "Skipping Bitget futures order history %s because a more detailed futures export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                status = (row.get("Status") or "").strip().lower()
                if status and status != "fully executed":
                    continue
                utc_time = self._parse_utc_time(row.get("Date", ""))
                realized_pnl = force_decimal(row.get("Realized P/L", "0"))
                self._append_realized_future_result(
                    book,
                    (row.get("Direction") or "").strip(),
                    utc_time,
                    (row.get("Coin") or "").strip() or "UNKNOWN",
                    realized_pnl,
                    decimal.Decimal(0),
                    row_num,
                    file_path,
                    f"Bitget futures order history {(row.get('Order ID') or '').strip()}".strip(
                    ),
                )

    def _read_futures_position_history(self, file_path: Path, book) -> None:
        if self._has_sibling_filename(file_path, "futures transactions") or self._has_sibling_filename(file_path, "futures order details") or self._has_sibling_filename(file_path, "futures order history"):
            log.info(
                "Skipping Bitget futures position history %s because a more detailed futures export is available.",
                file_path,
            )
            return

        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_num, row in enumerate(reader, start=2):
                realized_raw = (row.get("Realized PnL") or row.get(
                    "Position Pnl") or "").strip()
                if not realized_raw:
                    continue
                realized_pnl, coin = self._parse_concatenated_amount_and_coin(
                    realized_raw)
                if not realized_pnl:
                    continue
                futures_name = (row.get("Futures") or "").strip().lower()
                direction = "close_long" if "long" in futures_name else "close_short"
                fee_total = decimal.Decimal(0)
                for fee_key in ("Fees", "Opening fee", "Closing fee"):
                    fee_value = (row.get(fee_key) or "").strip()
                    if fee_value:
                        fee_total += abs(
                            self._parse_concatenated_amount_and_coin(fee_value)[0])
                self._append_realized_future_result(
                    book,
                    direction,
                    self._parse_utc_time(row.get("Closed time", "")),
                    coin,
                    realized_pnl,
                    fee_total,
                    row_num,
                    file_path,
                    f"Bitget futures position history {(row.get('Futures') or '').strip()}".strip(
                    ),
                )

    def _read_earn_records(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames or []

            if fieldnames == [
                "Product name",
                "Amount",
                "Profit type",
                "Date",
                "Type",
                "Status",
            ] or fieldnames == [
                "Product name",
                "Amount",
                "Profit type",
                "Duration",
                "Date",
                "Type",
            ]:
                self._read_simple_earn_records(file_path, book, reader)
                return

            if fieldnames == [
                "Reference",
                "Start time",
                "Coin",
                "Type",
                "Interest coin",
                "Amount",
                "Handling fee",
                "Status",
            ]:
                self._read_onchain_earn_records(file_path, book, reader)
                return

            if fieldnames == [
                "Time",
                "Coin",
                "APR",
                "Status",
                "Reward coin",
                "Interest",
            ]:
                self._read_onchain_earn_profit(file_path, book, reader)
                return

            if fieldnames == [
                "Time",
                "Product Name",
                "Strike Price",
                "Amount",
                "Type",
            ] or fieldnames == [
                "Time",
                "Product Name",
                "Direction",
                "APR",
                "Duration",
                "Amount",
                "Type",
            ]:
                self._read_structured_earn_records(file_path, book, reader)
                return

            log.warning("Bitget Earn CSV file type not supported yet: %s", file_path)

    def _read_simple_earn_records(self, file_path: Path, book, reader: csv.DictReader) -> None:
        for row_num, row in enumerate(reader, start=2):
            status = (row.get("Status") or "Completed").strip().lower()
            if status and status not in ("completed", "distributed"):
                continue

            event_type = (row.get("Type") or "").strip()
            operation = self._map_simple_earn_type(event_type)
            if operation is None:
                log.warning(
                    "%s row %s: Unknown Bitget Earn type '%s'. Skipping.",
                    file_path,
                    row_num,
                    event_type,
                )
                continue

            amount, coin = self._parse_amount_and_coin(row.get("Amount", ""))
            utc_time = self._parse_utc_time(row.get("Date", ""))
            product = (row.get("Product name") or "").strip()
            remark = f"Bitget earn {event_type} {product}".strip()
            self.append_operation(
                book,
                operation,
                utc_time,
                abs(amount),
                coin,
                row_num,
                file_path,
                remark=remark,
            )

    def _read_onchain_earn_records(self, file_path: Path, book, reader: csv.DictReader) -> None:
        for row_num, row in enumerate(reader, start=2):
            status = (row.get("Status") or "").strip().lower()
            if status and status not in ("staked", "redeemed", "completed"):
                continue

            event_type = (row.get("Type") or "").strip()
            operation = self._map_onchain_earn_type(event_type)
            if operation is None:
                log.warning(
                    "%s row %s: Unknown Bitget On-chain Earn type '%s'. Skipping.",
                    file_path,
                    row_num,
                    event_type,
                )
                continue

            coin = (row.get("Coin") or row.get(
                "Interest coin") or "").strip() or "UNKNOWN"
            amount = abs(force_decimal(row.get("Amount", "0")))
            fee = abs(force_decimal(row.get("Handling fee", "0")))
            utc_time = self._parse_utc_time(row.get("Start time", ""))
            reference = (row.get("Reference") or "").strip()
            remark = f"Bitget on-chain earn {event_type} {reference}".strip()
            self.append_operation(
                book,
                operation,
                utc_time,
                amount,
                coin,
                row_num,
                file_path,
                remark=remark,
            )
            self._append_fee_if_present(
                book,
                fee,
                utc_time,
                coin,
                row_num,
                file_path,
                operation,
            )

    def _read_onchain_earn_profit(self, file_path: Path, book, reader: csv.DictReader) -> None:
        for row_num, row in enumerate(reader, start=2):
            status = (row.get("Status") or "").strip().lower()
            if status and status not in ("distributed", "completed"):
                continue

            coin = (row.get("Reward coin") or row.get(
                "Coin") or "").strip() or "UNKNOWN"
            amount = abs(force_decimal(row.get("Interest", "0")))
            utc_time = self._parse_utc_time(row.get("Time", ""))
            remark = f"Bitget on-chain earn reward {row.get('APR', '').strip()}".strip()
            self.append_operation(
                book,
                "StakingInterest",
                utc_time,
                amount,
                coin,
                row_num,
                file_path,
                remark=remark,
            )

    def _read_structured_earn_records(self, file_path: Path, book, reader: csv.DictReader) -> None:
        for row_num, row in enumerate(reader, start=2):
            event_type = (row.get("Type") or "").strip()
            operation = self._map_structured_earn_type(event_type)
            if operation is None:
                log.warning(
                    "%s row %s: Unknown Bitget structured earn type '%s'. Skipping.",
                    file_path,
                    row_num,
                    event_type,
                )
                continue

            amount_value = (row.get("Amount") or "").strip()
            if not amount_value:
                continue
            amount, coin = self._parse_amount_and_coin(amount_value)
            utc_time = self._parse_utc_time(row.get("Time", ""))
            product = (row.get("Product Name") or "").strip()
            remark = f"Bitget structured earn {event_type} {product}".strip()
            self.append_operation(
                book,
                operation,
                utc_time,
                abs(amount),
                coin,
                row_num,
                file_path,
                remark=remark,
            )
