"""Bitget API client and readers."""

import base64
import csv
import datetime
import decimal
import hashlib
import hmac
import json
import time
from pathlib import Path
from typing import Any, Callable, Optional

import config
import log_config

from .base import ExchangeReader
from .utils import force_decimal

log = log_config.getLogger(__name__)


class BitgetApiReader(ExchangeReader):
    """Reader for Bitget API data."""

    SUPPORTED_RECORD_TYPES = ("spot", "future", "margin", "p2p")

    def __init__(self):
        super().__init__("bitget")

    def read_file(self, file_path: Path, book) -> None:
        """Bitget uses API, not CSV files. This should not be called."""
        raise NotImplementedError("Bitget uses API, not CSV files")

    def _get_resume_state_path(self) -> Path:
        return Path("data/bitget_resume_state.json")

    def _load_resume_state(self) -> dict[str, Any]:
        path = self._get_resume_state_path()
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_resume_state(self, state: dict[str, Any]) -> None:
        path = self._get_resume_state_path()
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
        timestamp = str(int(time.time() * 1000))
        message = timestamp + method.upper() + path + body
        signature = base64.b64encode(
            hmac.new(
                config.BITGET_API_SECRET.encode("utf-8"),
                message.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        return {
            "Content-Type": "application/json",
            "ACCESS-KEY": config.BITGET_API_KEY,
            "ACCESS-SIGN": signature,
            "ACCESS-TIMESTAMP": timestamp,
            "ACCESS-PASSPHRASE": config.BITGET_API_PASSPHRASE,
        }

    def _get(self, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        import requests
        url = f"{config.BITGET_API_BASE_URL}{path}"
        headers = self._headers("GET", path)
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    def _fetch_all(self, path: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        records = []
        while True:
            data = self._get(path, params)
            if "data" in data:
                records.extend(data["data"])
            else:
                break
            if "cursor" in data and data["cursor"]:
                params["cursor"] = data["cursor"]
            else:
                break
        return records

    def _get_cached_chunk(
        self, endpoint_state: dict[str, Any], start_ms: int, end_ms: int
    ) -> Optional[dict[str, Any]]:
        for chunk in endpoint_state.setdefault("chunks", []):
            if (
                chunk.get("start_ms") == start_ms
                and chunk.get("end_ms") == end_ms
            ):
                return chunk
        return None

    def _mark_chunk_processed(
        self,
        resume_state: dict[str, Any],
        endpoint_state: dict[str, Any],
        start_ms: int,
        end_ms: int,
    ) -> None:
        for chunk in endpoint_state.setdefault("chunks", []):
            if chunk.get("start_ms") == start_ms and chunk.get("end_ms") == end_ms:
                chunk["processed"] = True
                break
        endpoint_state["last_end_ms"] = end_ms
        endpoint_state["updated_at"] = int(time.time() * 1000)
        self._save_resume_state(resume_state)

    def _fetch_all_range(
        self,
        path: str,
        start_time_ms: int,
        end_time_ms: int,
        extra_params: Optional[dict[str, Any]] = None,
    ) -> list[tuple[int, int, list[dict[str, Any]], dict[str, Any], dict[str, Any]]]:
        cursor_start = start_time_ms
        chunk_size = 10 * 24 * 60 * 60 * 1000
        endpoint_key = path.strip("/").replace("/", "_")

        resume_state = self._load_resume_state()
        if resume_state.get("tax_year") != config.TAX_YEAR:
            resume_state = {"tax_year": config.TAX_YEAR, "endpoints": {}}

        endpoints = resume_state.setdefault("endpoints", {})
        endpoint_state = endpoints.setdefault(endpoint_key, {})
        endpoint_state.setdefault("chunks", [])
        last_end_ms = endpoint_state.get("last_end_ms", start_time_ms - 1)

        chunks: list[tuple[int, int, list[dict[str, Any]],
                           dict[str, Any], dict[str, Any]]] = []
        while cursor_start <= end_time_ms:
            cursor_end = min(cursor_start + chunk_size - 1, end_time_ms)
            params = {
                "startTime": cursor_start,
                "endTime": cursor_end,
                "limit": 500,
            }
            if extra_params:
                params.update(extra_params)

            cached_chunk = self._get_cached_chunk(
                endpoint_state, cursor_start, cursor_end
            )
            if cached_chunk is not None:
                log.info(
                    "Reusing cached Bitget API records %s - %s",
                    datetime.datetime.fromtimestamp(
                        cursor_start / 1000, tz=datetime.timezone.utc),
                    datetime.datetime.fromtimestamp(
                        cursor_end / 1000, tz=datetime.timezone.utc),
                )
                segment = cached_chunk["records"]
            else:
                log.info(
                    "Fetching Bitget API records %s - %s",
                    datetime.datetime.fromtimestamp(
                        cursor_start / 1000, tz=datetime.timezone.utc),
                    datetime.datetime.fromtimestamp(
                        cursor_end / 1000, tz=datetime.timezone.utc),
                )
                segment = self._fetch_all(path, params)
                endpoint_state["chunks"].append(
                    {
                        "start_ms": cursor_start,
                        "end_ms": cursor_end,
                        "records": segment,
                        "processed": False,
                    }
                )
                self._save_resume_state(resume_state)

            chunks.append(
                (cursor_start, cursor_end, segment, resume_state, endpoint_state)
            )

            if cursor_end == end_time_ms:
                break
            cursor_start = cursor_end + 1

        return chunks

    def _map_spot_tax_type(self, spot_tax_type: str) -> Optional[str]:
        normalized = spot_tax_type.strip()
        mapping = {
            "Deposit": "Deposit",
            "Withdrawal": "Withdrawal",
            "Buy": "Buy",
            "Sell": "Sell",
            "Buy Crypto": "Buy",
            "Sell Crypto": "Sell",
            "Crypto Voucher Distribution": "Airdrop",
            "Airdrop Reward-A": "Airdrop",
            "Airdrop Reward-B": "Airdrop",
            "Interest": "StakingInterest",
            "batch_interest_user_in": "StakingInterest",
            "User fees": "Fee",
            "Transaction fee deduct": "Fee",
            "System charges fees": "Fee",
            "financial_lock_out": "Fee",
            "Trading fee rebate": "Commission",
            "Fiat withdrawal success - Deduct": "Withdrawal",
            "Reward": "Airdrop",
            "Automatic deposit": "Deposit",
            "Automatic withdrawal": "Withdrawal",
            "Transfer in": "Deposit",
            "Transfer out": "Withdrawal",
            "fiat_recharge_in": "Deposit",
            "fiat_balance_success_user_in": "Deposit",
            "fiat_balance_user_out": "Withdrawal",
            "financial_rede_in": "Deposit",
            "financial_unlock_in": "Deposit",
            "financial_pos_out": "Withdrawal",
            "financial_subs_out": "Withdrawal",
            "Copy Trade expense": "Fee",
            "Refund Copy Trade commission": "Commission",
            "Consumption": "Sell",
            "Gains": "Airdrop",
        }

        if normalized in mapping:
            return mapping[normalized]

        lower_name = normalized.lower()
        if lower_name.endswith("_in"):
            return "Deposit"
        if lower_name.endswith("_out"):
            return "Withdrawal"
        if "commission" in lower_name:
            return "Commission"
        if "expense" in lower_name or "fee" in lower_name:
            return "Fee"

        return mapping.get(normalized)

    def _map_future_tax_type(self, future_tax_type: str) -> Optional[str]:
        future_tax_type = future_tax_type.upper()
        mapping = {
            "TRANSFER_IN": "Deposit",
            "TRANSFER_OUT": "Withdrawal",
            "ORDER_DEALT_IN": "Deposit",
            "ORDER_DEALT_FROZEN_OUT": "Fee",
            "ORDER_PLF_FEE_OUT": "Fee",
            "EXCHANGE_SOURCE_TOKEN_USER_OUT": "Sell",
            "EXCHANGE_TARGET_TOKEN_USER_IN": "Buy",
            "OPEN_LONG": "Buy",
            "OPEN_SHORT": "Buy",
            "CLOSE_LONG": "Sell",
            "CLOSE_SHORT": "Sell",
            "BUY_DEAL": "Buy",
            "SELL_DEAL": "Sell",
            "FORCE_CLOSE_LONG": "Sell",
            "FORCE_CLOSE_SHORT": "Sell",
            "BURST_CLOSE_LONG": "Sell",
            "BURST_CLOSE_SHORT": "Sell",
            "INTEREST_SETTLEMENT_OUT": "Fee",
            "CONTRACT_MAIN_SETTLE_FEE_USER_IN": "Commission",
            "CONTRACT_MAIN_SETTLE_FEE_USER_OUT": "Fee",
            "BONUS_ISSUE": "Airdrop",
            "RISK_CAPTITAL_USER_TRANSFER": "Deposit",
        }

        if result := mapping.get(future_tax_type):
            return result

        if future_tax_type.startswith("TRANS_FROM_"):
            return "Deposit"
        if future_tax_type.startswith("TRANS_TO_"):
            return "Withdrawal"
        if future_tax_type.startswith("TRANSFER_FROM_"):
            return "Deposit"
        if future_tax_type.startswith("TRANSFER_TO_"):
            return "Withdrawal"
        if future_tax_type.endswith("_FEE") or "SETTLE_FEE" in future_tax_type:
            return "Fee"
        if future_tax_type.endswith("_LONG"):
            return "Buy" if "OPEN" in future_tax_type else "Sell"
        if future_tax_type.endswith("_SHORT"):
            return "Buy" if "OPEN" in future_tax_type else "Sell"
        return None

    def _map_margin_tax_type(self, margin_tax_type: str) -> Optional[str]:
        mapping = {
            "transfer_in": "Deposit",
            "transfer_out": "Withdrawal",
            "borrow": "Deposit",
            "repay": "Withdrawal",
            "liquidation_fee": "Fee",
            "deal_in": "Buy",
            "deal_out": "Sell",
            "interest_repay": "Fee",
            "exchange_in": "Buy",
            "exchange_out": "Sell",
            "confiscated": "Fee",
            "compensate": "Fee",
        }
        return mapping.get(margin_tax_type)

    def _map_p2p_tax_type(self, p2p_tax_type: str) -> Optional[str]:
        mapping = {
            "BUY": "Buy",
            "SELL": "Sell",
            "TRANSFER_IN": "Deposit",
            "TRANSFER_OUT": "Withdrawal",
            "FEE": "Fee",
            "SERVICE_FEE": "Fee",
            "REFUND": "Buy",
            "COMMISSION": "Commission",
        }
        return mapping.get(p2p_tax_type)

    def import_tax_year_records(
        self,
        book,
        tax_year: int,
        record_types: Optional[list[str]] = None,
    ) -> None:
        """Import selected Bitget API record groups for one tax year.

        Args:
            book: Book instance receiving parsed operations.
            tax_year: Tax year to import.
            record_types: Optional list of groups to import.
                Supported values are: spot, future, margin, p2p.
                If omitted, all supported groups are imported.
        """
        year_start = datetime.datetime(tax_year, 1, 1, tzinfo=datetime.timezone.utc)
        year_end = datetime.datetime(
            tax_year, 12, 31, 23, 59, 59, 999000, tzinfo=datetime.timezone.utc
        )
        self.import_api_records(
            book,
            int(year_start.timestamp() * 1000),
            int(year_end.timestamp() * 1000),
            record_types=record_types,
        )

    def import_api_records(
        self,
        book,
        start_time_ms: int,
        end_time_ms: int,
        record_types: Optional[list[str]] = None,
    ) -> None:
        """Import selected Bitget API record groups for a timestamp range."""
        importers: dict[str, Callable[[Any, int, int], None]] = {
            "spot": self.import_spot_records,
            "future": self.import_future_records,
            "margin": self.import_margin_records,
            "p2p": self.import_p2p_records,
        }

        selected = record_types or list(self.SUPPORTED_RECORD_TYPES)
        normalized = [record_type.strip().lower() for record_type in selected]

        unknown_types = [
            record_type
            for record_type in normalized
            if record_type not in importers
        ]
        if unknown_types:
            log.warning(
                "Unknown Bitget API record types requested: %s. Supported: %s",
                ", ".join(sorted(set(unknown_types))),
                ", ".join(self.SUPPORTED_RECORD_TYPES),
            )

        for record_type in normalized:
            importer = importers.get(record_type)
            if importer is None:
                continue
            importer(book, start_time_ms, end_time_ms)

    def import_spot_records(self, book, start_time_ms: int, end_time_ms: int) -> None:
        chunks = self._fetch_all_range(
            "/api/v2/tax/spot-record",
            start_time_ms,
            end_time_ms,
        )

        if not chunks:
            log.info("No Bitget spot account records were returned.")
            return

        total_records = sum(len(segment) for _, _, segment, _, _ in chunks)
        log.info("Importing %s Bitget spot records.", total_records)
        empty_tax_type_rows: list[int] = []
        for chunk_start, chunk_end, records, resume_state, endpoint_state in chunks:
            for row_num, row in enumerate(records, start=1):
                tax_type = row.get("taxType", "")
                operation = self._map_spot_tax_type(tax_type)
                if operation is None:
                    if not tax_type.strip():
                        empty_tax_type_rows.append(row_num)
                        continue
                    log.warning(
                        f"Unknown Bitget spot tax type '{tax_type}' in row {row_num}. "
                        "Skipping."
                    )
                    continue

                coin = row.get("coin", "UNKNOWN")
                change = abs(force_decimal(row.get("amount", "0")))
                fee = abs(force_decimal(row.get("fee", "0")))
                utc_time = datetime.datetime.fromtimestamp(
                    int(row.get("ts", 0)) / 1000.0, datetime.timezone.utc
                )
                remark = f"Bitget spot record {row.get('bizOrderId', '')}"

                self.append_operation(
                    book, operation, utc_time, change, coin, row_num, Path(
                        "bitget-api"), remark
                )
                if fee and operation not in ("Fee", "Commission"):
                    self.append_operation(
                        book, "Fee", utc_time, fee, coin, row_num, Path("bitget-api")
                    )

        if empty_tax_type_rows:
            preview = ", ".join(map(str, empty_tax_type_rows[:10]))
            suffix = "..." if len(empty_tax_type_rows) > 10 else ""
            log.info(
                "Skipped %s Bitget spot rows with empty tax type (rows: %s%s).",
                len(empty_tax_type_rows),
                preview,
                suffix,
            )

    def import_future_records(self, book, start_time_ms: int, end_time_ms: int) -> None:
        chunks = self._fetch_all_range(
            "/api/v2/tax/future-record",
            start_time_ms,
            end_time_ms,
        )

        if not chunks:
            log.info("No Bitget future account records were returned.")
            return

        total_records = sum(len(segment) for _, _, segment, _, _ in chunks)
        log.info("Importing %s Bitget future records.", total_records)
        for chunk_start, chunk_end, records, resume_state, endpoint_state in chunks:
            for row_num, row in enumerate(records, start=1):
                tax_type = row.get("taxType", "")
                operation = self._map_future_tax_type(tax_type)
                if operation is None:
                    log.warning(
                        f"Unknown Bitget future tax type '{tax_type}' in row {row_num}. "
                        "Skipping."
                    )
                    continue

                coin = row.get("coin", "UNKNOWN")
                change = abs(force_decimal(row.get("amount", "0")))
                fee = abs(force_decimal(row.get("fee", "0")))
                utc_time = datetime.datetime.fromtimestamp(
                    int(row.get("ts", 0)) / 1000.0, datetime.timezone.utc
                )
                remark = f"Bitget future record {row.get('bizOrderId', '')}"

                self.append_operation(
                    book, operation, utc_time, change, coin, row_num, Path(
                        "bitget-api"), remark
                )
                if fee and operation not in ("Fee", "Commission"):
                    self.append_operation(
                        book, "Fee", utc_time, fee, coin, row_num, Path("bitget-api")
                    )

    def import_margin_records(self, book, start_time_ms: int, end_time_ms: int) -> None:
        chunks = self._fetch_all_range(
            "/api/v2/tax/margin-record",
            start_time_ms,
            end_time_ms,
        )

        if not chunks:
            log.info("No Bitget margin account records were returned.")
            return

        total_records = sum(len(segment) for _, _, segment, _, _ in chunks)
        log.info("Importing %s Bitget margin records.", total_records)
        for chunk_start, chunk_end, records, resume_state, endpoint_state in chunks:
            for row_num, row in enumerate(records, start=1):
                tax_type = row.get("taxType", "")
                operation = self._map_margin_tax_type(tax_type)
                if operation is None:
                    log.warning(
                        f"Unknown Bitget margin tax type '{tax_type}' in row {row_num}. "
                        "Skipping."
                    )
                    continue

                coin = row.get("coin", "UNKNOWN")
                change = abs(force_decimal(row.get("amount", "0")))
                fee = abs(force_decimal(row.get("fee", "0")))
                utc_time = datetime.datetime.fromtimestamp(
                    int(row.get("ts", 0)) / 1000.0, datetime.timezone.utc
                )
                remark = f"Bitget margin record {row.get('bizOrderId', '')}"

                self.append_operation(
                    book, operation, utc_time, change, coin, row_num, Path(
                        "bitget-api"), remark
                )
                if fee and operation not in ("Fee", "Commission"):
                    self.append_operation(
                        book, "Fee", utc_time, fee, coin, row_num, Path("bitget-api")
                    )

    def import_p2p_records(self, book, start_time_ms: int, end_time_ms: int) -> None:
        chunks = self._fetch_all_range(
            "/api/v2/tax/p2p-record",
            start_time_ms,
            end_time_ms,
        )

        if not chunks:
            log.info("No Bitget P2P account records were returned.")
            return

        total_records = sum(len(segment) for _, _, segment, _, _ in chunks)
        log.info("Importing %s Bitget P2P records.", total_records)
        for chunk_start, chunk_end, records, resume_state, endpoint_state in chunks:
            for row_num, row in enumerate(records, start=1):
                tax_type = row.get("p2pTaxType", row.get("taxType", ""))
                operation = self._map_p2p_tax_type(tax_type)
                if operation is None:
                    log.warning(
                        f"Unknown Bitget P2P tax type '{tax_type}' in row {row_num}. "
                        "Skipping."
                    )
                    continue

                coin = (
                    row.get("coin")
                    or row.get("amountCoin")
                    or row.get("orderCoin")
                    or "UNKNOWN"
                )
                change = abs(
                    force_decimal(
                        row.get("amount", row.get("quantity", "0"))
                    )
                )
                fee = abs(force_decimal(row.get("fee", "0")))
                utc_time = datetime.datetime.fromtimestamp(
                    int(row.get("ts", row.get("createTime", 0))) / 1000.0,
                    datetime.timezone.utc,
                )
                remark = (
                    f"Bitget P2P record {row.get('bizOrderId', '')}"
                    if row.get("bizOrderId")
                    else f"Bitget P2P record {row.get('orderId', '')}"
                )
                self.append_operation(
                    book,
                    operation,
                    utc_time,
                    change,
                    coin,
                    row_num,
                    Path("bitget-api"),
                    remark=remark,
                )
                if fee and operation not in ("Fee", "Commission"):
                    self.append_operation(
                        book,
                        "Fee",
                        utc_time,
                        fee,
                        coin,
                        row_num,
                        Path("bitget-api"),
                    )
