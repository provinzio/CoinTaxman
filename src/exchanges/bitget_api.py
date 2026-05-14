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
from urllib.parse import urlencode

import config
import log_config

from .base import ExchangeReader
from .utils import force_decimal

log = log_config.getLogger(__name__)


class BitgetApiReader(ExchangeReader):
    """Reader for Bitget API data."""

    SUPPORTED_RECORD_TYPES = ("spot", "future", "margin", "p2p", "copy")
    DEFAULT_RECORD_TYPES = ("spot", "future", "margin", "p2p")
    KNOWN_QUOTE_COINS = (
        "USDT",
        "USDC",
        "BUSD",
        "FDUSD",
        "EUR",
        "USD",
        "BTC",
        "ETH",
        "BNB",
        "TRY",
        "BRL",
        "GBP",
        "AUD",
        "JPY",
    )
    FUTURE_COPY_PRODUCT_TYPES = ("USDT-FUTURES", "COIN-FUTURES", "USDC-FUTURES")

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
        query_string = urlencode(params or {}, doseq=True)
        signed_path = f"{path}?{query_string}" if query_string else path
        url = f"{config.BITGET_API_BASE_URL}{signed_path}"
        max_attempts = 6
        backoff_seconds = 1.0
        for attempt in range(1, max_attempts + 1):
            headers = self._headers("GET", signed_path)
            try:
                response = requests.get(url, headers=headers)
            except requests.RequestException:
                if attempt == max_attempts:
                    raise
                log.warning(
                    "Bitget API request error on attempt %s/%s for %s. Retrying in %.1fs.",
                    attempt,
                    max_attempts,
                    signed_path,
                    backoff_seconds,
                )
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 10.0)
                continue

            if response.status_code in (429, 500, 502, 503, 504):
                if attempt == max_attempts:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")
                sleep_for = backoff_seconds
                if retry_after:
                    try:
                        sleep_for = max(float(retry_after), sleep_for)
                    except ValueError:
                        pass
                payload = response.text.strip()
                log.warning(
                    "Bitget API temporary failure %s on %s (attempt %s/%s). "
                    "Retrying in %.1fs. Response=%s",
                    response.status_code,
                    signed_path,
                    attempt,
                    max_attempts,
                    sleep_for,
                    payload,
                )
                time.sleep(sleep_for)
                backoff_seconds = min(max(backoff_seconds * 2, sleep_for), 10.0)
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError:
                payload = response.text.strip()
                if payload:
                    log.error(
                        "Bitget API request failed: %s %s params=%s status=%s response=%s",
                        "GET",
                        path,
                        params,
                        response.status_code,
                        payload,
                    )
                raise
            return response.json()

        raise RuntimeError("Bitget API request retry loop exited unexpectedly")

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

    def _fetch_copy_trade_history(
        self,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[dict[str, Any]]:
        path = "/api/v2/copy/spot-follower/query-history-orders"
        id_less_than: Optional[str] = None
        records: list[dict[str, Any]] = []

        while True:
            params: dict[str, Any] = {
                "startTime": start_time_ms,
                "endTime": end_time_ms,
                "limit": 50,
            }
            if id_less_than:
                params["idLessThan"] = id_less_than

            response = self._get(path, params)
            payload = response.get("data", {})
            if not isinstance(payload, dict):
                break

            page_records = payload.get("trackingList", [])
            if not isinstance(page_records, list) or not page_records:
                break

            records.extend(page_records)
            end_id = payload.get("endId")
            if not end_id:
                break

            next_id = str(end_id)
            if next_id == id_less_than:
                break
            id_less_than = next_id

        return records

    def _fetch_future_copy_trade_history(
        self,
        start_time_ms: int,
        end_time_ms: int,
        product_type: str,
    ) -> list[dict[str, Any]]:
        path = "/api/v2/copy/mix-follower/query-history-orders"
        id_less_than: Optional[str] = None
        records: list[dict[str, Any]] = []

        while True:
            params: dict[str, Any] = {
                "productType": product_type,
                "startTime": start_time_ms,
                "endTime": end_time_ms,
                "limit": 100,
            }
            if id_less_than:
                params["idLessThan"] = id_less_than

            response = self._get(path, params)
            payload = response.get("data", {})
            if not isinstance(payload, dict):
                break

            page_records = payload.get("trackingList", [])
            if not isinstance(page_records, list) or not page_records:
                break

            records.extend(page_records)
            end_id = payload.get("endId")
            if not end_id:
                break

            next_id = str(end_id)
            if next_id == id_less_than:
                break
            id_less_than = next_id

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
                # Bitget tax endpoints reject oversized page sizes (HTTP 400).
                "limit": 100,
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

    def _fetch_copy_trade_history_range(
        self,
        start_time_ms: int,
        end_time_ms: int,
    ) -> list[tuple[int, int, list[dict[str, Any]], dict[str, Any], dict[str, Any]]]:
        cursor_start = start_time_ms
        chunk_size = 10 * 24 * 60 * 60 * 1000
        endpoint_key = "api_v2_copy_spot-follower_query-history-orders"

        resume_state = self._load_resume_state()
        if resume_state.get("tax_year") != config.TAX_YEAR:
            resume_state = {"tax_year": config.TAX_YEAR, "endpoints": {}}

        endpoints = resume_state.setdefault("endpoints", {})
        endpoint_state = endpoints.setdefault(endpoint_key, {})
        endpoint_state.setdefault("chunks", [])

        chunks: list[tuple[int, int, list[dict[str, Any]],
                           dict[str, Any], dict[str, Any]]] = []
        while cursor_start <= end_time_ms:
            cursor_end = min(cursor_start + chunk_size - 1, end_time_ms)

            cached_chunk = self._get_cached_chunk(
                endpoint_state, cursor_start, cursor_end
            )
            if cached_chunk is not None:
                log.info(
                    "Reusing cached Bitget copy-trade records %s - %s",
                    datetime.datetime.fromtimestamp(
                        cursor_start / 1000, tz=datetime.timezone.utc),
                    datetime.datetime.fromtimestamp(
                        cursor_end / 1000, tz=datetime.timezone.utc),
                )
                segment = cached_chunk["records"]
            else:
                log.info(
                    "Fetching Bitget copy-trade records %s - %s",
                    datetime.datetime.fromtimestamp(
                        cursor_start / 1000, tz=datetime.timezone.utc),
                    datetime.datetime.fromtimestamp(
                        cursor_end / 1000, tz=datetime.timezone.utc),
                )
                segment = self._fetch_copy_trade_history(cursor_start, cursor_end)
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

    def _fetch_future_copy_trade_history_range(
        self,
        start_time_ms: int,
        end_time_ms: int,
        product_type: str,
    ) -> list[tuple[int, int, list[dict[str, Any]], dict[str, Any], dict[str, Any]]]:
        cursor_start = start_time_ms
        chunk_size = 10 * 24 * 60 * 60 * 1000
        endpoint_key = (
            "api_v2_copy_mix-follower_query-history-orders_"
            f"{product_type.lower()}"
        )

        resume_state = self._load_resume_state()
        if resume_state.get("tax_year") != config.TAX_YEAR:
            resume_state = {"tax_year": config.TAX_YEAR, "endpoints": {}}

        endpoints = resume_state.setdefault("endpoints", {})
        endpoint_state = endpoints.setdefault(endpoint_key, {})
        endpoint_state.setdefault("chunks", [])

        chunks: list[tuple[int, int, list[dict[str, Any]],
                           dict[str, Any], dict[str, Any]]] = []
        while cursor_start <= end_time_ms:
            cursor_end = min(cursor_start + chunk_size - 1, end_time_ms)

            cached_chunk = self._get_cached_chunk(
                endpoint_state, cursor_start, cursor_end
            )
            if cached_chunk is not None:
                log.info(
                    "Reusing cached Bitget future copy-trade records %s - %s (%s)",
                    datetime.datetime.fromtimestamp(
                        cursor_start / 1000, tz=datetime.timezone.utc),
                    datetime.datetime.fromtimestamp(
                        cursor_end / 1000, tz=datetime.timezone.utc),
                    product_type,
                )
                segment = cached_chunk["records"]
            else:
                log.info(
                    "Fetching Bitget future copy-trade records %s - %s (%s)",
                    datetime.datetime.fromtimestamp(
                        cursor_start / 1000, tz=datetime.timezone.utc),
                    datetime.datetime.fromtimestamp(
                        cursor_end / 1000, tz=datetime.timezone.utc),
                    product_type,
                )
                segment = self._fetch_future_copy_trade_history(
                    cursor_start,
                    cursor_end,
                    product_type,
                )
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

    def _split_symbol_pair(self, symbol: str) -> tuple[Optional[str], Optional[str]]:
        normalized = symbol.strip().upper().replace("-", "").replace("_", "")
        if not normalized:
            return None, None

        for quote in self.KNOWN_QUOTE_COINS:
            if normalized.endswith(quote) and len(normalized) > len(quote):
                return normalized[:-len(quote)], quote
        return None, None

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
            "Copy Trade expense": "Fee",
            "Refund Copy Trade commission": "Commission",
        }

        if normalized in mapping:
            return mapping[normalized]

        lower_name = normalized.lower()
        if lower_name.startswith("financial_"):
            # Internal earn/subscription account movements often come as
            # paired bookkeeping entries and are not safe to map 1:1 here.
            return None
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
            "ORDER_DEALT_IN": "FuturesPnlSigned",
            "ORDER_DEALT_FROZEN_OUT": "Fee",
            "ORDER_PLF_FEE_OUT": "Fee",
            "EXCHANGE_SOURCE_TOKEN_USER_OUT": "FuturesPnlSigned",
            "EXCHANGE_TARGET_TOKEN_USER_IN": "FuturesPnlSigned",
            "OPEN_LONG": "FuturesPnlSigned",
            "OPEN_SHORT": "FuturesPnlSigned",
            "CLOSE_LONG": "FuturesPnlSigned",
            "CLOSE_SHORT": "FuturesPnlSigned",
            "BUY_DEAL": "FuturesPnlSigned",
            "SELL_DEAL": "FuturesPnlSigned",
            "FORCE_CLOSE_LONG": "FuturesPnlSigned",
            "FORCE_CLOSE_SHORT": "FuturesPnlSigned",
            "BURST_CLOSE_LONG": "FuturesPnlSigned",
            "BURST_CLOSE_SHORT": "FuturesPnlSigned",
            "INTEREST_SETTLEMENT_OUT": "Fee",
            "CONTRACT_MAIN_SETTLE_FEE_USER_IN": "FuturesPnlSigned",
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
            return "FuturesPnlSigned"
        if future_tax_type.endswith("_SHORT"):
            return "FuturesPnlSigned"
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
                Supported values are: spot, future, margin, p2p, copy.
                If omitted, the default groups are imported.
        """
        start_year = min(tax_year, config.BITGET_API_START_YEAR)
        if start_year < tax_year:
            log.info(
                "Bitget API import includes opening inventory lookback: %s-%s.",
                start_year,
                tax_year,
            )

        year_start = datetime.datetime(start_year, 1, 1, tzinfo=datetime.timezone.utc)
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
            "copy": self.import_copy_trade_records,
        }

        selected = record_types or list(self.DEFAULT_RECORD_TYPES)
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

    def import_copy_trade_records(
        self, book, start_time_ms: int, end_time_ms: int
    ) -> None:
        self.import_spot_copy_trade_records(book, start_time_ms, end_time_ms)
        self.import_future_copy_trade_records(book, start_time_ms, end_time_ms)

    def import_spot_copy_trade_records(
        self, book, start_time_ms: int, end_time_ms: int
    ) -> None:
        chunks = self._fetch_copy_trade_history_range(start_time_ms, end_time_ms)

        if not chunks:
            log.info("No Bitget spot copy-trade records were returned.")
            return

        total_records = sum(len(segment) for _, _, segment, _, _ in chunks)
        log.info("Importing %s Bitget spot copy-trade records.", total_records)
        for chunk_start, chunk_end, records, resume_state, endpoint_state in chunks:
            for row_num, row in enumerate(records, start=1):
                symbol = str(row.get("symbol", ""))
                base_coin, quote_coin = self._split_symbol_pair(symbol)
                if base_coin is None:
                    log.warning(
                        "Unknown Bitget copy-trade symbol '%s' in row %s. "
                        "Skipping.",
                        symbol,
                        row_num,
                    )
                    continue

                fill_size = abs(force_decimal(row.get("fillSize", "0")))
                if fill_size == 0:
                    continue

                buy_time_raw = row.get("buyTime")
                sell_time_raw = row.get("sellTime")
                if not buy_time_raw or not sell_time_raw:
                    log.warning(
                        "Incomplete Bitget copy-trade timestamps in row %s. "
                        "Skipping.",
                        row_num,
                    )
                    continue

                buy_time = datetime.datetime.fromtimestamp(
                    int(buy_time_raw) / 1000.0,
                    datetime.timezone.utc,
                )
                sell_time = datetime.datetime.fromtimestamp(
                    int(sell_time_raw) / 1000.0,
                    datetime.timezone.utc,
                )

                tracking_no = row.get("trackingNo", "")
                trader_id = row.get("traderId", "")
                remark = (
                    f"Bitget spot copy-trade {tracking_no} "
                    f"(trader: {trader_id}, symbol: {symbol})"
                )

                self.append_operation(
                    book,
                    "Buy",
                    buy_time,
                    fill_size,
                    base_coin,
                    row_num,
                    Path("bitget-api"),
                    remark=remark,
                )

                buy_fee = abs(force_decimal(row.get("buyFee", "0")))
                if buy_fee and quote_coin:
                    self.append_operation(
                        book,
                        "Fee",
                        buy_time,
                        buy_fee,
                        quote_coin,
                        row_num,
                        Path("bitget-api"),
                        remark=remark,
                    )

                self.append_operation(
                    book,
                    "Sell",
                    sell_time,
                    fill_size,
                    base_coin,
                    row_num,
                    Path("bitget-api"),
                    remark=remark,
                )

                sell_fee = abs(force_decimal(row.get("sellFee", "0")))
                if sell_fee and quote_coin:
                    self.append_operation(
                        book,
                        "Fee",
                        sell_time,
                        sell_fee,
                        quote_coin,
                        row_num,
                        Path("bitget-api"),
                        remark=remark,
                    )

            self._mark_chunk_processed(
                resume_state,
                endpoint_state,
                chunk_start,
                chunk_end,
            )

    def import_future_copy_trade_records(
        self, book, start_time_ms: int, end_time_ms: int
    ) -> None:
        total_records = 0
        for product_type in self.FUTURE_COPY_PRODUCT_TYPES:
            chunks = self._fetch_future_copy_trade_history_range(
                start_time_ms,
                end_time_ms,
                product_type,
            )
            total_records += sum(len(segment) for _, _, segment, _, _ in chunks)
            for chunk_start, chunk_end, records, resume_state, endpoint_state in chunks:
                for row_num, row in enumerate(records, start=1):
                    symbol = str(row.get("symbol", ""))
                    base_coin, quote_coin = self._split_symbol_pair(symbol)
                    if quote_coin is None:
                        log.warning(
                            "Unknown Bitget future copy-trade symbol '%s' in row %s. "
                            "Skipping.",
                            symbol,
                            row_num,
                        )
                        continue

                    close_time_raw = row.get("closeTime") or row.get("openTime")
                    if not close_time_raw:
                        log.warning(
                            "Missing Bitget future copy-trade time in row %s. "
                            "Skipping.",
                            row_num,
                        )
                        continue

                    utc_time = datetime.datetime.fromtimestamp(
                        int(close_time_raw) / 1000.0,
                        datetime.timezone.utc,
                    )

                    tracking_no = row.get("trackingNo", "")
                    trader_id = row.get("traderId", "")
                    remark = (
                        f"Bitget future copy-trade {tracking_no} "
                        f"(trader: {trader_id}, symbol: {symbol}, "
                        f"productType: {product_type})"
                    )

                    signed_pnl = force_decimal(
                        row.get("netProfit", row.get("achievedPL", "0")))
                    if signed_pnl > 0:
                        operation = "FuturesProfit"
                    elif signed_pnl < 0:
                        operation = "FuturesLoss"
                    else:
                        operation = None

                    if operation is not None:
                        self.append_operation(
                            book,
                            operation,
                            utc_time,
                            abs(signed_pnl),
                            quote_coin,
                            row_num,
                            Path("bitget-api"),
                            remark=remark,
                        )

                    open_fee = abs(force_decimal(row.get("openFee", "0")))
                    if open_fee:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            open_fee,
                            quote_coin,
                            row_num,
                            Path("bitget-api"),
                            remark=remark,
                        )

                    close_fee = abs(force_decimal(row.get("closeFee", "0")))
                    if close_fee:
                        self.append_operation(
                            book,
                            "Fee",
                            utc_time,
                            close_fee,
                            quote_coin,
                            row_num,
                            Path("bitget-api"),
                            remark=remark,
                        )

                self._mark_chunk_processed(
                    resume_state,
                    endpoint_state,
                    chunk_start,
                    chunk_end,
                )

        if total_records == 0:
            log.info("No Bitget future copy-trade records were returned.")
            return
        log.info("Importing %s Bitget future copy-trade records.", total_records)

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
                tax_type = row.get("spotTaxType", row.get("taxType", ""))
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
                signed_change = force_decimal(row.get("amount", "0"))
                if operation == "FuturesPnlSigned":
                    if signed_change > 0:
                        operation = "FuturesProfit"
                    elif signed_change < 0:
                        operation = "FuturesLoss"
                    else:
                        continue
                change = abs(signed_change)
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
