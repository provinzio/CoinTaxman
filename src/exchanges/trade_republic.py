"""Trade Republic exchange reader."""

import csv
import datetime
from pathlib import Path

import log_config

from .base import ExchangeReader

log = log_config.getLogger(__name__)


class TradeRepublicReader(ExchangeReader):
    """Reader for Trade Republic CSV exports."""

    def __init__(self):
        super().__init__("traderepublic")

    def _parse_german_decimal(self, value: str):
        normalized = value.strip().replace(".", "").replace(",", ".")
        return self.force_decimal(normalized)

    def _parse_trade_date(self, value: str) -> tuple[datetime.datetime, str]:
        parts = value.strip().split()
        if len(parts) < 2:
            raise ValueError("missing action in transaktion column")

        action = parts[-1].upper()
        trade_date = parts[0]
        utc_time = self.parse_utc_time(trade_date, "%d.%m.%Y")
        return utc_time, action

    def read_file(self, file_path: Path, book) -> None:
        with open(file_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=";")

            header = next(reader, None)
            if header is None:
                return

            for columns in reader:
                row = reader.line_num
                if len(columns) != 8:
                    log.warning(
                        f"{file_path} row {row}: Expected 8 columns, got {len(columns)}. "
                        "Skipping row."
                    )
                    continue

                (
                    asset,
                    transaktion,
                    nominale_raw,
                    preis_pro_stueck_raw,
                    gebuehren_raw,
                    _gebucht_raw,
                    _gewinn_raw,
                    _gewinn_lt_1_jahr_raw,
                ) = columns

                try:
                    utc_time, action = self._parse_trade_date(transaktion)
                    asset_amount = self._parse_german_decimal(nominale_raw)
                    unit_price_eur = self._parse_german_decimal(preis_pro_stueck_raw)
                    fee_eur = self._parse_german_decimal(gebuehren_raw)
                    quote_eur = asset_amount * unit_price_eur
                except ValueError as exc:
                    log.warning(
                        f"{file_path} row {row}: Could not parse row ({exc}). Skipping row."
                    )
                    continue

                if action == "KAUF":
                    self.append_operation(
                        book,
                        "Buy",
                        utc_time,
                        asset_amount,
                        asset,
                        row,
                        file_path,
                    )
                    self.append_operation(
                        book,
                        "Sell",
                        utc_time,
                        quote_eur,
                        "EUR",
                        row,
                        file_path,
                    )
                elif action == "VERKAUF":
                    self.append_operation(
                        book,
                        "Sell",
                        utc_time,
                        asset_amount,
                        asset,
                        row,
                        file_path,
                    )
                    self.append_operation(
                        book,
                        "Buy",
                        utc_time,
                        quote_eur,
                        "EUR",
                        row,
                        file_path,
                    )
                else:
                    log.warning(
                        f"{file_path} row {row}: Unknown action '{action}'. Skipping row."
                    )
                    continue

                if fee_eur > 0:
                    self.append_operation(
                        book,
                        "Fee",
                        utc_time,
                        fee_eur,
                        "EUR",
                        row,
                        file_path,
                    )
