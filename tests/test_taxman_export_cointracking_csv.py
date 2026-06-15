from taxman import Taxman
import transaction as tr
import config
import csv
import datetime
import decimal
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

# Taxman imports xlsxwriter at module import time. Provide a minimal stub so
# logic tests can run without optional export dependencies installed.
if "xlsxwriter" not in sys.modules:
    class _WorkbookStub:
        def __init__(self, *args, **kwargs):
            pass

    class _FormatStub:
        pass

    xlsxwriter_stub = types.SimpleNamespace(
        Workbook=_WorkbookStub,
        format=types.SimpleNamespace(Format=_FormatStub),
    )
    sys.modules["xlsxwriter"] = xlsxwriter_stub

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class _BookStub:
    operations: list[tr.Operation] = []


class _PriceDataStub:
    pass


class TaxmanCoinTrackingCsvExportTests(unittest.TestCase):
    def _build_sell_entry(self) -> tr.SellReportEntry:
        sell_utc = datetime.datetime(
            config.TAX_YEAR,
            5,
            3,
            9,
            0,
            0,
            tzinfo=datetime.timezone.utc,
        )
        buy_utc = datetime.datetime(
            config.TAX_YEAR,
            1,
            15,
            10,
            0,
            0,
            tzinfo=datetime.timezone.utc,
        )
        return tr.SellReportEntry(
            sell_platform="bitget",
            buy_platform="kraken",
            amount=decimal.Decimal("1.23456789"),
            coin="BTC",
            sell_utc_time=sell_utc,
            buy_utc_time=buy_utc,
            first_fee_amount=decimal.Decimal("0.01"),
            first_fee_coin="EUR",
            first_fee_in_fiat=decimal.Decimal("0.10"),
            second_fee_amount=decimal.Decimal("0.02"),
            second_fee_coin="EUR",
            second_fee_in_fiat=decimal.Decimal("0.20"),
            sell_value_in_fiat=decimal.Decimal("150.00"),
            buy_cost_in_fiat=decimal.Decimal("100.00"),
            is_taxable=True,
            taxation_type="Einkünfte aus privaten Veräußerungsgeschäften",
            remark="",
        )

    def test_export_evaluation_as_steuertipps_csv_writes_expected_format(self) -> None:
        taxman = Taxman(_BookStub(), _PriceDataStub())
        taxman.tax_report_entries = [self._build_sell_entry()]

        with tempfile.TemporaryDirectory() as tmp_dir:
            excel_path = Path(tmp_dir) / f"{config.TAX_YEAR}_rev001.xlsx"
            csv_path = taxman.export_evaluation_as_steuertipps_csv(excel_path)

            self.assertEqual(csv_path.name, f"{config.TAX_YEAR}_rev001_steuertipps.csv")
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                rows = list(csv.reader(handle))

        self.assertEqual(
            rows[0],
            [
                "Identifier:Capital_Gains",
                f"Method:{config.PRINCIPLE.name}",
                f"Tax_Year:{config.TAX_YEAR}",
                f"Base_Currency:{config.FIAT}",
            ],
        )
        self.assertEqual(
            rows[1],
            [
                "Amount",
                "Currency",
                "Date Sold",
                "Date Acquired",
                "Short/Long",
                "Buy/Input at",
                "Sell/Output at",
                "Proceeds",
                "Cost Basis",
                "Gain/Loss",
            ],
        )
        self.assertEqual(
            rows[2],
            [
                "1.23456789",
                "BTC",
                "03.05." + str(config.TAX_YEAR),
                "15.01." + str(config.TAX_YEAR),
                "Short",
                "kraken",
                "bitget",
                "150.00",
                "100.30",
                "49.70",
            ],
        )

    def test_export_evaluation_as_wiso_csv_still_uses_legacy_suffix(self) -> None:
        taxman = Taxman(_BookStub(), _PriceDataStub())
        taxman.tax_report_entries = [self._build_sell_entry()]

        with tempfile.TemporaryDirectory() as tmp_dir:
            excel_path = Path(tmp_dir) / f"{config.TAX_YEAR}_rev001.xlsx"
            csv_path = taxman.export_evaluation_as_wiso_csv(excel_path)

            self.assertEqual(csv_path.name, f"{config.TAX_YEAR}_rev001_wiso.csv")
            with csv_path.open("r", encoding="utf-8", newline="") as handle:
                first_row = next(csv.reader(handle))

        self.assertEqual(first_row[0], "Identifier:Capital_Gains")


if __name__ == "__main__":
    unittest.main()
