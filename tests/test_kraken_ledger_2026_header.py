"""Unittest of the real CoinTaxman Book methods with synthetic Kraken CSV rows.

Loads *method source* from src/book.py via Python AST so the parser and
detection logic are genuinely executed, while unrelated configured tax
DB, locale and price APIs are not initialized. No real users, RPC, wallet,
trades, money or tax advice.
"""

from __future__ import annotations

import ast
import csv
import datetime
import decimal
import pathlib
import tempfile
import types
import unittest
from collections import defaultdict
from typing import Optional

ROOT = pathlib.Path(__file__).resolve().parents[1]
DOC = ast.parse((ROOT / "src/book.py").read_text(encoding="utf8"))
NODE = next(x for x in DOC.body if isinstance(x, ast.ClassDef) and x.name == "Book")
NAMES = {"detect_exchange", "_read_kraken_ledgers", "_read_kraken_ledgers_2026"}
METHODS = [x for x in NODE.body if isinstance(x, ast.FunctionDef) and x.name in NAMES]
assert len(METHODS) == len(NAMES), "Real Book parser source or test selection changed"
TEST_CLASS = ast.ClassDef(
    name="BookParserUnderTest", bases=[], keywords=[], body=METHODS, decorator_list=[]
)
CODE = compile(
    ast.fix_missing_locations(ast.Module(body=[TEST_CLASS], type_ignores=[])),
    str(ROOT / "src/book.py"),
    "exec",
)


class FakeLogger:
    def error(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass


ENV = {
    "Path": pathlib.Path,
    "Optional": Optional,
    "csv": csv,
    "datetime": datetime,
    "decimal": decimal,
    "log": FakeLogger(),
    "misc": types.SimpleNamespace(force_decimal=decimal.Decimal),
    "kraken_asset_map": {},
}
exec(CODE, ENV)
BookParserUnderTest = ENV["BookParserUnderTest"]

HEADER_2026 = [
    "txid",
    "refid",
    "time",
    "type",
    "subtype",
    "aclass",
    "subclass",
    "asset",
    "wallet",
    "amount",
    "fee",
    "balance",
]
HEADER_2020 = [
    "txid",
    "refid",
    "time",
    "type",
    "subtype",
    "aclass",
    "asset",
    "amount",
    "fee",
    "balance",
]
HEADER_OLD = [
    "txid",
    "refid",
    "time",
    "type",
    "aclass",
    "asset",
    "amount",
    "fee",
    "balance",
]


def ledger_row(
    extra12=True,
    wallet="spot",
    kind="deposit",
    dt="2026-10-12 10:11:00",
    subclass="crypto",
    txn_id="FAKE-KRAKEN-LEDGER-1",
):
    values = [
        txn_id,
        "FAKE-REF-100",
        dt,
        kind,
        "",
        "currency",
        subclass,
        "BTC",
        wallet,
        "1.250000",
        "0.00",
        "12.0",
    ]
    if not extra12:
        del values[8]  # wallet
        del values[6]  # subclass
    return values


class Event:
    def __init__(self, name, time, change, coin, line):
        self.type_name = name
        self.utc_time = time
        self.change = change
        self.coin = coin
        self.line = [line]


def book():
    b = BookParserUnderTest()
    b.kraken_held_ops = defaultdict(lambda: defaultdict(lambda: None))
    b.operations = []
    b._create_operation = lambda kind, time, platform, amount, coin, row, path: Event(
        kind, time, amount, coin, row
    )
    b._append_operation = lambda op: b.operations.append(op)
    b.append_operation = (
        lambda kind, time, platform, amount, coin, row, path: b.operations.append(
            Event(kind, time, amount, coin, row)
        )
    )
    return b


def csvfile(rows):
    directory = tempfile.TemporaryDirectory()
    path = pathlib.Path(directory.name) / "kraken-ledgers.csv"
    with path.open("w", encoding="utf8", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(rows)
    return directory, path


class RealKrakenParserSourceTests(unittest.TestCase):
    def test_2026_current_header_detected_as_distinct_profile(self):
        tmp, path = csvfile([HEADER_2026, ledger_row()])
        try:
            self.assertEqual(book().detect_exchange(path), "kraken_ledgers_2026")
        finally:
            tmp.cleanup()

    def test_original_old_nine_header_regression(self):
        tmp, path = csvfile([HEADER_OLD])
        try:
            self.assertEqual(book().detect_exchange(path), "kraken_ledgers_old")
        finally:
            tmp.cleanup()

    def test_original_2020_ten_header_regression(self):
        tmp, path = csvfile([HEADER_2020])
        try:
            self.assertEqual(book().detect_exchange(path), "kraken_ledgers")
        finally:
            tmp.cleanup()

    def test_2026_two_spot_deposit_rows_create_one_operation(self):
        rows = [HEADER_2026, ledger_row(), ledger_row(txn_id="FAKE-KRAKEN-LEDGER-2")]
        tmp, path = csvfile(rows)
        try:
            b = book()
            b._read_kraken_ledgers_2026(path)
            self.assertEqual(len(b.operations), 1)
            self.assertEqual(b.operations[0].type_name, "Deposit")
            self.assertEqual(b.operations[0].coin, "BTC")
            self.assertEqual(b.operations[0].change, decimal.Decimal("1.250000"))
            self.assertEqual(b.operations[0].utc_time.tzinfo, datetime.timezone.utc)
        finally:
            tmp.cleanup()

    def test_fractional_second_timestamp_is_utc(self):
        tmp, path = csvfile(
            [
                HEADER_2026,
                ledger_row(dt="2026-10-12 10:11:00.123456"),
                ledger_row(dt="2026-10-12 10:11:00.123456", txn_id="LEDGER-2"),
            ]
        )
        try:
            b = book()
            b._read_kraken_ledgers_2026(path)
            self.assertEqual(b.operations[0].utc_time.microsecond, 123456)
            self.assertEqual(b.operations[0].utc_time.tzinfo, datetime.timezone.utc)
        finally:
            tmp.cleanup()

    def test_coin_tax_earn_requires_review_not_arbitrary_income(self):
        for value in ("earn", "hybridearnwithdrawal"):
            with self.subTest(value=value):
                tmp, path = csvfile([HEADER_2026, ledger_row(kind=value)])
                try:
                    b = book()
                    with self.assertRaisesRegex(
                        NotImplementedError, "not yet classified"
                    ):
                        b._read_kraken_ledgers_2026(path)
                    self.assertEqual(b.operations, [])
                finally:
                    tmp.cleanup()

    def test_earn_wallet_deposit_not_silently_classified_as_spot(self):
        tmp, path = csvfile([HEADER_2026, ledger_row(wallet="earn")])
        try:
            with self.assertRaisesRegex(NotImplementedError, "wallet type"):
                book()._read_kraken_ledgers_2026(path)
        finally:
            tmp.cleanup()

    def test_unknown_subclass_requires_manual_review(self):
        tmp, path = csvfile([HEADER_2026, ledger_row(subclass="unreviewed_product")])
        try:
            with self.assertRaisesRegex(
                NotImplementedError, "Unsupported Kraken subclass"
            ):
                book()._read_kraken_ledgers_2026(path)
        finally:
            tmp.cleanup()

    def test_malformed_11_column_csv_not_labeled_as_valid_2026(self):
        tmp, path = csvfile([HEADER_2026[:-1], ledger_row()[:-1]])
        try:
            b = book()
            self.assertIsNone(b.detect_exchange(path))
            with self.assertRaises(RuntimeError):
                b._read_kraken_ledgers_2026(path)
            self.assertEqual(b.operations, [])
        finally:
            tmp.cleanup()

    def test_reordered_unknown_header_is_not_recognized(self):
        invalid = HEADER_2026[:]
        invalid[7], invalid[8] = invalid[8], invalid[7]
        tmp, path = csvfile([invalid])
        try:
            self.assertIsNone(book().detect_exchange(path))
        finally:
            tmp.cleanup()

    def test_unknown_asset_codes_not_fake_normalized(self):
        tmp, path = csvfile([HEADER_2026, ledger_row(), ledger_row(txn_id="UNIQUE-2")])
        try:
            b = book()
            b._read_kraken_ledgers_2026(path)
            self.assertEqual(b.operations[0].coin, "BTC")
            self.assertIsNone(ENV["kraken_asset_map"].get("BTC"))
        finally:
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
