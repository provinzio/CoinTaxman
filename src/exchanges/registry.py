"""Registry and detection helpers for exchange readers."""

import csv
from pathlib import Path
from typing import Optional

from exchanges.base import ExchangeReader
from exchanges.binance import BinanceReader
from exchanges.bitpanda import BitpandaReader
from exchanges.bitunix import BitunixReader
from exchanges.coinbase import CoinbaseReader
from exchanges.coinbase_pro import CoinbaseProReader
from exchanges.custom_eur import CustomEurReader
from exchanges.kraken import KrakenReader
from exchanges.pionex import PionexReader


def create_exchange_reader(exchange_name: str) -> Optional[ExchangeReader]:
    """Create an exchange reader instance based on the exchange name."""
    reader_map = {
        "binance": lambda: BinanceReader(version=1),
        "binance_v2": lambda: BinanceReader(version=2),
        "binance_v3": lambda: BinanceReader(version=3),
        "coinbase": lambda: CoinbaseReader(version=1),
        "coinbase_v2": lambda: CoinbaseReader(version=2),
        "coinbase_v3": lambda: CoinbaseReader(version=3),
        "coinbase_v4": lambda: CoinbaseReader(version=4),
        "coinbase_pro": CoinbaseProReader,
        "kraken_ledgers_old": KrakenReader,
        "kraken_ledgers": KrakenReader,
        "kraken_trades": KrakenReader,
        "bitpanda_pro_trades": BitpandaReader,
        "bitpanda": BitpandaReader,
        "bitunix": BitunixReader,
        "pionex_deposit_withdraw": PionexReader,
        "pionex_trading": PionexReader,
        "pionex_staking": PionexReader,
        "pionex_others": PionexReader,
        "custom_eur": CustomEurReader,
    }

    reader_factory = reader_map.get(exchange_name)
    if reader_factory:
        return reader_factory()
    return None


def detect_exchange_reader(file_path: Path) -> Optional[ExchangeReader]:
    """Detect exchange reader from a CSV file path and header."""
    if file_path.suffix != ".csv":
        return None

    expected_header_row = {
        "binance": 1,
        "binance_v2": 1,
        "binance_v3": 1,
        "coinbase": 1,
        "coinbase_v2": 1,
        "coinbase_v3": 1,
        "coinbase_v4": 4,
        "coinbase_pro": 1,
        "kraken_ledgers_old": 1,
        "kraken_ledgers": 1,
        "kraken_trades": 1,
        "bitpanda_pro_trades": 4,
        "bitpanda": 7,
        "bitunix": 1,
        "pionex_deposit_withdraw": 1,
        "pionex_trading": 1,
        "pionex_staking": 1,
        "pionex_others": 1,
        "custom_eur": 1,
    }

    expected_headers = {
        "binance": [
            "UTC_Time",
            "Account",
            "Operation",
            "Coin",
            "Change",
            "Remark",
        ],
        "binance_v2": [
            "User_ID",
            "UTC_Time",
            "Account",
            "Operation",
            "Coin",
            "Change",
            "Remark",
        ],
        "binance_v3": [
            "\ufeffUser ID",
            "Time",
            "Account",
            "Operation",
            "Coin",
            "Change",
            "Remark",
        ],
        "coinbase": [
            "You can use this transaction report to inform your "
            "likely tax obligations. For US customers, Sells, "
            "Converts, and Rewards Income, and Coinbase Earn "
            "transactions are taxable events. For final tax "
            "obligations, please consult your tax advisor."
        ],
        "coinbase_v2": [
            "You can use this transaction report to inform your "
            "likely tax obligations. For US customers, Sells, "
            "Converts, Rewards Income, Coinbase Earn "
            "transactions, and Donations are taxable events. "
            "For final tax obligations, please consult your tax advisor."
        ],
        "coinbase_v3": [
            "You can use this transaction report to inform your "
            "likely tax obligations. For US customers, Sells, "
            "Converts, Rewards Income, Learning Rewards, "
            "and Donations are taxable events. "
            "For final tax obligations, please consult your tax advisor."
        ],
        "coinbase_v4": [
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
        ],
        "coinbase_pro": [
            "portfolio",
            "trade id",
            "product",
            "side",
            "created at",
            "size",
            "size unit",
            "price",
            "fee",
            "total",
            "price/fee/total unit",
        ],
        "kraken_ledgers_old": [
            "txid",
            "refid",
            "time",
            "type",
            "aclass",
            "asset",
            "amount",
            "fee",
            "balance",
        ],
        "kraken_ledgers": [
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
        ],
        "kraken_trades": [
            "txid",
            "ordertxid",
            "pair",
            "time",
            "type",
            "ordertype",
            "price",
            "cost",
            "fee",
            "vol",
            "margin",
            "misc",
            "ledgers",
        ],
        "bitpanda_pro_trades": [
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
        "bitpanda": [
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
        ],
        "bitunix": [
            "Date (UTC)",
            "Label",
            "Outgoing Asset",
            "Outgoing Amount",
            "Incoming Asset",
            "Incoming Amount",
            "Fee Asset",
            "Fee Amount",
            "Trx. ID",
            "Comment",
        ],
        "pionex_deposit_withdraw": [
            "date(UTC+0)",
            "tx_type",
            "amount",
            "coin",
            "network",
            "txid",
            "fee",
        ],
        "pionex_trading": [
            "date(UTC+0)",
            "executed_qty",
            "amount",
            "price",
            "side",
            "symbol",
            "fee",
            "fee_coin",
            "market_type",
            "tax_id",
        ],
        "pionex_staking": [
            "date(UTC+0)",
            "Received Quantity",
            "Received Currency",
            "Sent Quantity",
            "Sent Currency",
            "tag",
        ],
        "pionex_others": [
            "date(UTC+0)",
            "coin",
            "amount",
            "tag",
            "comment",
        ],
        "custom_eur": [
            "Type",
            "Buy Quantity",
            "Buy Asset",
            "Buy Value in EUR",
            "Sell Quantity",
            "Sell Asset",
            "Sell Value in EUR",
            "Fee Quantity",
            "Fee Asset",
            "Fee Value in EUR",
            "Wallet",
            "Timestamp UTC",
            "Note",
        ],
    }

    # Special handling for Pionex which has multiple file types
    filename = file_path.name
    pionex_files = {
        "deposit-withdraw.csv": "pionex_deposit_withdraw",
        "trading.csv": "pionex_trading",
        "staking.csv": "pionex_staking",
        "others.csv": "pionex_others",
    }
    if filename in pionex_files:
        exchange_type = pionex_files[filename]
        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)
            expected = expected_headers[exchange_type]
            header = next(reader, None)
            if header == expected:
                return create_exchange_reader(exchange_type)

    with open(file_path, encoding="utf8") as f:
        reader = csv.reader(f)
        # Check all potential headers at their expected header row.
        for exchange, expected in expected_headers.items():
            # Skip Pionex entries as they're handled above.
            if exchange.startswith("pionex_"):
                continue
            header_row_num = expected_header_row[exchange]
            # Iterate since header row may appear earlier.
            for _ in range(header_row_num):
                header = next(reader, None)
                if header == expected:
                    return create_exchange_reader(exchange)
            # Rewind the file after each header check.
            f.seek(0)

    return None
