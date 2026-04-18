"""Registry and detection helpers for exchange readers."""

import csv
from pathlib import Path
from typing import Optional

from exchanges.base import ExchangeReader
from exchanges.binance import BinanceReader
from exchanges.bitget_csv import BitgetCsvReader
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
        "bitget_deposit_withdraw": BitgetCsvReader,
        "bitget_spot_transactions": BitgetCsvReader,
        "bitget_futures_transactions": BitgetCsvReader,
        "bitget_margin_transactions": BitgetCsvReader,
        "bitget_onchain_transactions": BitgetCsvReader,
        "bitget_onchain_history": BitgetCsvReader,
        "bitget_unified_transactions": BitgetCsvReader,
        "bitget_unified_convert_history": BitgetCsvReader,
        "bitget_earn_simple": BitgetCsvReader,
        "bitget_earn_onchain": BitgetCsvReader,
        "bitget_earn_onchain_profit": BitgetCsvReader,
        "bitget_earn_structured": BitgetCsvReader,
        "bitget_small_balance_conversion": BitgetCsvReader,
        "bitget_spot_order_details": BitgetCsvReader,
        "bitget_spot_order_history": BitgetCsvReader,
        "bitget_margin_order_history": BitgetCsvReader,
        "bitget_unified_order_history": BitgetCsvReader,
        "bitget_unified_position_history": BitgetCsvReader,
        "bitget_futures_order_details": BitgetCsvReader,
        "bitget_futures_order_history": BitgetCsvReader,
        "bitget_futures_position_history": BitgetCsvReader,
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
        "bitget_deposit_withdraw": 1,
        "bitget_spot_transactions": 1,
        "bitget_futures_transactions": 1,
        "bitget_margin_transactions": 1,
        "bitget_onchain_transactions": 1,
        "bitget_onchain_history": 1,
        "bitget_unified_transactions": 1,
        "bitget_unified_convert_history": 1,
        "bitget_earn_simple": 1,
        "bitget_earn_onchain": 1,
        "bitget_earn_onchain_profit": 1,
        "bitget_earn_structured": 1,
        "bitget_small_balance_conversion": 1,
        "bitget_spot_order_details": 1,
        "bitget_spot_order_history": 1,
        "bitget_margin_order_history": 1,
        "bitget_unified_order_history": 1,
        "bitget_unified_position_history": 1,
        "bitget_futures_order_details": 1,
        "bitget_futures_order_history": 1,
        "bitget_futures_position_history": 1,
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
        "bitget_deposit_withdraw": [
            "Date",
            "Type",
            "Funding account",
            "Coin",
            "Quantity",
            "Address",
            "TxID",
            "Status",
        ],
        "bitget_spot_transactions": [
            "order",
            "Date",
            "Coin",
            "Type",
            "Amount",
            "Fee",
            "Available",
        ],
        "bitget_futures_transactions": [
            "Order",
            "Date",
            "Coin",
            "Futures",
            "Margin Mode",
            "Type",
            "Amount",
            "Fee",
            "Wallet balance",
        ],
        "bitget_margin_transactions": [
            "Time",
            "Pair",
            "Coin",
            "Type",
            "Amount",
            "fee",
            "Balance",
        ],
        "bitget_onchain_transactions": [
            "Coin",
            "Type",
            "Time",
            "Quantity",
            "Balance",
        ],
        "bitget_onchain_history": [
            "Coin",
            "Chain",
            "Direction",
            "Order type",
            "Order price($)",
            "Order time",
            "Filled time",
            "Amount paid",
            "Amount received",
            "Onchain order size",
            "Realized Pnl",
            "Filled price",
            "Status",
            "Transaction fee",
            "Network fee",
        ],
        "bitget_unified_transactions": [
            "Order ID",
            "Date",
            "Trade Type",
            "Coin",
            "Trading Pair",
            "Transaction Type",
            "Amount",
            "Fee",
            "Balance Changes",
            "Balance",
        ],
        "bitget_unified_convert_history": [
            "Order ID",
            "Date",
            "Swap Type",
            "Coin to Sell",
            "Sell Quantity",
            "Coin to Buy",
            "Buy Quantity",
            "Price",
        ],
        "bitget_earn_simple": [
            "Product name",
            "Amount",
            "Profit type",
            "Date",
            "Type",
            "Status",
        ],
        "bitget_earn_onchain": [
            "Reference",
            "Start time",
            "Coin",
            "Type",
            "Interest coin",
            "Amount",
            "Handling fee",
            "Status",
        ],
        "bitget_earn_onchain_profit": [
            "Time",
            "Coin",
            "APR",
            "Status",
            "Reward coin",
            "Interest",
        ],
        "bitget_earn_structured": [
            "Time",
            "Product Name",
            "Strike Price",
            "Amount",
            "Type",
        ],
        "bitget_small_balance_conversion": [
            "Duration",
            "Conversion quantity",
            "Conversion price",
            "Fee (BGB)",
            "BGB received",
            "Status",
        ],
        "bitget_spot_order_details": [
            "Date",
            "Trading pair",
            "Base Asset",
            "Quote Asset",
            "Direction",
            "Price",
            "Amount",
            "Total",
            "Fee",
            "Fee Coin",
        ],
        "bitget_spot_order_history": [
            "Date",
            "Type",
            "Order Id",
            "Trading pair",
            "Base Asset",
            "Quote Asset",
            "Direction",
            "Price",
            "Order amount",
            "Executed",
            "Average Price",
            "Trading volume",
            "Status",
        ],
        "bitget_margin_order_history": [
            "Date",
            "Type",
            "Business",
            "Order ID",
            "Trading Pair",
            "Base Asset",
            "Quote Asset",
            "Direction",
            "Price",
            "Order amount",
            "Average Price",
            "Executed",
            "Trading volume",
            "Status",
        ],
        "bitget_unified_order_history": [
            "Date",
            "Order ID",
            "Trade Type",
            "Order Type",
            "Delegate Type",
            "Trading Pair",
            "Base Asset",
            "Quote Asset",
            "Direction",
            "Order Price",
            "Order Quantity",
            "Executed",
            "Average Price",
            "Trading Volume",
            "Fee",
            "Status",
        ],
        "bitget_unified_position_history": [
            "Trading Pair",
            "Trade Type",
            "Direction",
            "Margin Mode",
            "Opening Time",
            "Closed Time",
            "Avg.Entry Price",
            "Avg.Closing Price",
            "Closed Amount",
            "Closed Value",
            "Position PnL",
            "Funding Fee",
            "Opening Fee",
            "Closing Fee",
        ],
        "bitget_futures_order_details": [
            "Date",
            "Direction",
            "Coin",
            "Futures",
            "Transaction amount",
            "Average Price",
            "Trading volume",
            "Realized P/L",
            "NetProfits",
            "Fee",
        ],
        "bitget_futures_order_history": [
            "Date",
            "Order ID",
            "Direction",
            "Coin",
            "Futures",
            "order source",
            "Transaction type",
            "Price",
            "Average Price",
            "Order amount",
            "Executed",
            "Trading volume",
            "Realized P/L",
            "NetProfits",
            "Status",
        ],
        "bitget_futures_position_history": [
            "Futures",
            "Opening time",
            "Average entry price",
            "Average closing price",
            "Closed amount",
            "Closed value",
            "Position Pnl",
            "Realized PnL",
            "Fees",
            "Opening fee",
            "Closing fee",
            "Closed time",
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

    bitget_files = {
        "withdrawal records": "bitget_deposit_withdraw",
        "spot transactions": "bitget_spot_transactions",
        "futures transactions": "bitget_futures_transactions",
        "margin transactions": "bitget_margin_transactions",
        "onchain transactions": "bitget_onchain_transactions",
        "onchain history": "bitget_onchain_history",
        "transactions of unified trading account": "bitget_unified_transactions",
        "convert history of unified trading account": "bitget_unified_convert_history",
        "small balance conversion history": "bitget_small_balance_conversion",
        "spot order details": "bitget_spot_order_details",
        "spot order history": "bitget_spot_order_history",
        "margin order history": "bitget_margin_order_history",
        "order history of unified trading account": "bitget_unified_order_history",
        "position history of unified trading account": "bitget_unified_position_history",
        "futures order details": "bitget_futures_order_details",
        "futures order history": "bitget_futures_order_history",
        "futures position history": "bitget_futures_position_history",
    }
    filename_lower = filename.lower()
    for pattern, exchange_type in bitget_files.items():
        if pattern in filename_lower:
            with open(file_path, encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                expected = expected_headers[exchange_type]
                header = next(reader, None)
                if header == expected:
                    return create_exchange_reader(exchange_type)

    bitget_earn_headers = {
        "bitget_earn_simple": [
            [
                "Product name",
                "Amount",
                "Profit type",
                "Date",
                "Type",
                "Status",
            ],
            [
                "Product name",
                "Amount",
                "Profit type",
                "Duration",
                "Date",
                "Type",
            ],
        ],
        "bitget_earn_onchain": [
            [
                "Reference",
                "Start time",
                "Coin",
                "Type",
                "Interest coin",
                "Amount",
                "Handling fee",
                "Status",
            ]
        ],
        "bitget_earn_onchain_profit": [
            ["Time", "Coin", "APR", "Status", "Reward coin", "Interest"]
        ],
        "bitget_earn_structured": [
            ["Time", "Product Name", "Strike Price", "Amount", "Type"],
            [
                "Time",
                "Product Name",
                "Direction",
                "APR",
                "Duration",
                "Amount",
                "Type",
            ],
        ],
    }
    if "earn" in filename_lower:
        with open(file_path, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for exchange_type, header_options in bitget_earn_headers.items():
                if header in header_options:
                    return create_exchange_reader(exchange_type)

    with open(file_path, encoding="utf8") as f:
        reader = csv.reader(f)
        # Check all potential headers at their expected header row.
        for exchange, expected in expected_headers.items():
            # Skip Pionex entries as they're handled above.
            if exchange.startswith("pionex_") or exchange.startswith("bitget_"):
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
