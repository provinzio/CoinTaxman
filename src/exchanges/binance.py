"""Binance exchange reader."""

import datetime
import decimal
from pathlib import Path
from typing import Optional

import log_config
from .base import ExchangeReader
from .utils import force_decimal, parse_utc_time

log = log_config.getLogger(__name__)


class BinanceReader(ExchangeReader):
    """Reader for Binance CSV files."""

    def __init__(self, version: int = 1):
        super().__init__("binance")
        self.version = version
        self.operation_mapping = {
            "Distribution": "Airdrop",
            "Cash Voucher distribution": "Airdrop",
            "Cashback Voucher": "Airdrop",
            "Rewards Distribution": "Airdrop",
            "Simple Earn Flexible Airdrop": "Airdrop",
            "Airdrop Assets": "Airdrop",
            "Crypto Box": "Airdrop",
            "Launchpool Airdrop": "Airdrop",
            "Megadrop Rewards": "Airdrop",
            "HODLer Airdrops Distribution": "Airdrop",
            "Token Swap - Distribution": "Airdrop",
            "Launchpool Airdrop - System Distribution": "Airdrop",
            #
            "Savings Interest": "CoinLendInterest",
            "Savings purchase": "CoinLend",
            "Savings Principal redemption": "CoinLendEnd",
            "Savings distribution": "CoinLendInterest",
            "Simple Earn Flexible Subscription": "CoinLend",
            "Simple Earn Flexible Redemption": "CoinLendEnd",
            "Simple Earn Flexible Interest": "CoinLendInterest",
            "Simple Earn Locked Subscription": "CoinLend",
            "Simple Earn Locked Redemption": "CoinLendEnd",
            "Simple Earn Locked Rewards": "CoinLendInterest",
            "Savings Distribution": "CoinLendInterest",
            #
            "BNB Vault Rewards": "CoinLendInterest",
            "Launchpool Earnings Withdrawal": "CoinLendInterest",
            #
            "Commission History": "Commission",
            "Commission Fee Shared With You": "Commission",
            "Referrer rebates": "Commission",
            "Referral Kickback": "Commission",
            "Commission Rebate": "Commission",
            # DeFi yield farming
            "Liquid Swap add": "CoinLend",
            "Liquid Swap remove": "CoinLendEnd",
            "Liquid Swap rewards": "CoinLendInterest",
            "Launchpool Interest": "CoinLendInterest",
            #
            "Super BNB Mining": "StakingInterest",
            "POS savings interest": "StakingInterest",
            "POS savings purchase": "Staking",
            "POS savings redemption": "StakingEnd",
            "ETH 2.0 Staking Rewards": "StakingInterest",
            "Staking Purchase": "Staking",
            "Staking Rewards": "StakingInterest",
            "Staking Redemption": "StakingEnd",
            #
            "Fiat Deposit": "Deposit",
            "Fiat Withdraw": "Withdrawal",
            "Withdraw": "Withdrawal",
            #
            "Transaction Buy": "Buy",
            "Transaction Spend": "Sell",
            "Transaction Revenue": "Buy",
            "Transaction Sold": "Sell",
            "Transaction Fee": "Fee",
            "Asset Recovery": "Sell",
        }

    def read_file(self, file_path: Path, book) -> None:
        """Read Binance CSV file."""
        import csv

        with open(file_path, encoding="utf8") as f:
            reader = csv.reader(f)

            # Skip header.
            next(reader)

            for rowlist in reader:
                if self.version == 1:
                    _utc_time, account, operation, coin, _change, remark = rowlist
                elif self.version in (2, 3):
                    (
                        _,
                        _utc_time,
                        account,
                        operation,
                        coin,
                        _change,
                        remark,
                    ) = rowlist
                else:
                    log.error("File version not Supported " + str(file_path))
                    raise NotImplementedError

                row = reader.line_num

                # Parse data.
                if self.version in (1, 2):
                    utc_time = parse_utc_time(_utc_time, "%Y-%m-%d %H:%M:%S")
                elif self.version == 3:
                    utc_time = parse_utc_time(_utc_time, "%y-%m-%d %H:%M:%S")
                else:
                    log.error("File version not Supported " + str(file_path))
                    raise NotImplementedError

                change = force_decimal(_change)
                operation = self.operation_mapping.get(operation, operation)
                if operation in (
                    "The Easiest Way to Trade",
                    "Small assets exchange BNB",
                    "Small Assets Exchange BNB",
                    "Transaction Related",
                    "Large OTC trading",
                    "Sell",
                    "Buy",
                    "Binance Convert",
                ):
                    operation = "Sell" if change < 0 else "Buy"

                if operation == "Liquid Swap add/sell":
                    operation = "CoinLendEnd" if change < 0 else "CoinLend"

                if operation == "Commission" and account != "Spot":
                    # All comissions will be handled the same way.
                    # As of now, only Spot Binance Operations are supported,
                    # so we have to change the account type to Spot.
                    account = "Spot"

                if (
                    account in ("Spot", "P2P")
                    and operation
                    in (
                        "transfer_in",
                        "transfer_out",
                    )
                    or (
                        account in ("Spot", "Funding")
                        and operation == "Transfer Between Main and Funding Wallet"
                    )
                ):
                    # Ignore transfers
                    continue

                change = abs(change)

                # Validate data.
                supported_account_types = ("Spot", "Savings", "Earn", "Funding")
                assert account in supported_account_types, (
                    f"Other types than {supported_account_types} are currently "
                    f"not supported.  Given account type is `{account}`. "
                    "Please create an Issue or PR."
                )
                assert operation
                assert coin
                assert change

                if remark:
                    # Ignore default remarks
                    if remark in (
                        "Withdraw fee is included",
                        "Binance Earn",
                        "Binance Pay",
                        "Binance Launchpool",
                    ) or remark.endswith(" to BNB"):
                        remark = ""

                    # Do not warn for specific remarks
                    elif remark.startswith("Korrekturbuchung."):
                        pass

                    # Warn on other binance remarks, becuase all remarks should be some
                    # unnecessary default text which we'd like to ignore
                    else:
                        log.warning(
                            "I may have missed a remark in %s:%i: `%s`.",
                            file_path,
                            row,
                            remark,
                        )

                self.append_operation(
                    book, operation, utc_time, change, coin, row, file_path, remark
                )
