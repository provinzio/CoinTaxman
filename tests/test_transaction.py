import transaction as tr
import misc
import config
import datetime
import decimal
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class WithdrawalTests(unittest.TestCase):
    def test_partial_withdrawn_coins_preserves_total_with_rounding(self) -> None:
        utc_time = datetime.datetime(
            2025, 8, 25, 18, 5, 57, 361000, tzinfo=datetime.timezone.utc
        )
        withdrawal = tr.Withdrawal(
            utc_time=utc_time,
            platform="bitget",
            change=decimal.Decimal("230.47867853"),
            coin="USDT",
            line=[1],
            file_path=Path("account_statements/bitget 2025/debug.csv"),
        )
        first_deposit = tr.Deposit(
            utc_time=datetime.datetime(
                2025, 6, 23, 18, 38, 48, 188000, tzinfo=datetime.timezone.utc
            ),
            platform="bitget",
            change=decimal.Decimal("0.000078538896"),
            coin="USDT",
            line=[2],
            file_path=withdrawal.file_path,
        )
        second_deposit = tr.Deposit(
            utc_time=datetime.datetime(
                2025, 8, 25, 18, 5, 36, 304000, tzinfo=datetime.timezone.utc
            ),
            platform="bitget",
            change=decimal.Decimal("230.478599991104"),
            coin="USDT",
            line=[3],
            file_path=withdrawal.file_path,
        )
        withdrawal.withdrawn_coins = [
            tr.SoldCoin(first_deposit, decimal.Decimal("0.000078538896")),
            tr.SoldCoin(second_deposit, decimal.Decimal("230.478599991104")),
        ]

        percent = decimal.Decimal("3.859794358348237103141029145E-11")

        partials = withdrawal.partial_withdrawn_coins(percent)

        self.assertEqual(
            misc.dsum(partial.sold for partial in partials),
            percent * withdrawal.change,
        )
        self.assertEqual(partials[-1].op, second_deposit)

    def test_partial_withdrawn_coins_tolerates_tiny_rounding_residue(self) -> None:
        utc_time = datetime.datetime(
            2025, 8, 25, 18, 5, 57, 361000, tzinfo=datetime.timezone.utc
        )
        withdrawal = tr.Withdrawal(
            utc_time=utc_time,
            platform="bitget",
            change=decimal.Decimal("1.000000000001"),
            coin="BTC",
            line=[1],
            file_path=Path("account_statements/bitget 2025/debug.csv"),
        )
        d1 = tr.Deposit(
            utc_time=utc_time,
            platform="bitget",
            change=decimal.Decimal("0.333333333334"),
            coin="BTC",
            line=[2],
            file_path=withdrawal.file_path,
        )
        d2 = tr.Deposit(
            utc_time=utc_time,
            platform="bitget",
            change=decimal.Decimal("0.333333333333"),
            coin="BTC",
            line=[3],
            file_path=withdrawal.file_path,
        )
        d3 = tr.Deposit(
            utc_time=utc_time,
            platform="bitget",
            change=decimal.Decimal("0.333333333334"),
            coin="BTC",
            line=[4],
            file_path=withdrawal.file_path,
        )
        withdrawal.withdrawn_coins = [
            tr.SoldCoin(d1, d1.change),
            tr.SoldCoin(d2, d2.change),
            tr.SoldCoin(d3, d3.change),
        ]

        percent = decimal.Decimal("0.3333333333333333333333333333")
        with patch.object(config, "BALANCE_DUST_TOLERANCE", decimal.Decimal("0.000001")):
            partials = withdrawal.partial_withdrawn_coins(percent)

        self.assertEqual(
            misc.dsum(partial.sold for partial in partials),
            percent * withdrawal.change,
        )


if __name__ == "__main__":
    unittest.main()
