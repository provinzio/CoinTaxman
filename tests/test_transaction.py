import transaction as tr
import misc
import datetime
import decimal
import os
import sys
import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
