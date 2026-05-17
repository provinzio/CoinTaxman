import datetime
import decimal
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import balance_queue
import config
import transaction as tr

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class BalanceQueueDustTests(unittest.TestCase):
    def _utc(self) -> datetime.datetime:
        return datetime.datetime(
            config.TAX_YEAR,
            1,
            1,
            12,
            0,
            0,
            tzinfo=datetime.timezone.utc,
        )

    def _op(self, op_type, change: str, coin: str = "BTC"):
        return op_type(
            utc_time=self._utc(),
            platform="bitget",
            change=decimal.Decimal(change),
            coin=coin,
            line=[1],
            file_path=Path("account_statements/test.csv"),
        )

    def test_remove_ignores_tiny_rounding_residue(self) -> None:
        queue = balance_queue.BalanceFIFOQueue("BTC")
        queue.add(self._op(tr.Deposit, "1"))

        with patch.object(config, "BALANCE_DUST_TOLERANCE", decimal.Decimal("0.000001")):
            sold = queue.remove(self._op(tr.Sell, "1.00000099"))

        self.assertEqual(len(sold), 1)
        self.assertEqual(sold[0].sold, decimal.Decimal("1"))

    def test_remove_raises_for_residue_above_tolerance(self) -> None:
        queue = balance_queue.BalanceFIFOQueue("BTC")
        queue.add(self._op(tr.Deposit, "1"))

        with patch.object(config, "BALANCE_DUST_TOLERANCE", decimal.Decimal("0.000001")):
            with self.assertRaises(RuntimeError):
                queue.remove(self._op(tr.Sell, "1.00001"))


if __name__ == "__main__":
    unittest.main()
