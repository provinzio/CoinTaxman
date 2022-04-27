# CoinTaxman
# Copyright (C) 2021  Carsten Docktor <https://github.com/provinzio>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.

# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os

import log_config
from book import Book
from config import TMP_LOG_FILEPATH
from patch_database import patch_databases
from price_data import PriceData
from taxman import Taxman

log = log_config.getLogger(__name__)


def main() -> None:
    patch_databases()

    price_data = PriceData()
    book = Book(price_data)
    taxman = Taxman(book, price_data)

    status = book.read_files()

    if not status:
        log.warning("Stopping CoinTaxman.")
        return

    # Merge identical operations together, which makes it easier to match
    # buy/sell to get prices from csv, match fees and reduces database access
    # (as long as there are only one buy/sell pair per time,
    # might be problematic otherwhise).
    book.merge_identical_operations()
    # Resolve dependencies between withdrawals and deposits, which is
    # necessary to correctly fetch prices and to calculate p/l.
    book.resolve_deposits()
    book.get_price_from_csv()
    book.match_fees_with_operations()

    taxman.evaluate_taxation()
    # evaluation_file_path = taxman.export_evaluation_as_csv()
    evaluation_file_path = taxman.export_evaluation_as_excel()
    taxman.print_evaluation()

    # Save log
    log_file_path = evaluation_file_path.with_suffix(".log")
    log_config.shutdown()
    os.rename(TMP_LOG_FILEPATH, log_file_path)


if __name__ == "__main__":
    main()
