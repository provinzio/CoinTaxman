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

import logging

import log_config  # noqa: F401
from book import Book
from patch_database import patch_databases
from price_data import PriceData
from taxman import Taxman

log = logging.getLogger(__name__)


def main() -> None:
    patch_databases()

    price_data = PriceData()
    book = Book(price_data)
    taxman = Taxman(book, price_data)

    status = book.read_files()

    if not status:
        log.warning("Stopping CoinTaxman.")
        return

    book.get_price_from_csv()
    taxman.evaluate_taxation()
    taxman.export_evaluation_as_csv()
    taxman.print_evaluation()


if __name__ == "__main__":
    main()
