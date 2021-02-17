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

from book import Book
import log_config  # pylint: disable=unused-import
from price_data import PriceData
from taxman import Taxman

log = logging.getLogger(__name__)


def main() -> None:
    book = Book()
    price_data = PriceData()
    taxman = Taxman(book, price_data)

    status = book.read_files()

    if not status:
        log.warning("Stopping CoinTaxman.")
        return

    taxman.evaluate_taxation()
    taxman.export_evaluation()
    taxman.print_evaluation()


if __name__ == "__main__":
    main()
