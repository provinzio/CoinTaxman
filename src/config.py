# CoinTaxman
# Copyright (C) 2021  Carsten Docktor

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

from datetime import datetime

import core


# User specific constants.
COUNTRY = core.Country.GERMANY
TAX_YEAR = 2020

# Country specific constants.
if COUNTRY == core.Country.GERMANY:
    FIAT = core.Fiat.EUR
    PRINCIPLE = core.Principle.FIFO

    def IS_LONG_TERM(buy: datetime, sell: datetime):
        return (buy - sell).years > 1
else:
    raise NotImplementedError(f"Your country {COUNTRY} is not supported.")

# Program specific constants.
ACCOUNT_STATMENTS_DIR = "account_statements"
DATA_DIR = "data"
FIAT = FIAT.name  # Convert to string.