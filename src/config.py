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

from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta

import core


# User specific constants.
COUNTRY = core.Country.GERMANY
TAX_YEAR = 2020

# Country specific constants.
if COUNTRY == core.Country.GERMANY:
    FIAT_CLASS = core.Fiat.EUR
    PRINCIPLE = core.Principle.FIFO

    def IS_LONG_TERM(buy: datetime, sell: datetime) -> bool:
        return buy + relativedelta(years=1) < sell
else:
    raise NotImplementedError(f"Your country {COUNTRY} is not supported.")

# Program specific constants.
BASE_PATH = Path(__file__).parent.parent.absolute()
ACCOUNT_STATMENTS_PATH = Path(BASE_PATH, "account_statements")
DATA_PATH = Path(BASE_PATH, "data")
EXPORT_PATH = Path(BASE_PATH, "export")
FIAT = FIAT_CLASS.name  # Convert to string.
