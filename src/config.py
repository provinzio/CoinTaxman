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

import configparser
from datetime import datetime
from os import environ
from pathlib import Path

from dateutil.relativedelta import relativedelta

import core

config = configparser.ConfigParser()
config.read("config.ini")

try:
    COUNTRY = core.Country[config["BASE"].get("COUNTRY", "GERMANY")]
except KeyError as e:
    raise NotImplementedError(
        f"Your country {e} is currently not supported. " "Please create an Issue or PR."
    )

TAX_YEAR = int(config["BASE"].get("TAX_YEAR", "2021"))
MEAN_MISSING_PRICES = config["BASE"].getboolean("MEAN_MISSING_PRICES")
CALCULATE_VIRTUAL_SELL = config["BASE"].getboolean("CALCULATE_VIRTUAL_SELL")
MULTI_DEPOT = config["BASE"].getboolean("MULTI_DEPOT")
EXPORT_VIRTUAL_SELL = config["BASE"].getboolean("EXPORT_VIRTUAL_SELL")
EXPORT_ALL_EVENTS = config["BASE"].getboolean("EXPORT_ALL_EVENTS")

# Read in environmental variables.
if _env_country := environ.get("COUNTRY"):
    COUNTRY = core.Country[_env_country]
if _env_tax_year := environ.get("TAX_YEAR"):
    try:
        TAX_YEAR = int(_env_tax_year)
    except ValueError as e:
        raise ValueError(
            "Unable to convert environment variable `TAX_YEAR` to int"
        ) from e

# Country specific constants.
if COUNTRY == core.Country.GERMANY:
    FIAT_CLASS = core.Fiat.EUR
    PRINCIPLE = core.Principle.FIFO

    def IS_LONG_TERM(buy: datetime, sell: datetime) -> bool:
        return buy + relativedelta(years=1) < sell

else:
    raise NotImplementedError(
        f"Your country {COUNTRY} is currently not supported. "
        "Please create an Issue or PR."
    )

# Program specific constants.
BASE_PATH = Path(__file__).parent.parent.absolute()
ACCOUNT_STATMENTS_PATH = Path(BASE_PATH, "account_statements")
DATA_PATH = Path(BASE_PATH, "data")
EXPORT_PATH = Path(BASE_PATH, "export")
TMP_LOG_FILEPATH = Path(EXPORT_PATH, "tmp.log")

# Class for simplified casefold string comparison with configured fiat currency
class Fiat(str):
    def __init__(self, name):
        self.name = name

    def __eq__(self, fiat):
        if isinstance(fiat, str):
            return self.name.casefold() == fiat.casefold()
        else:
            raise TypeError(f"Unsupported operand for ==: {type(fiat)}")


FIAT = Fiat(FIAT_CLASS.name)
