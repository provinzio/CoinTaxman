"""Common utilities for exchange readers."""

import datetime
import decimal
from typing import Optional

import misc


def parse_utc_time(time_str: str, fmt: str) -> datetime.datetime:
    """Parse a UTC time string with the given format."""
    utc_time = datetime.datetime.strptime(time_str, fmt)
    return utc_time.replace(tzinfo=datetime.timezone.utc)


def force_decimal(value: str) -> decimal.Decimal:
    """Parse a decimal value, handling empty strings."""
    return misc.force_decimal(value)


def xdecimal(value: str) -> Optional[decimal.Decimal]:
    """Parse a decimal value, returning None for empty strings."""
    return misc.xdecimal(value)
