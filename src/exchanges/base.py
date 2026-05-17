"""Base classes for exchange readers using the Strategy pattern."""

import datetime
import decimal
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Optional

import misc
import transaction as tr


class ExchangeReader(ABC):
    """Abstract base class for exchange CSV readers.

    Each exchange implements this to handle its specific CSV format.
    """

    def __init__(self, platform: str):
        self.platform = platform

    @abstractmethod
    def read_file(self, file_path: Path, book: Any) -> None:
        """Read and parse the CSV file, appending operations to the book.

        Args:
            file_path: Path to the CSV file to read
            book: Book instance collecting parsed operations
        """
        pass

    def append_operation(
        self,
        book,
        operation: str,
        utc_time: datetime.datetime,
        change: decimal.Decimal,
        coin: str,
        row: int,
        file_path: Path,
        remark: Optional[str] = None,
    ) -> None:
        """Helper to append an operation to the book."""
        book.append_operation(
            operation, utc_time, self.platform, change, coin, row, file_path, remark
        )

    def parse_utc_time(self, time_str: str, fmt: str) -> datetime.datetime:
        """Parse a UTC time string with the given format."""
        utc_time = datetime.datetime.strptime(time_str, fmt)
        return utc_time.replace(tzinfo=datetime.timezone.utc)

    def force_decimal(self, value: str) -> decimal.Decimal:
        """Parse a decimal value, handling empty strings."""
        return misc.force_decimal(value)

    def xdecimal(self, value: str) -> Optional[decimal.Decimal]:
        """Parse a decimal value, returning None for empty strings."""
        return misc.xdecimal(value)
