import datetime
import decimal
from abc import ABC, abstractmethod
from typing import Any, Callable, Optional


class FallbackPriceNotFound(Exception):
    pass


class PriceProvider(ABC):
    def __init__(
        self,
        get_price_func: Callable[..., decimal.Decimal],
        missing_symbols: Optional[set[str]] = None,
    ) -> None:
        self.get_price = get_price_func
        self._missing_symbols: set[str] = set(
        ) if missing_symbols is None else set(missing_symbols)
        self._missing_symbols_dirty = False

    def is_known_missing_symbol(self, symbol: str) -> bool:
        return symbol in self._missing_symbols

    def mark_missing_symbol(self, symbol: str) -> None:
        if symbol not in self._missing_symbols:
            self._missing_symbols.add(symbol)
            self._missing_symbols_dirty = True

    def get_missing_symbols(self) -> set[str]:
        return set(self._missing_symbols)

    def has_missing_symbols_update(self) -> bool:
        return self._missing_symbols_dirty

    def mark_missing_symbols_persisted(self) -> None:
        self._missing_symbols_dirty = False

    @abstractmethod
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        pass
