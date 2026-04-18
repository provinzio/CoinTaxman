import datetime
import decimal
from abc import ABC, abstractmethod
from typing import Any, Callable


class FallbackPriceNotFound(Exception):
    pass


class PriceProvider(ABC):
    def __init__(
        self,
        get_price_func: Callable[..., decimal.Decimal],
    ) -> None:
        self.get_price = get_price_func

    @abstractmethod
    def fetch_price(
        self,
        base_asset: str,
        utc_time: datetime.datetime,
        quote_asset: str,
        **kwargs: Any,
    ) -> decimal.Decimal:
        pass
