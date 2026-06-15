from typing import Callable, Optional

from .base import PriceProvider
from .binance import BinancePriceProvider
from .bitget import BitgetPriceProvider
from .bitpanda import BitpandaPriceProvider, BitpandaProPriceProvider
from .bitunix import BitunixPriceProvider
from .coinbase import CoinbasePriceProvider, CoinbaseProPriceProvider
from .kraken import KrakenPriceProvider
from .pionex import PionexPriceProvider


def create_price_provider(
    platform: str,
    get_price_func: Callable,
    missing_symbols: Optional[set[str]] = None,
) -> Optional[PriceProvider]:
    provider_map = {
        "binance": BinancePriceProvider,
        "coinbase": CoinbasePriceProvider,
        "coinbase_pro": CoinbaseProPriceProvider,
        "kraken": KrakenPriceProvider,
        "bitpanda": BitpandaPriceProvider,
        "bitpanda_pro": BitpandaProPriceProvider,
        "pionex": PionexPriceProvider,
        "bitunix": BitunixPriceProvider,
        "bitget": BitgetPriceProvider,
    }

    provider_class = provider_map.get(platform)
    if provider_class is None:
        return None
    return provider_class(get_price_func, missing_symbols=missing_symbols)
