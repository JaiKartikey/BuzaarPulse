"""Market-data sources behind one interface.

``build_provider`` is the only place that knows which concrete feed is in use.
"""

from __future__ import annotations

from bazaarpulse.config import Settings
from bazaarpulse.marketdata.instruments import universe_for
from bazaarpulse.marketdata.providers.base import MarketDataProvider


def build_provider(settings: Settings, start_prices: dict[str, float] | None = None) -> MarketDataProvider:
    specs = universe_for(settings.market_provider)
    if settings.market_provider == "finnhub":
        from bazaarpulse.marketdata.providers.finnhub import FinnhubProvider

        if not settings.finnhub_api_key:
            raise ValueError("MARKET_PROVIDER=finnhub needs FINNHUB_API_KEY")
        return FinnhubProvider(specs, settings.finnhub_api_key)
    if settings.market_provider == "yahoo":
        from bazaarpulse.marketdata.providers.yahoo import YahooProvider

        return YahooProvider(specs, poll_seconds=settings.yahoo_poll_seconds)
    from bazaarpulse.marketdata.providers.simulator import SimulatorProvider

    return SimulatorProvider(
        specs,
        seed=settings.sim_seed,
        tick_interval=settings.sim_tick_interval_ms / 1000,
        shocks_per_hour=settings.sim_shock_per_hour,
        start_prices=start_prices,
    )


__all__ = ["MarketDataProvider", "build_provider"]
