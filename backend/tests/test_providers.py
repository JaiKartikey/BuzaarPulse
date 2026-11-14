"""Live-feed adapters, tested against payloads in the shape each service documents.

These tests prove the parsing and polling logic. They do not prove the remote
services still answer in this shape: that needs a run against the real feed.
"""

from __future__ import annotations

import httpx
import orjson
import pytest

from bazaarpulse.config import Settings
from bazaarpulse.marketdata.instruments import NSE_UNIVERSE, US_UNIVERSE, universe_for
from bazaarpulse.marketdata.providers import build_provider
from bazaarpulse.marketdata.providers.finnhub import FinnhubProvider, parse_news, parse_trades
from bazaarpulse.marketdata.providers.simulator import SimulatorProvider
from bazaarpulse.marketdata.providers.yahoo import YahooProvider, parse_bars, parse_snapshot
from tests.conftest import make_settings

RELIANCE = next(s for s in NSE_UNIVERSE if s.symbol == "RELIANCE")


def chart(price: float, at: int, volume: int) -> dict[str, object]:
    return {
        "chart": {
            "result": [
                {
                    "meta": {
                        "regularMarketPrice": price,
                        "regularMarketTime": at,
                        "regularMarketVolume": volume,
                        "chartPreviousClose": 2890.0,
                    },
                    "timestamp": [1_800_000_000, 1_800_000_060, 1_800_000_120],
                    "indicators": {
                        "quote": [
                            {
                                "open": [2900.0, None, 2904.0],
                                "high": [2905.0, None, 2906.0],
                                "low": [2899.0, None, 2901.5],
                                "close": [2903.0, None, 2902.0],
                                "volume": [1200, None, 800],
                            }
                        ]
                    },
                }
            ]
        }
    }


def test_finnhub_trade_messages_become_ticks() -> None:
    raw = orjson.dumps(
        {
            "type": "trade",
            "data": [
                {"s": "AAPL", "p": 230.15, "t": 1_800_000_000_500, "v": 120},
                {"s": "NVDA", "p": 141.0, "t": 1_800_000_000_900, "v": 0.4},
            ],
        }
    )
    ticks = parse_trades(raw, {"AAPL": 229.0})
    assert [(t.symbol, t.price, t.size, t.ts, t.prev_close) for t in ticks] == [
        ("AAPL", 230.15, 120, 1_800_000_000.5, 229.0),
        ("NVDA", 141.0, 1, 1_800_000_000.9, None),  # fractional volume still counts as a trade
    ]
    assert parse_trades(b'{"type":"ping"}', {}) == []


def test_finnhub_news_is_normalised() -> None:
    items = parse_news(
        "AAPL",
        [
            {"headline": "Apple unveils chips", "datetime": 1_800_000_000, "source": "Wire", "url": "https://x.test/a"},
            {"headline": "", "datetime": 1_800_000_001},  # no headline: dropped
        ],
    )
    assert len(items) == 1 and items[0].symbol == "AAPL" and items[0].source == "Wire" and not items[0].is_simulated


def test_yahoo_bars_skip_minutes_without_trades() -> None:
    bars = parse_bars("RELIANCE", chart(2902.0, 1_800_000_130, 2000))
    assert [(b.start, b.open, b.close, b.volume) for b in bars] == [
        (1_800_000_000, 2900.0, 2903.0, 1200),
        (1_800_000_120, 2904.0, 2902.0, 800),
    ]
    assert parse_snapshot(chart(2902.0, 1_800_000_130, 2000)) == (2902.0, 1_800_000_130.0, 2000, 2890.0)


def test_yahoo_emits_a_tick_only_when_something_traded() -> None:
    provider = YahooProvider((RELIANCE,))
    first = provider.tick_from(RELIANCE, chart(2902.0, 1_800_000_130, 2000))
    assert first is not None and (first.price, first.size, first.prev_close) == (2902.0, 1, 2890.0)
    assert provider.tick_from(RELIANCE, chart(2902.0, 1_800_000_130, 2000)) is None  # unchanged snapshot
    third = provider.tick_from(RELIANCE, chart(2905.0, 1_800_000_140, 2350))
    assert third is not None and third.size == 350  # shares traded between the two polls


async def test_yahoo_backfill_uses_provider_symbol_and_survives_errors() -> None:
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(request.url.path)
        if "TCS" in request.url.path:
            return httpx.Response(500)
        return httpx.Response(200, json=chart(2902.0, 1_800_000_130, 2000))

    specs = tuple(s for s in NSE_UNIVERSE if s.symbol in {"RELIANCE", "TCS"})
    provider = YahooProvider(specs, http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
    bars = await provider.backfill(60)
    await provider.aclose()
    assert requested == ["/v8/finance/chart/RELIANCE.NS", "/v8/finance/chart/TCS.NS"]
    assert {b.symbol for b in bars} == {"RELIANCE"} and len(bars) == 2


def test_provider_factory() -> None:
    assert isinstance(build_provider(make_settings()), SimulatorProvider)
    assert isinstance(build_provider(make_settings(market_provider="yahoo")), YahooProvider)
    assert isinstance(
        build_provider(make_settings(market_provider="finnhub", finnhub_api_key="test-key")), FinnhubProvider
    )
    with pytest.raises(ValueError, match="FINNHUB_API_KEY"):
        build_provider(make_settings(market_provider="finnhub"))
    assert universe_for("finnhub") is US_UNIVERSE and universe_for("simulator") is NSE_UNIVERSE
    # A restarted simulator resumes from the last quotes instead of jumping back to the reference prices.
    resumed = build_provider(make_settings(), start_prices={"TCS": 4321.0})
    assert isinstance(resumed, SimulatorProvider) and resumed.market.prices["TCS"] == 4321.0


def test_production_refuses_the_development_secret() -> None:
    with pytest.raises(ValueError, match="JWT_SECRET"):
        Settings(_env_file=None, env="prod")  # type: ignore[call-arg]
    assert Settings(_env_file=None, env="prod", jwt_secret="x" * 40).env == "prod"  # type: ignore[call-arg]
