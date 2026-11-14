"""Yahoo Finance chart endpoint: near-real-time NSE quotes by polling, no API key.

This is an unofficial, undocumented endpoint. It is fine for a personal
learning project, can change or rate-limit without notice, and must not be
used commercially. The provider polls gently (one request per symbol per
refresh interval, spread evenly) and backs off when Yahoo pushes back.

Because it polls, it sees a price snapshot every few seconds rather than every
trade, so intraday highs and lows between polls are approximate. Startup
backfill uses Yahoo's own one-minute bars, which are exact.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import httpx

from bazaarpulse.logging import get_logger
from bazaarpulse.marketdata.instruments import InstrumentSpec
from bazaarpulse.marketdata.types import Bar, Batch, Tick

log = get_logger(__name__)
CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
# Yahoo rejects clients that do not look like a browser.
_HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126 Safari/537.36"}


def parse_bars(symbol: str, chart: dict[str, Any]) -> list[Bar]:
    """One-minute bars from a chart response; minutes with no trades come back as nulls and are skipped."""
    result = chart["chart"]["result"][0]
    quote = result["indicators"]["quote"][0]
    bars = []
    for i, ts in enumerate(result.get("timestamp") or []):
        o, h, low, c, v = (quote[k][i] for k in ("open", "high", "low", "close", "volume"))
        if None in (o, h, low, c):
            continue
        volume = int(v or 0)
        bars.append(
            Bar(symbol, int(ts) // 60 * 60, o, h, low, c, volume, notional=(h + low + c) / 3 * volume, trades=1)
        )
    return bars


def parse_snapshot(chart: dict[str, Any]) -> tuple[float, float, int, float | None] | None:
    """``(price, time, day_volume, previous_close)`` from a chart response's metadata."""
    meta = chart["chart"]["result"][0]["meta"]
    price, at = meta.get("regularMarketPrice"), meta.get("regularMarketTime")
    if price is None or at is None:
        return None
    prev = meta.get("chartPreviousClose") or meta.get("previousClose")
    return float(price), float(at), int(meta.get("regularMarketVolume") or 0), float(prev) if prev else None


class YahooProvider:
    name = "yahoo"

    def __init__(
        self, specs: tuple[InstrumentSpec, ...], *, poll_seconds: float = 30.0, http: httpx.AsyncClient | None = None
    ) -> None:
        self.specs = specs
        self.poll_seconds = poll_seconds
        self._http = http or httpx.AsyncClient(timeout=10, headers=_HEADERS)
        self._seen: dict[str, tuple[float, float, int]] = {}  # symbol -> (price, time, day_volume)
        self._closed = False

    async def _chart(self, spec: InstrumentSpec) -> dict[str, Any] | None:
        url = CHART_URL.format(symbol=spec.provider_symbol or spec.symbol)
        r = await self._http.get(url, params={"interval": "1m", "range": "1d"})
        if r.status_code == 429:
            raise httpx.HTTPStatusError("rate limited", request=r.request, response=r)
        r.raise_for_status()
        body: dict[str, Any] = r.json()
        return body if body.get("chart", {}).get("result") else None

    async def backfill(self, minutes: int) -> list[Bar]:
        bars: list[Bar] = []
        for spec in self.specs:
            try:
                if chart := await self._chart(spec):
                    bars.extend(parse_bars(spec.symbol, chart)[-minutes:])
            except (httpx.HTTPError, KeyError, ValueError) as e:
                log.warning("yahoo.backfill_failed", symbol=spec.symbol, error=str(e))
            await asyncio.sleep(0.3)
        return sorted(bars, key=lambda b: (b.start, b.symbol))

    def tick_from(self, spec: InstrumentSpec, chart: dict[str, Any]) -> Tick | None:
        snapshot = parse_snapshot(chart)
        if snapshot is None:
            return None
        price, at, day_volume, prev_close = snapshot
        last = self._seen.get(spec.symbol)
        if last and (last[0], last[1]) == (price, at):
            return None  # nothing traded since the previous poll
        self._seen[spec.symbol] = (price, at, day_volume)
        # Size = shares traded since the last poll; on the first poll that is unknowable, so 1.
        size = max(1, day_volume - last[2]) if last else 1
        return Tick(spec.symbol, price, size, at, prev_close)

    async def stream(self) -> AsyncIterator[Batch]:
        gap = self.poll_seconds / max(1, len(self.specs))  # spread requests evenly across the interval
        penalty = 0.0
        while not self._closed:
            for spec in self.specs:
                try:
                    chart = await self._chart(spec)
                    penalty = 0.0
                    if chart and (tick := self.tick_from(spec, chart)):
                        yield Batch([tick])
                except httpx.HTTPStatusError as e:
                    penalty = min(max(penalty * 2, 15.0), 300.0)
                    log.warning("yahoo.backing_off", status=e.response.status_code, seconds=penalty)
                except (httpx.HTTPError, KeyError, ValueError) as e:
                    log.warning("yahoo.poll_failed", symbol=spec.symbol, error=str(e))
                await asyncio.sleep(gap + penalty)

    async def control(self, command: dict[str, Any]) -> None:
        return None

    async def aclose(self) -> None:
        self._closed = True
        await self._http.aclose()
