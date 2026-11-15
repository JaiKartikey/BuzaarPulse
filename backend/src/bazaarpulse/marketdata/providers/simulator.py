"""A synthetic market that behaves enough like a real one to exercise the whole system.

Each step, a stock's log-return is the sum of three random parts:

* a **market** move shared by everything, scaled by the stock's beta,
* a **sector** move shared by its sector,
* an **idiosyncratic** move of its own,

plus a gentle pull back towards an anchor price so nothing drifts to zero or
infinity over a long run. On top of that, random **shocks** hit one stock at a
time: a few percent over a few seconds, on heavy volume, with a headline that
explains it. Shocks are what make the anomaly detector and the AI analyst
have something to do.

``SimulatedMarket`` is pure and seeded (same seed, same market), which makes
tests deterministic. ``SimulatorProvider`` wraps it in real time.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from bazaarpulse.marketdata.candles import BAR_SECONDS
from bazaarpulse.marketdata.instruments import InstrumentSpec
from bazaarpulse.marketdata.types import Bar, Batch, NewsItem, Tick

# Volatility of one second of trading, as a fraction (about 2.4% over a 6-hour session).
SIGMA_PER_SQRT_SECOND = 0.00016
MEAN_REVERSION_PER_SECOND = 1 / 3600
SHOCK_SECONDS = 8.0

_NEGATIVE = (
    (
        "slides {pct}% after a brokerage downgrade",
        "Analysts cut their rating, citing margin pressure in the core business.",
    ),
    ("drops {pct}% on weak quarterly guidance", "Management guided next quarter's revenue below street estimates."),
    (
        "falls {pct}% as a large shareholder sells a block",
        "A block deal at a discount to the market price went through this session.",
    ),
    (
        "sinks {pct}% on reports of a regulatory inquiry",
        "Reports say the regulator has sought details on recent disclosures.",
    ),
)
_POSITIVE = (
    ("jumps {pct}% after winning a large order", "The company announced its biggest contract win of the year."),
    ("rallies {pct}% on a brokerage upgrade", "Analysts raised their target price, citing improving demand."),
    ("surges {pct}% as quarterly profit beats estimates", "Net profit came in ahead of consensus on better margins."),
    (
        "climbs {pct}% on a stake sale at a premium",
        "A strategic investor is buying a minority stake above the market price.",
    ),
)


@dataclass(slots=True)
class _Shock:
    remaining: float  # log-return still to apply
    steps_left: int


class SimulatedMarket:
    def __init__(
        self,
        specs: tuple[InstrumentSpec, ...],
        *,
        seed: int | None = None,
        tick_interval: float = 0.25,
        shocks_per_hour: float = 6.0,
        start_prices: dict[str, float] | None = None,
        prev_closes: dict[str, float] | None = None,
    ) -> None:
        self.specs = {s.symbol: s for s in specs}
        self.rng = random.Random(seed)
        self.dt = tick_interval
        self.shocks_per_hour = shocks_per_hour
        self.prices = {s.symbol: (start_prices or {}).get(s.symbol, float(s.ref_price)) for s in specs}
        self.anchors = dict(self.prices)
        # Like a real feed, every tick carries the previous session's close, so "change today" is
        # right from the first tick and stays right across restarts.
        self.prev_close = {s.symbol: (prev_closes or {}).get(s.symbol, self.prices[s.symbol]) for s in specs}
        self._day: int | None = None
        self.sectors = sorted({s.sector for s in specs})
        self._shocks: dict[str, _Shock] = {}
        self.pending_news: list[NewsItem] = []

    def shock(self, symbol: str, pct: float, now: float) -> NewsItem:
        """Schedule a move of ``pct`` percent on ``symbol`` and publish the headline behind it."""
        spec = self.specs[symbol]
        steps = max(1, round(SHOCK_SECONDS / self.dt))
        self._shocks[symbol] = _Shock(math.log1p(pct / 100), steps)
        headline, summary = self.rng.choice(_POSITIVE if pct > 0 else _NEGATIVE)
        item = NewsItem(
            symbol=symbol,
            headline=f"{spec.name} {headline.format(pct=f'{abs(pct):.1f}')}",
            summary=summary,
            ts=now,
            sentiment=0.7 if pct > 0 else -0.7,
            is_simulated=True,
        )
        self.pending_news.append(item)
        return item

    def step(self, now: float) -> list[Tick]:
        """Advance the market by one interval and return the trades that printed."""
        rng, dt = self.rng, self.dt
        sigma = SIGMA_PER_SQRT_SECOND * math.sqrt(dt)

        day = int(now // 86_400)
        if self._day is not None and day != self._day:
            self.prev_close = dict(self.prices)  # a new session: yesterday's last price is today's reference
        self._day = day

        # Shocks arrive as a Poisson process: each step has a small, equal chance of one.
        if self.shocks_per_hour and rng.random() < self.shocks_per_hour / 3600 * dt:
            symbol = rng.choice(list(self.specs))
            if symbol not in self._shocks:
                self.shock(symbol, rng.choice((-1, 1)) * rng.uniform(1.2, 3.5), now)

        market = rng.gauss(0, 0.6 * sigma)
        sector_moves = {sec: rng.gauss(0, 0.4 * sigma) for sec in self.sectors}
        ticks: list[Tick] = []
        for symbol, spec in self.specs.items():
            price = self.prices[symbol]
            ret = spec.beta * market + sector_moves[spec.sector] + rng.gauss(0, 0.7 * sigma)
            ret -= MEAN_REVERSION_PER_SECOND * dt * math.log(price / self.anchors[symbol])

            shock = self._shocks.get(symbol)
            if shock:
                portion = shock.remaining / shock.steps_left
                ret += portion + rng.gauss(0, 2 * sigma)
                # Move the anchor with the shock, or mean reversion would undo the news within the hour.
                self.anchors[symbol] *= math.exp(portion)
                shock.remaining -= portion
                shock.steps_left -= 1
                if shock.steps_left == 0:
                    del self._shocks[symbol]

            price *= math.exp(ret)
            self.prices[symbol] = price
            # Not every stock trades every instant; a stock in the news trades almost continuously.
            if rng.random() < (0.95 if shock else 0.55):
                tick_size = float(spec.tick_size)
                printed = max(tick_size, round(price / tick_size) * tick_size)
                size = max(1, int(rng.lognormvariate(4.2, 1.0) * (4 if shock else 1)))
                ticks.append(Tick(symbol, round(printed, 4), size, now, round(self.prev_close[symbol], 2)))
        return ticks

    def history(self, end: float, minutes: int, step_seconds: float = 5.0) -> list[Bar]:
        """Fast-forward through ``minutes`` of trading ending at ``end``; returns the 1-minute bars.

        Advances the market itself, so live trading continues from where history ends.
        """
        live_dt, self.dt = self.dt, step_seconds
        # History is calmer than the live demo: a third of the shocks, so prices arrive near where they started.
        live_shocks, self.shocks_per_hour = self.shocks_per_hour, self.shocks_per_hour / 3
        bars: dict[tuple[str, int], Bar] = {}
        start = (int(end) // BAR_SECONDS - minutes) * BAR_SECONDS
        t = float(start)
        try:
            while t < int(end) // BAR_SECONDS * BAR_SECONDS:
                for tick in self.step(t):
                    key = (tick.symbol, int(t // BAR_SECONDS) * BAR_SECONDS)
                    bar = bars.get(key)
                    if bar is None:
                        bar = bars[key] = Bar(tick.symbol, key[1], tick.price, tick.price, tick.price, tick.price)
                    bar.add(tick.price, tick.size * int(step_seconds / live_dt))
                t += step_seconds
        finally:
            self.dt, self.shocks_per_hour = live_dt, live_shocks
            self.pending_news.clear()  # headlines for moves that are already history are not news
        return sorted(bars.values(), key=lambda b: (b.start, b.symbol))


class SimulatorProvider:
    name = "simulator"

    def __init__(
        self,
        specs: tuple[InstrumentSpec, ...],
        *,
        seed: int | None = None,
        tick_interval: float = 0.25,
        shocks_per_hour: float = 6.0,
        start_prices: dict[str, float] | None = None,
        prev_closes: dict[str, float] | None = None,
    ) -> None:
        self.market = SimulatedMarket(
            specs,
            seed=seed,
            tick_interval=tick_interval,
            shocks_per_hour=shocks_per_hour,
            start_prices=start_prices,
            prev_closes=prev_closes,
        )
        self.interval = tick_interval
        self._closed = False

    async def backfill(self, minutes: int) -> list[Bar]:
        if minutes <= 0:
            return []
        # CPU-bound (hundreds of thousands of random draws): keep it off the event loop.
        return await asyncio.to_thread(self.market.history, time.time(), minutes)

    async def stream(self) -> AsyncIterator[Batch]:
        next_at = time.monotonic()
        while not self._closed:
            ticks = self.market.step(time.time())
            news, self.market.pending_news = self.market.pending_news, []
            if ticks or news:
                yield Batch(ticks, news)
            # Schedule against a fixed timeline so processing time does not slow the clock down.
            next_at += self.interval
            await asyncio.sleep(max(0.0, next_at - time.monotonic()))

    async def control(self, command: dict[str, Any]) -> None:
        if command.get("op") == "shock" and command.get("symbol") in self.market.specs:
            pct = max(-8.0, min(8.0, float(command.get("pct", -3.0))))
            self.market.shock(command["symbol"], pct, time.time())

    async def aclose(self) -> None:
        self._closed = True
