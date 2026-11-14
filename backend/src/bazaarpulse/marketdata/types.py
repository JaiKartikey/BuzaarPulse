"""Plain data types for the market-data path.

Prices are floats here: this path is analytics (candles, indicators, charts),
where speed matters and a rounding error in the 12th digit does not. The
moment a price becomes *money* (a fill), the trading code converts it to a
``Decimal`` snapped to the instrument's tick size.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class Tick:
    """One trade print: ``size`` shares changed hands at ``price``."""

    symbol: str
    price: float
    size: int
    ts: float  # epoch seconds, as stamped by the source
    prev_close: float | None = None  # sent by live feeds so the day's change is right from the first tick

    def to_wire(self) -> dict[str, Any]:
        wire: dict[str, Any] = {"s": self.symbol, "p": self.price, "v": self.size, "t": self.ts}
        if self.prev_close is not None:
            wire["pc"] = self.prev_close
        return wire

    @classmethod
    def from_wire(cls, d: dict[str, Any]) -> Tick:
        return cls(d["s"], float(d["p"]), int(d["v"]), float(d["t"]), d.get("pc"))


@dataclass(slots=True)
class Bar:
    """An OHLCV candle. ``notional`` (sum of price x size) is kept so VWAP survives merging."""

    symbol: str
    start: int  # epoch seconds, aligned to the interval
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    notional: float = 0.0
    trades: int = 0

    @property
    def vwap(self) -> float:
        return self.notional / self.volume if self.volume else self.close

    def add(self, price: float, size: int) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += size
        self.notional += price * size
        self.trades += 1

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_chart(self) -> dict[str, Any]:
        return {
            "t": self.start, "o": self.open, "h": self.high, "l": self.low, "c": self.close,
            "v": self.volume, "vw": round(self.vwap, 4),
        }  # fmt: skip


@dataclass(frozen=True, slots=True)
class NewsItem:
    headline: str
    ts: float
    symbol: str | None = None
    summary: str = ""
    source: str = "SimWire"
    url: str | None = None
    sentiment: float | None = None
    is_simulated: bool = False

    def to_wire(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AnomalyEvent:
    symbol: str
    kind: str  # price_spike | price_drop
    severity: str  # low | medium | high
    ts: float
    move_pct: float
    zscore: float
    window_seconds: int
    price: float
    volume_ratio: float

    def to_wire(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Batch:
    """What a provider yields: everything that arrived since the last yield."""

    ticks: list[Tick] = field(default_factory=list)
    news: list[NewsItem] = field(default_factory=list)
